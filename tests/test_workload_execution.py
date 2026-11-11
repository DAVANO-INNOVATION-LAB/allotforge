"""Execution security/lifecycle contracts against an API double, not a real cluster."""
import copy
import os
import sqlite3
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from davano.control_models import DeletePreconditions
from davano.kube import ApiError, ROOT, MANAGER_LABEL
from davano.reconciler import Reconciler, KUEUE, pool_name, policy_name, cr_path
from davano.store import Store
from davano.workload_api import install_workload_api
from davano.workload_models import ExecutionConfig, WorkloadCreate, token_hash
from davano.workload_service import PROJECT_LABEL, OPERATION_ANNOTATION, summarize
from test_control import fixture

TOKEN = 's' * 64
VIEWER = 'v' * 64
OTHER = 'o' * 64


def config():
    return ExecutionConfig.model_validate({
        'profiles': [{'id': 'smoke', 'label': 'Approved smoke test', 'image': 'registry.example/gpu-smoke@sha256:' + 'a' * 64,
                      'command': ['/opt/bin/smoke'], 'cpu_millicores': 1000, 'memory_mib': 1024, 'gpu_max': 2, 'max_runtime_seconds': 300}],
        'projects': [{'id': 'research', 'namespace': 'team', 'namespace_uid': 'namespace-1', 'policy': 'research',
                      'policy_uid': '22222222-2222-2222-2222-222222222222', 'pool_uid': '11111111-1111-1111-1111-111111111111',
                      'service_account': 'davano-runtime', 'service_account_uid': 'runtime-1', 'network_policy': 'deny-all', 'profiles': ['smoke']},
                     {'id': 'other', 'namespace': 'other-team', 'namespace_uid': 'namespace-2', 'policy': 'other', 'policy_uid': 'policy-2',
                      'pool_uid': 'pool-2', 'service_account': 'davano-runtime', 'service_account_uid': 'runtime-2', 'network_policy': 'deny-all', 'profiles': ['smoke']}],
        'identities': [{'id': 'alice', 'token_sha256': token_hash(TOKEN), 'projects': ['research'], 'role': 'submitter'},
                       {'id': 'reader', 'token_sha256': token_hash(VIEWER), 'projects': ['research'], 'role': 'viewer'},
                       {'id': 'bob', 'token_sha256': token_hash(OTHER), 'projects': ['other'], 'role': 'submitter'}]})


def setup():
    c, p, a = fixture()
    c.objects['/api/v1/namespaces/team']['metadata'].update(uid='namespace-1', labels={PROJECT_LABEL: 'research', 'pod-security.kubernetes.io/enforce': 'restricted', 'pod-security.kubernetes.io/enforce-version': 'latest'})
    c.seed('/api/v1/namespaces/team/serviceaccounts/davano-runtime', {'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'davano-runtime', 'uid': 'runtime-1'}, 'automountServiceAccountToken': False})
    c.seed('/apis/networking.k8s.io/v1/namespaces/team/networkpolicies/deny-all', {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': {'name': 'deny-all'}, 'spec': {'podSelector': {}, 'policyTypes': ['Ingress', 'Egress'], 'ingress': [], 'egress': []}})
    c.seed('/api/v1/namespaces/team/resourcequotas/davano-execution-bounds', {'apiVersion': 'v1', 'kind': 'ResourceQuota', 'metadata': {'name': 'davano-execution-bounds'}, 'spec': {'hard': {'count/jobs.batch': '20'}}, 'status': {'hard': {'count/jobs.batch': '20'}}})
    Reconciler(c, ['team']).cycle()
    c.writes = []
    store = Store(':memory:', source='kubernetes')
    app = FastAPI()
    with patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'true'}):
        service = install_workload_api(app, 'kubernetes', store, client=c, config=config())
    return c, p, a, store, service, TestClient(app)


class ExecutionTests(unittest.TestCase):
    def setUp(self):
        self.kube, self.pool, self.policy, self.store, self.service, self.http = setup()
        self.headers = {'Authorization': 'Bearer ' + TOKEN}
        self.base = '/api/v1/execution/projects/research'
        self.body = {'name': 'test-001', 'profile': 'smoke', 'gpu_count': 1}
    def tearDown(self):
        self.http.close()
        self.store.close()
    def submit(self):
        return self.http.post(self.base + '/workloads', headers=self.headers, json=self.body)
    def preview(self):
        return self.http.post(self.base + '/preview', headers=self.headers, json=self.body)
    def test_preview_never_writes(self):
        response = self.preview()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertFalse(response.json()['submitted'])
        self.assertEqual(self.kube.writes, [])
    def test_restricted_template_has_no_escape_hatches(self):
        response = self.preview()
        job = response.json()['manifest']
        spec = job['spec']['template']['spec']
        self.assertTrue(job['spec']['suspend'])
        self.assertEqual(job['metadata']['labels']['kueue.x-k8s.io/queue-name'], policy_name(self.policy))
        self.assertFalse(spec['automountServiceAccountToken'])
        self.assertFalse(spec['enableServiceLinks'])
        for key in ('volumes', 'imagePullSecrets', 'initContainers', 'ephemeralContainers', 'nodeName', 'tolerations'):
            self.assertNotIn(key, spec)
        container = spec['containers'][0]
        self.assertNotIn('env', container)
        self.assertNotIn('envFrom', container)
        self.assertFalse(container['securityContext']['allowPrivilegeEscalation'])
        self.assertEqual(container['securityContext']['capabilities']['drop'], ['ALL'])
        self.assertTrue(container['securityContext']['readOnlyRootFilesystem'])
        self.assertEqual(spec['securityContext']['runAsUser'], 65532)
        self.assertEqual(container['resources']['requests'], container['resources']['limits'])
    def test_submit_idempotent_and_cancel_preconditioned(self):
        first = self.submit()
        self.assertEqual(first.status_code, 201, first.text)
        result = first.json()
        self.assertTrue(result['submitted'])
        self.assertFalse(result['allocated'])
        self.assertEqual(result['workload']['phase'], 'Suspended')
        retry = self.submit()
        self.assertTrue(retry.json()['idempotent'])
        self.assertEqual(len([x for x in self.kube.writes if x[0] == 'POST']), 1)
        listed = self.http.get(self.base + '/workloads', headers=self.headers).json()
        self.assertEqual(len(listed['items']), 1)
        identity = {k: result['workload'][k] for k in ('uid', 'resourceVersion')}
        deleted = self.http.post(self.base + '/workloads/test-001/cancel', headers=self.headers, json=identity)
        self.assertEqual(deleted.status_code, 200, deleted.text)
        self.assertTrue(deleted.json()['deletion_requested'])
        self.assertFalse(deleted.json()['deleted'])
        self.assertEqual(self.submit().status_code, 409)
    def test_cancel_changed_resource_rejected(self):
        self.submit()
        response = self.http.post(self.base + '/workloads/test-001/cancel', headers=self.headers, json={'uid': 'different', 'resourceVersion': '1'})
        self.assertEqual(response.status_code, 409)
        self.assertFalse(any(x[0] == 'DELETE' for x in self.kube.writes))
    def test_invalid_token_cannot_inspect_projects(self):
        for headers in ({}, {'Authorization': 'Bearer bad'}, {b'Authorization': b'Bearer ' + b'\xc3\xa9' * 64}):
            self.assertEqual(self.http.get('/api/v1/execution/projects', headers=headers).status_code, 401)
        self.assertEqual(self.kube.writes, [])
    def test_scope_does_not_leak_projects(self):
        projects = self.http.get('/api/v1/execution/projects', headers=self.headers).json()['items']
        self.assertEqual([p['id'] for p in projects], ['research'])
        self.assertNotIn('token_sha256', str(projects))
        self.assertEqual(self.http.get('/api/v1/execution/projects/other/workloads', headers=self.headers).status_code, 404)
    def test_viewer_cannot_mutate(self):
        headers = {'Authorization': 'Bearer ' + VIEWER}
        self.assertEqual(self.http.post(self.base + '/workloads', headers=headers, json=self.body).status_code, 403)
        self.assertEqual(self.http.post(self.base + '/workloads/anything/cancel', headers=headers, json={'uid': 'uid', 'resourceVersion': '1'}).status_code, 403)
        self.assertEqual(self.http.post(self.base + '/preview', headers=headers, json=self.body).status_code, 200)
        self.assertEqual(self.kube.writes, [])
    def test_arbitrary_template_fields_rejected(self):
        for field, value in [('command', ['sh']), ('image', 'evil'), ('namespace', 'other-team'), ('env', {}), ('serviceAccountName', 'admin'), ('pod_spec', {})]:
            response = self.http.post(self.base + '/workloads', headers=self.headers, json={**self.body, field: value})
            self.assertEqual(response.status_code, 422, field)
        self.assertEqual(self.kube.writes, [])
    def test_profile_and_gpu_boundaries(self):
        for key, value, status in [('profile', 'unapproved', 403), ('gpu_count', 3, 422), ('gpu_count', True, 422)]:
            response = self.http.post(self.base + '/workloads', headers=self.headers, json={**self.body, key: value})
            self.assertEqual(response.status_code, status, response.text)
        self.assertEqual(self.kube.writes, [])
    def test_namespace_replacement_blocks(self):
        self.kube.objects['/api/v1/namespaces/team']['metadata']['uid'] = 'replaced'
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_serviceaccount_credentials_block(self):
        for field, value in [('secrets', [{'name': 'credential'}]), ('imagePullSecrets', [{'name': 'registry'}]), ('automountServiceAccountToken', True)]:
            with self.subTest(field=field):
                key = '/api/v1/namespaces/team/serviceaccounts/davano-runtime'
                old = copy.deepcopy(self.kube.objects[key])
                self.kube.objects[key][field] = value
                self.assertEqual(self.submit().status_code, 409)
                self.kube.objects[key] = old
        self.assertEqual(self.kube.writes, [])
    def test_additive_network_allow_policy_blocks(self):
        self.kube.seed('/apis/networking.k8s.io/v1/namespaces/team/networkpolicies/allow', {'kind': 'NetworkPolicy', 'metadata': {'name': 'allow'}, 'spec': {}})
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_stale_or_unknown_guardrails_block(self):
        path = cr_path(self.policy)
        original = copy.deepcopy(self.kube.objects[path])
        for mutate in (lambda p: p['status'].update(lastReconcileTime='2000-01-01T00:00:00Z'), lambda p: p['status'].update(workloadGrants='revocation-pending'), lambda p: p['metadata'].update(generation=2)):
            self.kube.objects[path] = copy.deepcopy(original)
            mutate(self.kube.objects[path])
            self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_queue_hold_blocks(self):
        self.kube.objects[KUEUE + '/clusterqueues/' + pool_name(self.pool)]['spec']['stopPolicy'] = 'Hold'
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_inventory_failure_no_fallback(self):
        self.kube.fail = '/api/v1/nodes'
        self.assertEqual(self.submit().status_code, 503)
        self.assertEqual(self.kube.writes, [])
    def test_gpu_mode_drift_blocks(self):
        self.kube.objects['/api/v1/nodes/worker']['metadata']['labels']['nvidia.com/gpu.sharing-strategy'] = 'time-slicing'
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_foreign_job_never_adopted_or_canceled(self):
        path = self.service.job_path(config().projects[0], self.body['name'])
        self.kube.seed(path, {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {'name': self.body['name'], 'namespace': 'team', 'uid': 'foreign', 'resourceVersion': '1'}, 'spec': {}})
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.submit().status_code, 409)
        self.assertFalse(any(method == 'DELETE' for method, _, _ in self.kube.writes))
    def test_uid_replacement_cannot_be_read_or_canceled(self):
        result = self.submit().json()['workload']
        path = self.service.job_path(config().projects[0], 'test-001')
        self.kube.objects[path]['metadata']['uid'] = 'replacement'
        self.assertEqual(self.http.get(self.base + '/workloads/test-001', headers=self.headers).status_code, 409)
        self.assertEqual(self.http.post(self.base + '/workloads/test-001/cancel', headers=self.headers, json={k: result[k] for k in ('uid', 'resourceVersion')}).status_code, 409)
    def test_uncertain_submit_recovered_without_duplicate(self):
        original = self.kube.request
        state = {'done': False}
        def request(method, path, body=None):
            result = original(method, path, body)
            if method == 'POST' and '/jobs' in path and not state['done']:
                state['done'] = True
                raise ApiError(503, 'Response lost')
            return result
        with patch.object(self.kube, 'request', side_effect=request):
            self.assertEqual(self.submit().status_code, 503)
            response = self.submit()
        self.assertEqual(response.status_code, 201, response.text)
        self.assertTrue(response.json()['idempotent'])
        self.assertEqual(len([x for x in self.kube.writes if x[0] == 'POST']), 1)
    def test_audit_is_project_scoped_and_redacted(self):
        self.submit()
        result = self.http.get(self.base + '/audit', headers=self.headers)
        self.assertEqual(result.status_code, 200)
        self.assertEqual([x['outcome'] for x in result.json()['items']], ['accepted', 'intent'])
        self.assertNotIn(TOKEN, result.text)
        self.assertNotIn('token_sha256', result.text)
        self.assertEqual(self.http.get(self.base + '/audit', headers={'Authorization': 'Bearer ' + OTHER}).status_code, 404)
    def test_audit_failure_prevents_new_mutation(self):
        with patch.object(self.service, 'audit', side_effect=ApiError(503, 'audit unavailable')):
            self.assertEqual(self.submit().status_code, 503)
        self.assertEqual(self.kube.writes, [])
    def test_changed_request_same_name_conflicts(self):
        self.submit()
        self.body['gpu_count'] = 2
        self.assertEqual(self.submit().status_code, 409)
    def test_disabled_mode_never_constructs_kubernetes_client(self):
        app = FastAPI()
        with patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'false'}), patch('davano.workload_api.KubeClient') as kube:
            install_workload_api(app, 'demo', self.store)
            http = TestClient(app)
            self.assertFalse(http.get('/api/v1/execution/status').json()['enabled'])
            self.assertEqual(http.get('/api/v1/execution/projects').status_code, 409)
            kube.assert_not_called()
    def test_config_rejects_tag_only_images_and_unknown_scope(self):
        for change in ('image', 'project'):
            raw = config().model_dump()
            if change == 'image':
                raw['profiles'][0]['image'] = 'image:latest'
            else:
                raw['identities'][0]['projects'] = ['missing']
            with self.assertRaises(ValidationError):
                ExecutionConfig.model_validate(raw)
    def test_api_defaulted_guards_pass_but_narrowing_mutations_fail(self):
        for obj in self.kube.objects.values():
            if obj['kind'] == 'ValidatingAdmissionPolicy':
                constraints = obj['spec']['matchConstraints']
                constraints.update(namespaceSelector={}, objectSelector={})
                for rule in constraints['resourceRules']:
                    rule['scope'] = '*'
            if obj['kind'] == 'LocalQueue':
                obj['spec']['stopPolicy'] = 'None'
        self.assertEqual(self.preview().status_code, 200)
        vap = next(o for o in self.kube.objects.values() if o['kind'] == 'ValidatingAdmissionPolicy')
        vap['spec']['matchConstraints']['objectSelector'] = {'matchLabels': {'bypass': 'true'}}
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_impossible_single_node_request_blocks(self):
        self.kube.objects['/api/v1/nodes/worker']['status']['allocatable']['nvidia.com/gpu'] = '1'
        self.body['gpu_count'] = 2
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_pool_nominal_quota_checked(self):
        self.kube.objects[cr_path(self.pool)]['spec']['nominalQuota'] = 0
        self.assertEqual(self.submit().status_code, 422)
        self.assertEqual(self.kube.writes, [])
    def test_pending_retry_rejects_changed_resolved_profile(self):
        self.kube.fail = 'POST'
        self.assertEqual(self.submit().status_code, 503)
        self.kube.fail = None
        self.service.config.profiles[0].image = 'other/image@sha256:' + 'b' * 64
        response = self.submit()
        self.assertEqual(response.status_code, 409, response.text)
        self.assertIn('configuration changed', response.text)
        self.assertEqual(self.kube.writes, [])
    def test_pending_retry_rejects_changed_policy(self):
        self.kube.fail = 'POST'
        self.assertEqual(self.submit().status_code, 503)
        self.kube.fail = None
        self.kube.objects[cr_path(self.policy)]['spec']['maxGPUsPerPod'] = 1
        Reconciler(self.kube, ['team']).cycle()
        self.kube.writes = []
        response = self.submit()
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.kube.writes, [])
    def test_parent_replacement_during_submit_is_fenced(self):
        original = self.service.audit
        def audit(*args):
            original(*args)
            if args[4] == 'intent':
                self.kube.objects[cr_path(self.policy)]['metadata']['uid'] = 'replacement'
        with patch.object(self.service, 'audit', side_effect=audit):
            self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_legacy_inventory_locked_in_execution_mode(self):
        from davano.app import create_app
        from test_api import MutableInventory
        from tempfile import TemporaryDirectory
        from pathlib import Path
        import json
        with TemporaryDirectory() as temp:
            path = Path(temp) / 'execution.json'
            path.write_text(json.dumps(config().model_dump()))
            with patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'true', 'DAVANO_CONTROL_ENABLED': 'false', 'DAVANO_EXECUTION_CONFIG_PATH': str(path), 'DAVANO_API_TOKEN': 'a' * 64}), patch('davano.workload_api.KubeClient', return_value=self.kube):
                app = create_app(source='kubernetes', inventory=MutableInventory(), db_path=':memory:')
            with TestClient(app) as client:
                for route in ('state', 'workloads', 'observability', 'pools', 'ray/capabilities'):
                    self.assertEqual(client.get('/api/v1/' + route).status_code, 403)
                    self.assertEqual(client.get('/api/v1/' + route, headers=self.headers).status_code, 403)
                self.assertEqual(client.get('/api/v1/state', headers={'Authorization': 'Bearer ' + 'a' * 64}).status_code, 200)
                self.assertEqual(client.get('/api/v1/execution/status').status_code, 200)
                self.assertEqual(client.get('/api/v1/execution/projects', headers=self.headers).status_code, 200)
    def test_native_job_count_quota_is_required_and_unscoped(self):
        path = '/api/v1/namespaces/team/resourcequotas/davano-execution-bounds'
        self.kube.objects[path]['spec']['scopes'] = ['BestEffort']
        self.assertEqual(self.submit().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_transactional_history_bound_under_concurrency(self):
        from concurrent.futures import ThreadPoolExecutor
        self.service.config.projects[0].history_limit = 1
        def submit(index):
            return self.http.post(self.base + '/workloads', headers=self.headers, json={**self.body, 'name': 'parallel-' + str(index)}).status_code
        with ThreadPoolExecutor(max_workers=6) as executor:
            results = list(executor.map(submit, range(6)))
        self.assertEqual(results.count(201), 1, results)
        self.assertEqual(results.count(409), 5, results)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM execution_jobs').fetchone()[0], 1)
    def test_audit_bound_reserves_outcome_before_write(self):
        with self.store.db:
            self.store.db.executemany('INSERT INTO execution_audit(at,actor,project,operation,workload,outcome,operation_id) VALUES (?,?,?,?,?,?,?)', [('now', 'alice', 'research', 'submit', 'history', 'accepted', str(i)) for i in range(9999)])
        response = self.submit()
        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(self.kube.writes, [])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM execution_audit').fetchone()[0], 9999)
    def test_post_write_audit_failure_explicitly_reports_accepted(self):
        original = self.service.audit
        def audit(*args):
            if args[4] == 'accepted':
                raise ApiError(503, 'disk failure')
            return original(*args)
        with patch.object(self.service, 'audit', side_effect=audit):
            response = self.submit()
        self.assertEqual(response.status_code, 503)
        self.assertIn('Kubernetes accepted this Job', response.text)
        self.assertTrue(self.submit().json()['idempotent'])
    def test_admin_and_project_tokens_must_be_distinct(self):
        with patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'true', 'DAVANO_API_TOKEN': TOKEN}), self.assertRaises(ValueError):
            install_workload_api(FastAPI(), 'kubernetes', self.store, client=self.kube, config=config())
    def test_deployment_keeps_broker_and_runtime_authority_separate(self):
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parent.parent / 'deploy' / 'execution'
        resources = list(yaml.safe_load_all((root / 'project-template.yaml').read_text()))
        runtime = next(o for o in resources if o['kind'] == 'ServiceAccount')
        self.assertFalse(runtime['automountServiceAccountToken'])
        for binding in [o for o in resources if o['kind'] == 'RoleBinding']:
            self.assertEqual(binding['subjects'], [{'kind': 'ServiceAccount', 'name': 'davano-execution-api', 'namespace': 'davano'}])
        role = next(o for o in resources if o['kind'] == 'Role')
        for rule in role['rules']:
            self.assertFalse(set(rule['verbs']) & {'bind', 'escalate', 'impersonate', '*'})
            self.assertNotIn('secrets', rule['resources'])
        patch = yaml.safe_load((root / 'deployment-patch.yaml').read_text())
        data = next(v for v in patch['spec']['template']['spec']['volumes'] if v['name'] == 'data')
        self.assertIsNone(data['emptyDir'])
        self.assertEqual(data['persistentVolumeClaim']['claimName'], 'davano-execution-data')
        self.assertEqual(patch['spec']['replicas'], 1)
    def test_example_config_requires_real_reviewed_values(self):
        from pathlib import Path
        path = Path(__file__).resolve().parent.parent / 'deploy' / 'execution' / 'config.example.json'
        with self.assertRaises(ValidationError):
            ExecutionConfig.model_validate_json(path.read_text())
    def test_canonical_quota_units_are_semantically_equal(self):
        from davano.workload_service import normalized_guard_spec
        from davano.quantities import normalize_quota_hard
        self.assertEqual(normalized_guard_spec({'kind': 'ResourceQuota', 'spec': {'hard': {'requests.nvidia.com/gpu': '1k'}}}),
                         normalized_guard_spec({'kind': 'ResourceQuota', 'spec': {'hard': {'requests.nvidia.com/gpu': '1000'}}}))
        self.assertEqual(normalize_quota_hard({'gpu': '1k'}), {'gpu': '1000'})
        self.assertNotEqual(normalized_guard_spec({'kind': 'ResourceQuota', 'spec': {'hard': {'gpu': '1k'}, 'scopes': ['BestEffort']}}),
                            normalized_guard_spec({'kind': 'ResourceQuota', 'spec': {'hard': {'gpu': '1000'}}}))
    def test_broker_accepts_canonical_native_gpu_quota(self):
        policy_path = cr_path(self.policy)
        self.kube.objects[policy_path]['spec']['gpuQuota'] = 1000
        self.kube.objects[cr_path(self.pool)]['spec']['nominalQuota'] = 1000
        self.kube.objects['/api/v1/nodes/worker']['status']['allocatable']['nvidia.com/gpu'] = '1000'
        Reconciler(self.kube, ['team']).cycle()
        quota = self.kube.objects['/api/v1/namespaces/team/resourcequotas/' + policy_name(self.policy)]
        quota['spec']['hard']['requests.nvidia.com/gpu'] = '1k'
        quota['status']['hard']['requests.nvidia.com/gpu'] = '1k'
        self.kube.writes = []
        self.assertEqual(self.preview().status_code, 200)
        Reconciler(self.kube, ['team']).cycle()
        self.assertFalse(any(path.endswith('/resourcequotas/' + policy_name(self.policy)) and method == 'PUT' for method, path, _ in self.kube.writes))
    def test_enabled_demo_rejected(self):
        with patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'true'}), self.assertRaises(ValueError):
            install_workload_api(FastAPI(), 'demo', self.store, client=self.kube, config=config())

if __name__ == '__main__':
    unittest.main()
