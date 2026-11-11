"""Queued replacement contracts against API doubles; no live execution proof."""
import copy
import json
import unittest
from unittest.mock import patch
from urllib.parse import urlsplit
from davano.kube import ApiError, ROOT
from davano.reconciler import KUEUE, Reconciler, cr_path
from davano.workload_models import WorkloadCreate
from davano.workload_reassignment import WorkloadReassignments
from test_workload_execution import setup, TOKEN, VIEWER, OTHER


class ReassignmentTests(unittest.TestCase):
    def setUp(self):
        self.kube, self.pool, self.policy, self.store, self.execution, self.http = setup()
        self.headers = {'Authorization': 'Bearer ' + TOKEN}
        self.base = '/api/v1/execution/projects/research'
        self.orig = self.kube.request
        def request(method, path, body=None):
            clean = urlsplit(path).path
            if method == 'GET' and clean.endswith('/workloads'):
                return {'items': [copy.deepcopy(o) for p, o in self.kube.objects.items() if p.rsplit('/', 1)[0] == clean]}
            return self.orig(method, path, body)
        self.kube.request = request
        response = self.http.post(self.base + '/workloads', headers=self.headers, json={'name': 'source', 'profile': 'smoke', 'gpu_count': 1})
        self.assertEqual(response.status_code, 201, response.text)
        self.source = response.json()['workload']
        self.body = {'uid': self.source['uid'], 'resourceVersion': self.source['resourceVersion'], 'target_project': 'research', 'target': {'name': 'replacement', 'profile': 'smoke', 'gpu_count': 2}}
        self.source_path = self.execution.job_path(self.execution.config.projects[0], 'source')
        self.target_path = self.execution.job_path(self.execution.config.projects[0], 'replacement')
        self.kube.writes = []
    def tearDown(self):
        self.http.close()
        self.store.close()
    def preview(self, headers=None):
        return self.http.post(self.base + '/workloads/source/reassignment/preview', headers=headers or self.headers, json=self.body)
    def begin(self, payload=None, headers=None):
        if payload is None:
            response = self.preview()
            self.assertEqual(response.status_code, 200, response.text)
            payload = {**self.body, 'plan_fingerprint': response.json()['plan_fingerprint']}
        return self.http.post(self.base + '/workloads/source/reassignments', headers=headers or self.headers, json=payload)
    def advance(self, operation):
        return self.http.post('/api/v1/execution/reassignments/' + operation['id'] + '/advance', headers=self.headers, json={})
    def test_preview_is_read_only_and_reports_real_semantics(self):
        response = self.preview()
        self.assertEqual(response.status_code, 200, response.text)
        value = response.json()
        self.assertEqual(value['semantics'], 'cancel-and-recreate')
        self.assertEqual(value['target']['manifest']['spec']['template']['spec']['containers'][0]['resources']['limits']['nvidia.com/gpu'], '2')
        self.assertNotIn('_plan', value)
        self.assertEqual(self.kube.writes, [])
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM execution_reassignments').fetchone()[0], 0)
    def test_complete_replacement_deletes_before_target_submit(self):
        response = self.begin()
        self.assertEqual(response.status_code, 202, response.text)
        operation = response.json()
        self.assertEqual(operation['phase'], 'TargetSubmitted', operation)
        self.assertTrue(operation['source_deleted'])
        self.assertTrue(operation['target_submitted'])
        self.assertFalse(operation['allocated'])
        self.assertNotIn(self.source_path, self.kube.objects)
        self.assertIn(self.target_path, self.kube.objects)
        methods = [method for method, _, _ in self.kube.writes]
        self.assertEqual(methods, ['DELETE', 'POST'])
        self.kube.writes = []
        self.assertEqual(self.advance(operation).json()['phase'], 'TargetSubmitted')
        self.assertEqual(self.kube.writes, [])
    def test_running_source_cannot_move(self):
        self.kube.objects[self.source_path]['status'] = {'active': 1, 'startTime': '2026-11-11T00:00:00Z'}
        self.assertEqual(self.preview().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_previously_executed_suspended_source_blocked(self):
        self.kube.objects[self.source_path]['status'] = {'conditions': [{'type': 'Suspended', 'status': 'False'}]}
        self.assertEqual(self.preview().status_code, 409)
    def test_owned_pod_blocks_even_if_terminal(self):
        self.kube.seed('/api/v1/namespaces/team/pods/child', {'kind': 'Pod', 'metadata': {'name': 'child', 'ownerReferences': [{'kind': 'Job', 'uid': self.source['uid']}]}, 'status': {'phase': 'Succeeded'}})
        self.assertEqual(self.preview().status_code, 409)
    def test_kueue_admission_or_execution_history_blocks(self):
        path = KUEUE + '/namespaces/team/workloads/source-workload'
        obj = {'kind': 'Workload', 'metadata': {'name': 'source-workload', 'ownerReferences': [{'kind': 'Job', 'uid': self.source['uid']}]}}
        for status in ({'admission': {'clusterQueue': 'queue'}}, {'conditions': [{'type': 'QuotaReserved', 'status': 'True'}]}, {'conditions': [{'type': 'Evicted', 'status': 'True'}]}, {'requeueState': {'count': 1}}):
            self.kube.seed(path, {**obj, 'status': status})
            self.assertEqual(self.preview().status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_source_revision_mismatch_and_same_name_block(self):
        self.body['resourceVersion'] = '1'
        self.assertEqual(self.preview().status_code, 409)
        self.body['resourceVersion'] = self.source['resourceVersion']
        self.body['target']['name'] = 'source'
        self.assertEqual(self.preview().status_code, 422)
    def test_viewer_can_preview_but_cannot_begin(self):
        headers = {'Authorization': 'Bearer ' + VIEWER}
        response = self.preview(headers)
        self.assertEqual(response.status_code, 200)
        applied = self.begin({**self.body, 'plan_fingerprint': response.json()['plan_fingerprint']}, headers)
        self.assertEqual(applied.status_code, 403)
        self.assertEqual(self.kube.writes, [])
    def test_unassigned_target_project_not_disclosed(self):
        self.body['target_project'] = 'other'
        self.assertEqual(self.preview().status_code, 404)
        self.assertEqual(self.kube.writes, [])
    def test_plan_drift_before_apply_leaves_source_untouched(self):
        preview = self.preview().json()
        self.execution.config.profiles[0].memory_mib = 2048
        response = self.begin({**self.body, 'plan_fingerprint': preview['plan_fingerprint']})
        self.assertEqual(response.status_code, 409, response.text)
        self.assertIn(self.source_path, self.kube.objects)
        self.assertEqual(self.kube.writes, [])
    def test_waits_for_actual_source_deletion_and_target_reserved(self):
        original = self.kube.request
        def request(method, path, body=None):
            if method == 'DELETE' and path == self.source_path:
                self.kube.writes.append((method, path, copy.deepcopy(body)))
                self.kube.objects[path]['metadata']['deletionTimestamp'] = '2026-11-11T00:00:00Z'
                return {'status': 'Success'}
            return original(method, path, body)
        with patch.object(self.kube, 'request', side_effect=request):
            operation = self.begin().json()
        self.assertEqual(operation['phase'], 'WaitingForSourceDeletion', operation)
        self.assertNotIn(self.target_path, self.kube.objects)
        direct = self.http.post(self.base + '/workloads', headers=self.headers, json=self.body['target'])
        self.assertEqual(direct.status_code, 409)
        del self.kube.objects[self.source_path]
        resumed = self.advance(operation)
        self.assertEqual(resumed.json()['phase'], 'TargetSubmitted', resumed.text)
    def test_waits_for_owned_kueue_child_absence(self):
        path = KUEUE + '/namespaces/team/workloads/source-workload'
        self.kube.seed(path, {'kind': 'Workload', 'metadata': {'name': 'source-workload', 'ownerReferences': [{'kind': 'Job', 'uid': self.source['uid']}]}, 'status': {'conditions': [{'type': 'Admitted', 'status': 'False'}]}})
        operation = self.begin().json()
        self.assertEqual(operation['phase'], 'WaitingForSourceDeletion', operation)
        self.assertNotIn(self.target_path, self.kube.objects)
        del self.kube.objects[path]
        self.assertEqual(self.advance(operation).json()['phase'], 'TargetSubmitted')
    def test_target_api_failure_after_delete_resumable_not_atomic(self):
        self.kube.fail = 'POST'
        operation = self.begin().json()
        self.assertEqual(operation['phase'], 'NeedsAttention', operation)
        self.assertTrue(operation['source_deleted'])
        self.assertFalse(operation['target_submitted'])
        self.assertNotIn(self.source_path, self.kube.objects)
        self.kube.fail = None
        self.assertEqual(self.advance(operation).json()['phase'], 'TargetSubmitted')
    def test_changed_target_profile_after_delete_never_submits(self):
        self.kube.fail = 'POST'
        operation = self.begin().json()
        self.kube.fail = None
        self.execution.config.profiles[0].image = 'other/image@sha256:' + 'c' * 64
        resumed = self.advance(operation).json()
        self.assertEqual(resumed['phase'], 'NeedsAttention')
        self.assertNotIn(self.target_path, self.kube.objects)
    def test_unknown_delete_does_not_infer_unexecuted_source(self):
        original = self.kube.request
        state = {'once': False}
        def request(method, path, body=None):
            result = original(method, path, body)
            if method == 'DELETE' and not state['once']:
                state['once'] = True
                raise ApiError(503, 'Delete response lost')
            return result
        with patch.object(self.kube, 'request', side_effect=request):
            operation = self.begin().json()
            self.assertEqual(operation['phase'], 'NeedsAttention')
            self.assertNotIn(self.target_path, self.kube.objects)
            resumed = self.advance(operation).json()
        self.assertEqual(resumed['phase'], 'NeedsAttention')
        self.assertIn('execution history is uncertain', resumed['detail'])
        self.assertNotIn(self.target_path, self.kube.objects)
        self.assertEqual(len([w for w in self.kube.writes if w[0] == 'DELETE']), 1)
    def test_unknown_target_submit_recovers_without_duplicate(self):
        original = self.kube.request
        state = {'once': False}
        def request(method, path, body=None):
            result = original(method, path, body)
            if method == 'POST' and not state['once']:
                state['once'] = True
                raise ApiError(503, 'Submit response lost')
            return result
        with patch.object(self.kube, 'request', side_effect=request):
            operation = self.begin().json()
            self.assertEqual(operation['phase'], 'NeedsAttention')
            self.assertEqual(self.advance(operation).json()['phase'], 'TargetSubmitted')
        self.assertEqual(len([w for w in self.kube.writes if w[0] == 'POST']), 1)
    def test_source_starting_after_preview_is_not_deleted(self):
        value = self.preview().json()
        self.kube.objects[self.source_path]['metadata']['resourceVersion'] = '900'
        self.kube.objects[self.source_path]['spec']['suspend'] = False
        response = self.begin({**self.body, 'plan_fingerprint': value['plan_fingerprint']})
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.kube.writes, [])
    def test_unrelated_jobs_children_are_never_deleted(self):
        path = '/api/v1/namespaces/team/pods/foreign'
        self.kube.seed(path, {'kind': 'Pod', 'metadata': {'name': 'foreign', 'ownerReferences': [{'kind': 'Job', 'uid': 'other-uid'}]}})
        operation = self.begin().json()
        self.assertEqual(operation['phase'], 'TargetSubmitted')
        self.assertIn(path, self.kube.objects)
        self.assertFalse(any('/pods/' in path for _, path, _ in self.kube.writes))
    def test_operation_reads_and_lists_scoped(self):
        operation = self.begin().json()
        url = '/api/v1/execution/reassignments'
        self.assertEqual(self.http.get(url, headers=self.headers).json()['items'][0]['id'], operation['id'])
        other = {'Authorization': 'Bearer ' + OTHER}
        self.assertEqual(self.http.get(url, headers=other).json()['items'], [])
        self.assertEqual(self.http.get(url + '/' + operation['id'], headers=other).status_code, 404)
        self.assertEqual(self.http.post(url + '/' + operation['id'] + '/advance', headers=other, json={}).status_code, 404)
    def test_durable_operation_recovers_after_service_restart(self):
        self.kube.fail = 'POST'
        operation = self.begin().json()
        self.kube.fail = None
        restored = WorkloadReassignments(self.execution)
        project = self.execution.config.projects[0]
        result = restored.advance(operation['id'], project, project, self.execution.config.identities[0])
        self.assertEqual(result['phase'], 'TargetSubmitted')
    def test_external_source_disappearance_before_delete_attempt_blocks(self):
        with patch.object(self.execution, 'cancel', side_effect=ApiError(503, 'Audit unavailable before DELETE')):
            operation = self.begin().json()
        self.assertEqual(operation['source_deletion_state'], 'not-requested')
        del self.kube.objects[self.source_path]
        response = self.advance(operation).json()
        self.assertEqual(response['phase'], 'NeedsAttention')
        self.assertIn('without this operation recording', response['detail'])
        self.assertNotIn(self.target_path, self.kube.objects)
    def test_external_deleting_source_before_own_attempt_blocks(self):
        with patch.object(self.execution, 'cancel', side_effect=ApiError(503, 'Audit unavailable before DELETE')):
            operation = self.begin().json()
        self.kube.objects[self.source_path]['metadata']['deletionTimestamp'] = '2026-11-11T00:00:00Z'
        response = self.advance(operation).json()
        self.assertEqual(response['phase'], 'NeedsAttention')
        self.assertIn('externally marked', response['detail'])
        self.assertNotIn(self.target_path, self.kube.objects)
    def test_definitively_rejected_delete_cannot_authorize_disappearance(self):
        original = self.kube.request
        def request(method, path, body=None):
            if method == 'DELETE':
                raise ApiError(403, 'Denied')
            return original(method, path, body)
        with patch.object(self.kube, 'request', side_effect=request):
            operation = self.begin().json()
        self.assertEqual(operation['source_deletion_state'], 'rejected')
        del self.kube.objects[self.source_path]
        response = self.advance(operation).json()
        self.assertEqual(response['phase'], 'NeedsAttention')
        self.assertNotIn(self.target_path, self.kube.objects)
    def test_failed_delete_request_then_external_cleanup_stays_blocked(self):
        original = self.kube.request
        def request(method, path, body=None):
            if method == 'DELETE':
                raise ApiError(503, 'Request never reached API server')
            return original(method, path, body)
        with patch.object(self.kube, 'request', side_effect=request):
            operation = self.begin().json()
        self.assertEqual(operation['source_deletion_state'], 'attempted')
        # The source could have run before another administrator removed it.
        del self.kube.objects[self.source_path]
        response = self.advance(operation).json()
        self.assertEqual(response['phase'], 'NeedsAttention')
        self.assertNotIn(self.target_path, self.kube.objects)
    def test_confirmed_delete_survives_later_audit_result_failure(self):
        original = self.execution.audit
        def audit(*args):
            if args[2] == 'cancel' and args[4] == 'accepted':
                raise ApiError(503, 'Audit outcome failed after accepted DELETE')
            return original(*args)
        with patch.object(self.execution, 'audit', side_effect=audit):
            operation = self.begin().json()
        self.assertEqual(operation['source_deletion_state'], 'accepted')
        self.assertEqual(operation['phase'], 'NeedsAttention')
        self.assertEqual(self.advance(operation).json()['phase'], 'TargetSubmitted')
    def test_cross_project_replacement_targets_configured_namespace(self):
        source_project, target = self.execution.config.projects
        target.pool_uid = source_project.pool_uid
        self.execution.config.identities[0].projects.append('other')
        other_policy = copy.deepcopy(self.policy)
        other_policy['metadata'].update(name='other', namespace='other-team', uid='policy-2')
        self.kube.seed(cr_path(other_policy), other_policy)
        namespace = copy.deepcopy(self.kube.objects['/api/v1/namespaces/team'])
        namespace['metadata'].update(name='other-team', uid='namespace-2')
        namespace['metadata']['labels']['fleet.davano.io/project'] = 'other'
        self.kube.seed('/api/v1/namespaces/other-team', namespace)
        for path in ('serviceaccounts/davano-runtime', 'resourcequotas/davano-execution-bounds'):
            item = copy.deepcopy(self.kube.objects['/api/v1/namespaces/team/' + path])
            if path.startswith('serviceaccounts'):
                item['metadata']['uid'] = 'runtime-2'
            self.kube.seed('/api/v1/namespaces/other-team/' + path, item)
        self.kube.seed('/apis/networking.k8s.io/v1/namespaces/other-team/networkpolicies/deny-all', copy.deepcopy(self.kube.objects['/apis/networking.k8s.io/v1/namespaces/team/networkpolicies/deny-all']))
        Reconciler(self.kube, ['team', 'other-team']).cycle()
        self.kube.writes = []
        self.body['target_project'] = 'other'
        operation = self.begin().json()
        self.assertEqual(operation['phase'], 'TargetSubmitted', operation)
        self.assertEqual(operation['workload']['namespace'], 'other-team')
        self.assertNotIn(self.source_path, self.kube.objects)
        self.assertIn(self.execution.job_path(target, 'replacement'), self.kube.objects)

if __name__ == '__main__':
    unittest.main()
