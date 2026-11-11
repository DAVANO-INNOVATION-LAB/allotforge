"""Mocked Kubernetes allocation contracts; not live admission/scheduling proof."""
import copy
import os
import unittest
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
from davano.allocation_models import AllocationUpdate
from davano.allocation_service import AllocationService
from davano.control_api import install_control_api
from davano.kube import ApiError, OWNER_LABEL, ROOT
from davano.reconciler import Reconciler, cr_path, access_resources
from test_control import fixture


class AllocationTests(unittest.TestCase):
    def setUp(self):
        self.client, self.pool, self.policy = fixture()
        Reconciler(self.client, ['team']).cycle()
        self.current = self.client.get(cr_path(self.policy))
        self.body = dict(uid=self.current['metadata']['uid'], resourceVersion=self.current['metadata']['resourceVersion'], poolRef='a', gpuQuota=6, maxGPUsPerPod=3)
        self.service = AllocationService(self.client)
        self.client.writes.clear()

    def run_update(self, apply=False):
        return self.service.update('team', 'research', AllocationUpdate(**self.body), apply)

    def test_preview_is_read_only(self):
        result = self.run_update()
        self.assertEqual(result['status'], 'preview')
        self.assertEqual(result['before']['gpuQuota'], 4)
        self.assertEqual(result['desired']['gpuQuota'], 6)
        self.assertEqual(self.client.writes, [])

    def test_apply_is_only_exact_policy_cas(self):
        original = copy.deepcopy(self.current)
        result = self.run_update(True)
        self.assertEqual(result['status'], 'pending-reconciliation')
        self.assertEqual(len(self.client.writes), 1)
        method, path, body = self.client.writes[0]
        self.assertEqual((method, path), ('PUT', cr_path(self.policy)))
        self.assertEqual(body['metadata'], original['metadata'])
        self.assertEqual(body['spec']['subjects'], original['spec']['subjects'])
        self.assertEqual(body['spec']['gpuQuota'], 6)
        with self.assertRaises(ApiError): self.run_update(True)

    def test_reduction_and_zero_do_not_evict_existing_pods(self):
        self.body['gpuQuota'] = 0
        self.client.seed('/api/v1/namespaces/team/pods/running', {'kind': 'Pod', 'metadata': {'name': 'running'}, 'status': {'phase': 'Running'}})
        self.run_update(True)
        self.assertEqual(len(self.client.writes), 1)
        self.assertIn('/api/v1/namespaces/team/pods/running', self.client.objects)

    def test_stale_identity_and_pool_change_rejected_without_writes(self):
        for field, value in [('uid', 'other-uid'), ('resourceVersion', '999'), ('poolRef', 'other'), ('gpuQuota', 9)]:
            with self.subTest(field=field):
                old = self.body[field]; self.body[field] = value
                with self.assertRaises(ApiError): self.run_update(True)
                self.body[field] = old
        self.assertEqual(self.client.writes, [])

    def test_policy_pending_or_terminating_blocks(self):
        for field, value in [('generation', 999), ('deletionTimestamp', 'now')]:
            with self.subTest(field=field):
                original = copy.deepcopy(self.client.objects[cr_path(self.policy)])
                self.client.objects[cr_path(self.policy)]['metadata'][field] = value
                with self.assertRaises(ApiError): self.run_update(True)
                self.client.objects[cr_path(self.policy)] = original
        self.assertEqual(self.client.writes, [])

    def test_native_guard_drift_missing_or_foreign_blocks(self):
        for collection, desired in access_resources(self.current, self.pool):
            path = collection + '/' + desired['metadata']['name']
            original = copy.deepcopy(self.client.objects[path])
            for mutation in ('foreign', 'drift', 'missing'):
                with self.subTest(kind=desired['kind'], mutation=mutation):
                    if mutation == 'foreign': self.client.objects[path]['metadata']['labels'][OWNER_LABEL] = 'foreign'
                    elif mutation == 'drift': self.client.objects[path]['spec']['unexpected'] = 'drift'
                    else: del self.client.objects[path]
                    with self.assertRaises(ApiError): self.run_update(True)
                    self.client.objects[path] = copy.deepcopy(original)
        self.assertEqual(self.client.writes, [])

    def test_cas_race_rejected(self):
        request = self.client.request
        def race(method, path, body=None):
            if method == 'PUT': self.client.objects[path]['metadata']['resourceVersion'] = '999'
            return request(method, path, body)
        self.client.request = race
        with self.assertRaises(ApiError) as error: self.run_update(True)
        self.assertEqual(error.exception.status, 409)
        self.assertEqual(self.client.objects[cr_path(self.policy)]['spec']['gpuQuota'], 4)

    def test_apply_rechecks_native_state_after_preview(self):
        self.run_update()
        collection, desired = access_resources(self.current, self.pool)[0]
        self.client.objects[collection + '/' + desired['metadata']['name']]['status']['hard'] = {}
        with self.assertRaises(ApiError): self.run_update(True)
        self.assertEqual(self.client.writes, [])

    def test_api_authentication_and_strict_input(self):
        with patch.dict(os.environ, {'DAVANO_CONTROL_ENABLED': 'true', 'DAVANO_API_TOKEN': 'x'*32}):
            app = FastAPI(); install_control_api(app, 'kubernetes', self.client)
        api = TestClient(app)
        path = '/api/v1/control/policies/team/research/allocation/preview'
        self.assertEqual(api.post(path, json=self.body).status_code, 401)
        headers = {'Authorization': 'Bearer ' + 'x'*32}
        self.assertEqual(api.post(path, json=self.body, headers=headers).status_code, 200)
        for extra in ({'gpuQuota': True}, {'cpu': 4}, {'resourceVersion': ''}, {'gpuQuota': -1}):
            self.assertEqual(api.post(path, json={**self.body, **extra}, headers=headers).status_code, 422)
        self.assertEqual(self.client.writes, [])

    def test_api_disabled_without_cluster_calls(self):
        with patch.dict(os.environ, {'DAVANO_CONTROL_ENABLED': 'false'}):
            app = FastAPI(); install_control_api(app, 'fixture', self.client)
        self.client.fail = 'GET'
        response = TestClient(app).post('/api/v1/control/policies/team/research/allocation/apply', json=self.body)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(self.client.writes, [])

    def test_optional_rbac_grants_exact_policy_update_and_native_reads_only(self):
        from pathlib import Path
        import yaml
        path = Path(__file__).resolve().parent.parent / 'deploy/examples/allocation-api-rbac.yaml'
        objects = list(yaml.safe_load_all(path.read_text()))
        cluster_role = next(o for o in objects if o['kind'] == 'ClusterRole')
        self.assertTrue(all(rule['verbs'] == ['get'] and rule.get('resourceNames') for rule in cluster_role['rules']))
        role = next(o for o in objects if o['kind'] == 'Role')
        for rule in role['rules']:
            self.assertTrue(rule.get('resourceNames'))
            if 'gpuaccesspolicies' in rule['resources']:
                self.assertEqual(set(rule['verbs']), {'get', 'update'})
            else:
                self.assertEqual(rule['verbs'], ['get'])
        for binding in [o for o in objects if o['kind'].endswith('Binding')]:
            self.assertEqual(binding['subjects'], [{'kind': 'ServiceAccount', 'name': 'davano-control-api', 'namespace': 'davano'}])

    def test_api_canonical_native_quota_accepted(self):
        self.client.objects[cr_path(self.policy)]['spec']['gpuQuota'] = 1000
        self.client.objects[cr_path(self.pool)]['spec']['nominalQuota'] = 1000
        self.client.objects['/api/v1/nodes/worker']['status']['allocatable']['nvidia.com/gpu'] = '1000'
        Reconciler(self.client, ['team']).cycle()
        self.current = self.client.get(cr_path(self.policy))
        self.body['resourceVersion'] = self.current['metadata']['resourceVersion']
        collection, desired = access_resources(self.current, self.pool)[0]
        quota = self.client.objects[collection + '/' + desired['metadata']['name']]
        quota['spec']['hard']['requests.nvidia.com/gpu'] = '1k'
        quota['status']['hard']['requests.nvidia.com/gpu'] = '1k'
        self.client.writes.clear()
        self.assertEqual(self.run_update()['status'], 'preview')
        self.assertEqual(self.client.writes, [])
