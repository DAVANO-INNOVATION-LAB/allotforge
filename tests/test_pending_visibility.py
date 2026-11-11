"""Synthetic Kueue visibility contracts; no live cluster or scheduling claims."""
import json
import unittest
from unittest.mock import Mock, patch

from davano.kube import ApiError
from davano.observability import KueueReader, ObservabilityService, normalize_queue_resource
from davano.pending_visibility import GROUP, LIMIT, PendingVisibilityReader, parse_targets


def workload():
    return normalize_queue_resource({'metadata': {'name': 'job', 'namespace': 'team', 'uid': 'uid-1', 'generation': 2},
                                    'spec': {'queueName': 'queue'}, 'status': {'conditions': [
                                        {'type': k, 'status': 'False', 'observedGeneration': 2} for k in ('Admitted', 'QuotaReserved')]}}, 'workloads')


def entry():
    return {'metadata': {'name': 'job', 'namespace': 'team', 'uid': 'uid-1'}, 'localQueueName': 'queue',
            'positionInLocalQueue': 0, 'positionInClusterQueue': 3}


class Client:
    def __init__(self, items=None, version='v1beta2'):
        self.items = [entry()] if items is None else items
        self.version = version
        self.paths = []
    def get(self, path):
        self.paths.append(path)
        if path == GROUP:
            return {'versions': [{'version': self.version}]}
        if path == GROUP + '/' + self.version:
            return {'resources': [{'name': 'localqueues/pendingworkloads', 'namespaced': True, 'verbs': ['get']}]}
        return {'apiVersion': 'visibility.kueue.x-k8s.io/' + self.version, 'kind': 'PendingWorkloadsSummary', 'items': self.items}


class PendingVisibilityTests(unittest.TestCase):
    def run_reader(self, client=None, workloads=None):
        workloads = [workload()] if workloads is None else workloads
        result = PendingVisibilityReader(client or Client(), [('team', 'queue')]).snapshot(workloads)
        return result, workloads

    def test_discovery_exact_scoped_bounded_get_and_zero_preserved(self):
        client = Client()
        result, ws = self.run_reader(client)
        self.assertEqual(result['status'], 'connected')
        self.assertEqual(ws[0]['pending_position']['local_queue_position'], 0)
        self.assertEqual(ws[0]['pending_position']['cluster_queue_position'], 3)
        self.assertEqual(len(client.paths), 3)
        self.assertEqual(client.paths[-1], GROUP + '/v1beta2/namespaces/team/localqueues/queue/pendingworkloads?limit=100&offset=0')
        self.assertNotIn('eta', ws[0]['pending_position'])
        self.assertNotIn('total', result['targets'][0])

    def test_v1beta1_only_when_served_and_unknown_version(self):
        self.assertEqual(self.run_reader(Client(version='v1beta1'))[0]['api_version'], 'v1beta1')
        client = Client(version='v9')
        self.assertEqual(self.run_reader(client)[0]['status'], 'unavailable')
        self.assertEqual(len(client.paths), 1)

    def test_discovery_must_advertise_namespaced_get(self):
        for bad in ({'resources': []}, {'resources': [{'name': 'localqueues/pendingworkloads', 'namespaced': False, 'verbs': ['get']}]},
                    {'resources': [{'name': 'localqueues/pendingworkloads', 'namespaced': True, 'verbs': ['list']}]}):
            client = Mock()
            client.get.side_effect = [{'versions': [{'version': 'v1beta2'}]}, bad]
            self.assertEqual(self.run_reader(client)[0]['status'], 'unavailable')
            self.assertEqual(client.get.call_count, 2)

    def test_upstream_omitted_uid_never_uses_owner_uid(self):
        item = entry()
        del item['metadata']['uid']
        item['metadata']['ownerReferences'] = [{'uid': 'uid-1'}]
        result, ws = self.run_reader(Client([item]))
        self.assertEqual(result['status'], 'connected')
        self.assertEqual(ws[0]['pending_position']['status'], 'unknown')
        self.assertIn('omits Workload UID', ws[0]['pending_position']['reason'])
        report = result['targets'][0]['reported_entries'][0]
        self.assertEqual(report['identity_status'], 'unverified')
        self.assertEqual(report['local_queue_position'], 0)
        self.assertNotIn('uid', report)
        self.assertNotIn('ownerReferences', report)

    def test_identity_and_generation_fail_closed(self):
        for field, value in [('uid', 'new-uid'), ('queue', 'different'), ('namespace', 'other'), ('name', 'other')]:
            w = workload()
            w[field] = value
            self.assertEqual(self.run_reader(workloads=[w])[1][0]['pending_position']['status'], 'unknown')
        for flag in ('current_generation', 'status'):
            w = workload()
            w['conditions'][0][flag] = False if flag == 'current_generation' else 'True'
            self.assertEqual(self.run_reader(workloads=[w])[1][0]['pending_position']['status'], 'unknown')
        ws = [workload(), workload()]
        self.assertTrue(all(w['pending_position']['status'] == 'unknown' for w in self.run_reader(workloads=ws)[1]))

    def test_malformed_duplicate_missing_rank_never_partially_join(self):
        decreasing = entry()
        decreasing['metadata'] = {'namespace': 'team', 'name': 'other', 'uid': 'uid-2'}
        decreasing.update(positionInLocalQueue=1, positionInClusterQueue=2)
        cases = [[entry(), entry()], [None], [{}], [entry(), decreasing]]
        for field, value in [('positionInLocalQueue', True), ('positionInLocalQueue', -1), ('positionInClusterQueue', None), ('localQueueName', 'other')]:
            item = entry()
            item[field] = value
            cases.append([item])
        item = entry()
        del item['positionInLocalQueue']
        cases.append([item])
        for items in cases:
            with self.subTest(items=items):
                result, ws = self.run_reader(Client(items))
                self.assertEqual(result['status'], 'unavailable')
                self.assertEqual(ws[0]['pending_position']['status'], 'unknown')
                self.assertEqual(result['targets'][0]['reported_entries'], [])

    def test_bounds_and_full_window_unknown(self):
        for raw in ('', '../x', 'ns/q?x=1', 'ns/q,ns/q', ','.join(f'ns/q{i}' for i in range(11)), 'x' * 4001, 'bad.ns/q'):
            with self.assertRaises(ValueError): parse_targets(raw)
        self.assertEqual(len(parse_targets(','.join(f'ns/q{i}' for i in range(10)))), 10)
        items = []
        for i in range(LIMIT):
            item = entry()
            item['metadata'].update(name=f'job-{i}', uid=f'uid-{i}')
            item.update(positionInLocalQueue=i, positionInClusterQueue=i)
            items.append(item)
        result, _ = self.run_reader(Client(items))
        self.assertEqual(result['status'], 'partial')
        result, _ = self.run_reader(Client(items + [entry()]))
        self.assertEqual(result['status'], 'unavailable')

    def test_errors_sanitized_and_no_retry(self):
        for status in (403, 404, 500):
            client = Mock()
            client.get.side_effect = ApiError(status, 'secret-token')
            result, ws = self.run_reader(client)
            self.assertEqual(client.get.call_count, 1)
            self.assertNotIn('secret-token', json.dumps(result))
            self.assertEqual(ws[0]['pending_position']['status'], 'unknown')

    def test_deadline_stops_requests_and_rejects_late_result(self):
        client = Client()
        ticks = iter([0, 0, 13, 13, 13])
        result = PendingVisibilityReader(client, [('team', 'queue')], clock=lambda: next(ticks)).snapshot([workload()])
        self.assertEqual(result['status'], 'unavailable')
        self.assertEqual(len(client.paths), 1)

    def test_no_arbitrary_metadata_or_messages_leak(self):
        item = entry()
        item['metadata'].update(labels={'secret': 'SECRET'}, annotations={'secret': 'SECRET'})
        item['message'] = 'SECRET'
        result, ws = self.run_reader(Client([item]))
        self.assertNotIn('SECRET', json.dumps([result, ws]))

    def test_optional_failure_preserves_basic_observations(self):
        client = Mock()
        client.get.side_effect = [{'versions': [{'version': 'v1beta2'}]},
                                  {'resources': [{'name': n, 'verbs': ['list']} for n in ('localqueues', 'clusterqueues', 'workloads')]}, ApiError(403, 'secret')]
        client.list.return_value = []
        with patch.dict('os.environ', {'DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED': 'true', 'DAVANO_KUEUE_PENDING_LOCAL_QUEUES': 'team/queue'}):
            state = KueueReader(client).snapshot()
        self.assertEqual(state['status'], 'connected')
        self.assertEqual(state['pending_visibility']['status'], 'unavailable')

    def test_dns_subdomain_labels_are_validated(self):
        for name in ('a..b', 'a.-b', 'a-.b', 'a' * 64, '.a', 'a.', 'A', '.'.join(['a' * 63] * 4)):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    parse_targets('team/' + name)
                item = entry()
                item['metadata']['name'] = name
                result, _ = self.run_reader(Client([item]))
                self.assertEqual(result['status'], 'unavailable')
                self.assertEqual(result['targets'][0]['reported_entries'], [])
        for name in ('a', 'a-b', 'a.b', 'a' * 63, '.'.join(['a' * 63] * 3 + ['a' * 61])):
            self.assertEqual(parse_targets('team/' + name), [('team', name)])
        for namespace in ('a.b', 'a' * 64, '-a'):
            with self.assertRaises(ValueError):
                parse_targets(namespace + '/queue')

    def test_duplicate_uid_across_names_not_bound(self):
        other = workload()
        other['name'] = 'different'
        _, ws = self.run_reader(workloads=[workload(), other])
        self.assertTrue(all(w['pending_position']['status'] == 'unknown' for w in ws))

    def test_failed_target_preserves_next_target_and_no_extra_calls(self):
        class MultiClient(Client):
            def get(self, path):
                if '/namespaces/bad/' in path:
                    self.paths.append(path)
                    raise ApiError(403, 'SECRET')
                return super().get(path)
        client = MultiClient()
        ws = [workload()]
        result = PendingVisibilityReader(client, [('bad', 'queue'), ('team', 'queue')]).snapshot(ws)
        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['targets'][0]['reported_entries'], [])
        self.assertEqual(result['targets'][1]['returned_count'], 1)
        self.assertEqual(ws[0]['pending_position']['status'], 'observed')
        self.assertEqual(len(client.paths), 4)

    def test_warning_or_continuation_rejected(self):
        for extra in ({'warnings': ['SECRET']}, {'metadata': {'continue': 'SECRET'}}):
            class PartialClient(Client):
                def get(self, path):
                    value = super().get(path)
                    if 'pendingworkloads?' in path:
                        value.update(extra)
                    return value
            result, ws = self.run_reader(PartialClient())
            self.assertEqual(result['targets'][0]['reported_entries'], [])
            self.assertEqual(ws[0]['pending_position']['status'], 'unknown')
            self.assertNotIn('SECRET', json.dumps(result))

    def test_template_get_only_scoped_and_unbound(self):
        from pathlib import Path
        import yaml
        root = Path(__file__).resolve().parents[1]
        objects = list(yaml.safe_load_all((root / 'deploy/observability/pending-visibility-role.yaml').read_text()))
        self.assertEqual([o['kind'] for o in objects], ['Role', 'ClusterRole'])
        self.assertEqual(objects[0]['rules'][0]['resources'], ['localqueues/pendingworkloads'])
        self.assertTrue(objects[0]['rules'][0]['resourceNames'])
        for obj in objects:
            for rule in obj['rules']:
                self.assertEqual(rule['verbs'], ['get'])
        for manifest in (root / 'deploy').rglob('kustomization.yaml'):
            self.assertNotIn('pending-visibility', manifest.read_text())

    def test_invalid_config_no_visibility_calls_and_demo_isolation(self):
        with patch.dict('os.environ', {'DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED': 'true', 'DAVANO_KUEUE_PENDING_LOCAL_QUEUES': '../secret'}):
            reader = KueueReader(Mock())
            self.assertIsNone(reader.pending)
            self.assertTrue(reader.pending_error)
        reader = Mock()
        state = ObservabilityService('demo', queues=reader).snapshot([], 'now')
        reader.snapshot.assert_not_called()
        self.assertEqual(state['queues']['pending_visibility']['status'], 'disabled')
        self.assertEqual(state['queues']['workloads'], [])


if __name__ == '__main__':
    unittest.main()
