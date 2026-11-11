"""Synthetic adapter-contract tests, not GPU hardware or live-cluster evidence."""
import io
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient
from davano.app import create_app
from davano.inventory import normalize
from davano.kube import ApiError
from davano.observability import (KueueReader, METRICS, ObservabilityService, ObservationUnavailable,
                                  PrometheusReader, normalize_metrics, normalize_queue_resource, pending_reasons)

UUID = 'GPU-12345678-1234-1234-1234-123456789abc'
AMD_UUID = '17ff74b5-0000-1000-805c-37b0fbe69430'
NOW = 1794400000.0
NODES = [{'name': 'gpu-01', 'labels': {'kubernetes.io/hostname': 'host-01'}}]


def series(name='DCGM_FI_DEV_GPU_UTIL', value=35, timestamp=NOW, **labels):
    return {'metric': {'__name__': name, 'UUID': UUID, 'Hostname': 'host-01', 'modelName': 'H100', **labels},
            'values': [[timestamp, str(value)]]}


def metrics(items):
    return normalize_metrics(items, NODES, now_epoch=NOW)[0]


def response(payload):
    data = io.BytesIO(json.dumps(payload).encode())
    data.__enter__ = lambda *_: data
    return data


class MetricContractTests(unittest.TestCase):
    def test_identity_and_source_evidence_not_indices(self):
        devices, warnings = normalize_metrics([series(gpu='0'), series(UUID='', gpu='1')], NODES, NOW)
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]['uuid'], UUID)
        self.assertEqual(devices[0]['node'], 'gpu-01')
        self.assertEqual(devices[0]['provenance']['adapter'], 'nvidia-dcgm')
        self.assertTrue(warnings)
        self.assertNotIn('health', devices[0])

    def test_nvidia_49_units_and_hostname_alias(self):
        dev = metrics([series('DCGM_FI_DEV_GPU_UTIL_RATIO', .7, Hostname='', hostname='gpu-01'),
                       series('DCGM_FI_DEV_FB_USED_BYTES', 3145728, Hostname='', hostname='gpu-01'),
                       series('DCGM_FI_DEV_FB_CAPACITY_BYTES', 85899345920, Hostname='', hostname='gpu-01')])[0]
        self.assertEqual(dev['metrics']['utilization_percent']['value'], 70)
        self.assertEqual(dev['metrics']['memory_used_bytes']['value'], 3145728)
        self.assertEqual(dev['metrics']['memory_total_bytes']['unit'], 'bytes')

    def test_legacy_mib_explicitly_converted(self):
        self.assertEqual(metrics([series('DCGM_FI_DEV_FB_USED', 3)])[0]['metrics']['memory_used_bytes']['value'], 3 * 1024 ** 2)

    def test_new_legacy_equal_aliases_are_not_double_counted(self):
        dev = metrics([series('DCGM_FI_DEV_GPU_UTIL_RATIO', .35), series()])[0]
        self.assertEqual(dev['metrics']['utilization_percent']['value'], 35)

    def test_stale_missing_invalid_future_are_not_zero(self):
        for item, status in [(series(timestamp=NOW - 121), 'stale'), (series(value='NaN'), 'unknown'),
                             (series(value='Inf'), 'unknown'), (series(value=-1), 'unknown'),
                             (series(value=9223372036854775794), 'unknown'), (series(timestamp=NOW + 6), 'unknown')]:
            with self.subTest(item=item):
                dev = metrics([item])[0]
                self.assertIsNone(dev['metrics']['utilization_percent']['value'])
                self.assertEqual(dev['metrics']['utilization_percent']['status'], status)
                self.assertIsNone(dev['metrics']['temperature_celsius']['value'])

    def test_fresh_zero_is_distinct_from_unknown(self):
        dev = metrics([series(value=0)])[0]
        self.assertEqual(dev['metrics']['utilization_percent']['value'], 0)
        self.assertEqual(dev['metrics']['utilization_percent']['status'], 'fresh')

    def test_uses_latest_source_sample_not_matrix_query_time(self):
        item = series()
        item['values'] = [[NOW - 100, '1'], [NOW - 4, '44'], [NOW - 70, '70']]
        metric = metrics([item])[0]['metrics']['utilization_percent']
        self.assertEqual(metric['value'], 44)
        self.assertIn('Z', metric['sample_at'])

    def test_mig_and_compute_instances_are_not_merged_with_parent(self):
        devices = metrics([series(), series(GPU_I_ID='1'), series(GPU_I_ID='1', GPU_CI_ID='0'), series(GPU_I_ID='1', GPU_CI_ID='1')])
        self.assertEqual(len(devices), 4)
        self.assertEqual({d['entity_kind'] for d in devices}, {'gpu', 'mig-instance', 'mig-compute-instance'})
        self.assertEqual(len({d['id'] for d in devices}), 4)

    def test_repeated_parent_sensor_does_not_become_instance_measurement(self):
        dev = metrics([series('DCGM_FI_DEV_POWER_USAGE', 300, GPU_I_ID='1')])[0]
        self.assertIsNone(dev['metrics']['power_watts']['value'])
        self.assertIn('parent', dev['metrics']['power_watts']['reason'])

    def test_workload_attributed_usage_is_not_device_utilization(self):
        dev = metrics([series(value=250, pod='job-1', container='train')])[0]
        self.assertIsNone(dev['metrics']['utilization_percent']['value'])
        self.assertEqual(dev['metrics']['utilization_percent']['scope'], 'workload')
        dev = metrics([series(value=250, pod='job-1'), series(value=20)])[0]
        self.assertEqual(dev['metrics']['utilization_percent']['value'], 20)

    def test_conflicting_targets_are_not_averaged(self):
        dev = metrics([series(value=10, instance='a:9400'), series(value=90, instance='b:9400')])[0]
        self.assertIsNone(dev['metrics']['utilization_percent']['value'])
        self.assertEqual(dev['metrics']['utilization_percent']['status'], 'conflict')

    def test_no_arbitrary_label_or_condition_message_leaks(self):
        dev = metrics([series(password='super-secret', token='abc', pod_label_secret='private')])[0]
        self.assertNotIn('super-secret', json.dumps(dev))
        self.assertNotIn('private', json.dumps(dev))
        obj = {'metadata': {'name': 'job'}, 'status': {'conditions': [{'type': 'Admitted', 'status': 'False', 'reason': 'Pending', 'message': 'Bearer secret'}]}}
        normalized = normalize_queue_resource(obj, 'workloads')
        self.assertNotIn('Bearer secret', json.dumps(normalized))
        self.assertIsNone(normalized['admitted'])

    def test_unknown_or_ambiguous_node_join_does_not_guess(self):
        self.assertEqual(metrics([series(Hostname='other')])[0]['node_match'], 'unmatched')
        nodes = NODES + [{'name': 'host-01', 'labels': {}}]
        dev = normalize_metrics([series()], nodes, NOW)[0][0]
        self.assertEqual(dev['node_match'], 'ambiguous')
        self.assertIsNone(dev['node'])

    def test_federated_clusters_are_never_merged_or_joined_without_selector(self):
        first, second = series(cluster='one'), series(cluster='two')
        devices = metrics([first, second])
        self.assertEqual(len(devices), 2)
        self.assertTrue(all(d['node_match'] == 'unmatched' for d in devices))
        devices, warnings = normalize_metrics([first, second], NODES, NOW, expected_cluster='one')
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices[0]['node'], 'gpu-01')
        self.assertTrue(warnings)

    def test_legacy_mig_uuid_is_preserved(self):
        uuid = 'MIG-' + UUID + '/1/0'
        dev = metrics([series(UUID=uuid)])[0]
        self.assertEqual(dev['uuid'], uuid)
        self.assertEqual(dev['entity_kind'], 'mig-instance')

    def test_amd_requires_uuid_and_does_not_guess_memory_units(self):
        item = series('amd_gpu_gfx_activity', 22, UUID='', gpu_uuid=AMD_UUID, hostname='gpu-01', Hostname='', gpu_compute_partition_type='spx')
        dev = metrics([item])[0]
        self.assertEqual(dev['vendor'], 'AMD')
        self.assertEqual(dev['metrics']['utilization_percent']['value'], 22)
        self.assertIsNone(dev['metrics']['memory_used_bytes']['value'])
        self.assertEqual(metrics([series('gpu_gfx_activity', 22, UUID='', gpu_id='0')]), [])

    def test_amd_partition_zero_power_is_not_claimed_as_sensor_zero(self):
        item = series('gpu_power_usage', 0, UUID='', gpu_uuid=AMD_UUID, hostname='gpu-01', Hostname='', gpu_compute_partition_type='cpx', gpu_partition_id='3')
        dev = metrics([item])[0]
        self.assertEqual(dev['entity_kind'], 'partition')
        self.assertIsNone(dev['metrics']['power_watts']['value'])

    def test_malformed_or_excessive_response_fails_closed(self):
        for value in [None, [None], [{'metric': {}, 'values': [[None, '1']]}]]:
            if value == [{'metric': {}, 'values': [[None, '1']]}]:
                value[0]['metric'] = series()['metric']
            with self.assertRaises(ObservationUnavailable):
                normalize_metrics(value, NODES, NOW)
        with patch('davano.observability.MAX_SERIES', 1):
            with self.assertRaises(ObservationUnavailable):
                normalize_metrics([series(), series()], NODES, NOW)


class PrometheusTransportTests(unittest.TestCase):
    def test_https_origin_only(self):
        for url in ('http://p.example', 'https://u:p@p.example', 'https://p.example/x', 'https://p.example?token=x', 'https://p.example/#x'):
            with self.assertRaises(ValueError):
                PrometheusReader(url)

    def test_fixed_raw_range_query_get_only_with_rotating_file_token(self):
        with tempfile.TemporaryDirectory() as td:
            token_path = Path(td) / 'token'
            token_path.write_text('one')
            seen = []
            def opener(req, **kwargs):
                seen.append(req)
                return response({'status': 'success', 'data': {'resultType': 'matrix', 'result': [series()]}})
            reader = PrometheusReader('https://p.example', str(token_path), opener=opener)
            with patch('davano.observability.ssl.create_default_context'):
                reader.samples()
                token_path.write_text('two')
                reader.samples()
            self.assertEqual([r.get_method() for r in seen], ['GET', 'GET'])
            self.assertEqual([r.get_header('Authorization') for r in seen], ['Bearer one', 'Bearer two'])
            query = parse_qs(urlparse(seen[0].full_url).query)['query'][0]
            self.assertTrue(query.endswith('[15m]'))
            self.assertNotIn('token', seen[0].full_url)

    def test_cluster_selector_is_fixed_server_configuration(self):
        seen = []
        def opener(req, **kwargs):
            seen.append(req)
            return response({'status': 'success', 'data': {'resultType': 'matrix', 'result': []}})
        reader = PrometheusReader('https://p.example', opener=opener, cluster='gpu-prod')
        with patch('davano.observability.ssl.create_default_context'):
            reader.samples()
        self.assertIn('cluster="gpu-prod"', parse_qs(urlparse(seen[0].full_url).query)['query'][0])
        with self.assertRaises(ValueError):
            PrometheusReader('https://p.example', cluster='x"} or {__name__=~".*')

    def test_http_errors_do_not_expose_server_body_or_token(self):
        error = HTTPError('https://p.example?secret=x', 403, 'private password', {}, io.BytesIO(b'private bearer credential'))
        reader = PrometheusReader('https://p.example', opener=Mock(side_effect=error))
        with patch('davano.observability.ssl.create_default_context'):
            with self.assertRaises(ObservationUnavailable) as caught:
                reader.samples()
        self.assertEqual(str(caught.exception), 'Prometheus read failed (HTTP 403).')

    def test_matrix_required_and_partial_results_rejected(self):
        for payload in ({'status': 'error', 'error': 'credential'}, [],
                        {'status': 'success', 'data': {'resultType': 'vector', 'result': []}},
                        {'status': 'success', 'warnings': ['partial'], 'data': {'resultType': 'matrix', 'result': []}}):
            reader = PrometheusReader('https://p.example', opener=lambda *a, **k: response(payload))
            with patch('davano.observability.ssl.create_default_context'):
                with self.assertRaises(ObservationUnavailable):
                    reader.samples()

    def test_default_transport_rejects_redirects(self):
        from davano.inventory import open_without_redirect
        reader = PrometheusReader('https://p.example')
        self.assertIs(reader.opener, open_without_redirect)


class QueueContractTests(unittest.TestCase):
    def test_version_discovery_and_lists_only(self):
        class Client:
            def __init__(self): self.paths = []
            def get(self, path):
                self.paths.append(('GET', path))
                if path == '/apis/kueue.x-k8s.io': return {'versions': [{'version': 'v1beta1'}, {'version': 'v1beta2'}]}
                return {'resources': [{'name': n, 'verbs': ['list']} for n in ('localqueues', 'clusterqueues', 'workloads')]}
            def list(self, path):
                self.paths.append(('LIST', path))
                return []
        client = Client()
        state = KueueReader(client).snapshot()
        self.assertEqual(state['api_version'], 'v1beta2')
        self.assertEqual(len(client.paths), 5)
        self.assertTrue(all(method in ('GET', 'LIST') for method, path in client.paths))

    def test_generation_current_required_for_admission(self):
        resource = {'metadata': {'name': 'job', 'namespace': 'research', 'generation': 2}, 'spec': {'queueName': 'team'}, 'status': {'conditions': [
            {'type': 'Admitted', 'status': 'True', 'observedGeneration': 1, 'reason': 'Admitted'}]}}
        self.assertIsNone(normalize_queue_resource(resource, 'workloads')['admitted'])
        resource['status']['conditions'][0]['observedGeneration'] = 2
        self.assertTrue(normalize_queue_resource(resource, 'workloads')['admitted'])
        resource['status']['conditions'][0]['status'] = 'False'
        self.assertFalse(normalize_queue_resource(resource, 'workloads')['admitted'])

    def test_no_invented_reason_from_pending_phase(self):
        reasons = pending_reasons({'status': {'phase': 'Pending'}})
        self.assertEqual(reasons[0]['reason'], 'NotBound')
        self.assertNotIn('quota exhausted', json.dumps(reasons))

    def test_observed_scheduler_and_container_reasons(self):
        pod = {'metadata': {'name': 'job'}, 'spec': {'containers': [{'resources': {'requests': {'nvidia.com/gpu': '1'}}}]},
               'status': {'phase': 'Pending', 'conditions': [{'type': 'PodScheduled', 'status': 'False', 'reason': 'Unschedulable', 'message': 'Do not leak'}],
                          'containerStatuses': [{'state': {'waiting': {'reason': 'ImagePullBackOff', 'message': 'secret'}}}]}}
        workloads = normalize([], [pod])[1]
        self.assertEqual([r['reason'] for r in workloads[0]['pending_reasons']], ['Unschedulable', 'ImagePullBackOff'])
        self.assertNotIn('Do not leak', json.dumps(workloads))
        self.assertNotIn('secret', json.dumps(workloads))

    def test_queue_observer_template_is_read_only_and_not_bound(self):
        import yaml
        root = Path(__file__).resolve().parents[1]
        obj = yaml.safe_load((root / 'deploy/observability/queue-observer-clusterrole.yaml').read_text())
        self.assertEqual(obj['kind'], 'ClusterRole')
        for rule in obj['rules']:
            self.assertTrue(set(rule['verbs']) <= {'get', 'list'})
        for path in (root / 'deploy').rglob('kustomization.yaml'):
            self.assertNotIn('observability', path.read_text())

    def test_queue_failure_does_not_fail_inventory_or_claim_empty_backlog(self):
        reader = Mock()
        reader.snapshot.side_effect = ApiError(403, 'Bearer credential')
        state = ObservabilityService('kubernetes', queues=reader).snapshot([], 'now')
        self.assertEqual(state['queues']['status'], 'unavailable')
        self.assertNotIn('credential', json.dumps(state))
        self.assertNotEqual(state['queues']['status'], 'connected')


class ObservabilityAPITests(unittest.TestCase):
    def test_default_demo_returns_unknown_not_fabricated_metrics(self):
        with TestClient(create_app(db_path=':memory:')) as client:
            state = client.get('/api/v1/observability').json()
            self.assertEqual(state['telemetry']['status'], 'disabled')
            self.assertEqual(state['telemetry']['devices'], [])
            self.assertTrue(state['read_only'])
            self.assertEqual(client.get('/api/v1/state').json()['observability']['telemetry'], state['telemetry'])

    def test_demo_ignores_configured_live_readers(self):
        prometheus, queues = Mock(), Mock()
        state = ObservabilityService('demo', prometheus=prometheus, queues=queues).snapshot([], 'now')
        prometheus.samples.assert_not_called()
        queues.snapshot.assert_not_called()
        self.assertEqual(state['telemetry']['devices'], [])

    def test_transport_failure_has_no_metrics_fallback(self):
        prometheus = Mock()
        prometheus.samples.side_effect = ObservationUnavailable('Prometheus read unavailable.')
        state = ObservabilityService('kubernetes', prometheus=prometheus).snapshot([], 'now')
        self.assertEqual(state['telemetry']['status'], 'unavailable')
        self.assertEqual(state['telemetry']['devices'], [])

    def test_bad_configuration_exposes_no_embedded_password(self):
        with patch.dict('os.environ', {'DAVANO_PROMETHEUS_URL': 'https://user:private@prom.example'}):
            state = ObservabilityService('kubernetes').snapshot([], 'now')
        self.assertEqual(state['telemetry']['status'], 'invalid-config')
        self.assertNotIn('private', json.dumps(state))


if __name__ == '__main__':
    unittest.main()
