"""Synthetic adapter contract checks, not live CPU/GPU performance measurements."""
import copy
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from fastapi.testclient import TestClient
from davano.app import create_app
from davano.workload_advisor import (METRICS, POLICY, WINDOW, WorkloadAdvisor,
                                     WorkloadPrometheusReader, workload_inventory)
from davano.observability import ObservationUnavailable, iso

NOW = 1_800_000_000
NODE = {'metadata': {'name': 'gpu-node', 'labels': {'nvidia.com/gpu.sharing-strategy': 'none'}}}
POD = {'metadata': {'name': 'train', 'namespace': 'research', 'uid': 'pod-1'},
       'spec': {'nodeName': 'gpu-node', 'containers': [{'name': 'main', 'resources': {'requests': {'cpu': '2', 'memory': '4Gi', 'nvidia.com/gpu': '1'}}}]},
       'status': {'phase': 'Running', 'containerStatuses': [{'name': 'main', 'containerID': 'containerd://abc123', 'state': {'running': {'startedAt': iso(NOW - 1800)}}}]}}


def data(pod=None, gpu=5, cpu=1):
    pod = pod or POD
    series = []
    for c, s in zip(pod['spec']['containers'], pod['status']['containerStatuses']):
        for key, value in [('cpu_cores', cpu), ('memory_bytes', 1048576), ('gpu_busy_percent', gpu)]:
            series.append({'metric': {'__name__': METRICS[key][0], 'namespace': pod['metadata']['namespace'],
                                     'pod': pod['metadata']['name'], 'pod_uid': pod['metadata']['uid'],
                                     'node': pod['spec']['nodeName'], 'container': c['name'],
                                     'container_id': s['containerID'], 'scope': 'container',
                                     'attribution': 'container-runtime', 'gpu_count': '1'},
                           'values': [[NOW - 900 + 15 * n, str(value)] for n in range(61)]})
    return series


class Reader:
    cluster = None
    def __init__(self, series): self.series = series
    def samples(self): return self.series


def observe(series=None, pod=None, source='kubernetes', nodes=None):
    workloads = workload_inventory([pod or POD], nodes or [NODE])
    return WorkloadAdvisor(source, Reader(data() if series is None else series)).snapshot(workloads, iso(NOW), NOW)


class WorkloadAdvisorTests(unittest.TestCase):
    def item(self, series=None, pod=None, **kwargs):
        return observe(series, pod, **kwargs)['workloads'][0]

    def assertUnknown(self, item):
        self.assertEqual(item['assessment']['verdict'], 'insufficient-evidence')
        self.assertEqual(item['assessment']['confidence'], 'none')
        self.assertIs(item['assessment']['automatic_action'], False)

    def test_complete_low_activity_proposes_experiment_not_placement(self):
        item = self.item()
        self.assertEqual(item['assessment']['verdict'], 'cpu-comparison-candidate')
        self.assertEqual(item['assessment']['confidence'], 'limited')
        self.assertEqual(item['assessment']['performance'], 'unknown')
        self.assertEqual(item['assessment']['cost'], 'unknown')
        self.assertIn('unproven', item['assessment']['reason'])
        self.assertEqual(item['metrics']['gpu_busy_percent']['mean'], 5)
        self.assertEqual(item['metrics']['cpu_cores']['coverage_percent'], 100)
        self.assertEqual(len(item['comparison_plan']['steps']), 6)
        self.assertNotIn('containers', item)

    def test_demo_never_consumes_reader_values(self):
        result = observe(source='demo')
        self.assertEqual(result['status'], 'disabled')
        self.assertUnknown(result['workloads'][0])
        self.assertIsNone(result['workloads'][0]['metrics']['gpu_busy_percent']['mean'])

    def test_cpu_only_pods_are_monitored_without_claim_of_optimality(self):
        pod = copy.deepcopy(POD)
        del pod['spec']['containers'][0]['resources']['requests']['nvidia.com/gpu']
        item = self.item(data(pod), pod)
        self.assertEqual(item['requests']['gpu_count'], 0)
        self.assertEqual(item['assessment']['verdict'], 'cpu-requested')
        self.assertIsNone(item['metrics']['gpu_busy_percent']['mean'])
        self.assertEqual(item['metrics']['cpu_cores']['mean'], 1)

    def test_custom_accelerator_is_not_claimed_absent(self):
        pod = copy.deepcopy(POD)
        requests = pod['spec']['containers'][0]['resources']['requests']
        del requests['nvidia.com/gpu']
        requests['vendor.example/accelerator'] = '1'
        item = self.item([], pod)
        self.assertEqual(item['requests']['gpu_count'], 0)
        self.assertIn('No supported GPU resource', item['assessment']['reason'])
        self.assertIn('Custom accelerator resources are not classified', item['assessment']['reason'])

    def test_mixed_custom_resources_block_candidate_but_preserve_observations(self):
        for section, value in [('requests', '1'), ('limits', '1'), ('requests', 'bad')]:
            pod = copy.deepcopy(POD)
            pod['spec']['containers'][0]['resources'].setdefault(section, {})['vendor.example/accelerator'] = value
            item = self.item(data(pod), pod)
            self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'observed')
            self.assertEqual(item['assessment']['verdict'], 'inconclusive')
            self.assertIn('Unclassified extended resources', item['assessment']['reason'])
            self.assertFalse(item['assessment']['basis']['recognized_resources_only'])
            self.assertEqual(self.item(data(pod, gpu=70), pod)['assessment']['verdict'], 'gpu-activity-observed')
        pod = copy.deepcopy(POD)
        pod['spec']['containers'][0]['resources']['requests']['vendor.example/license'] = '0'
        self.assertEqual(self.item(data(pod), pod)['assessment']['verdict'], 'cpu-comparison-candidate')
        pod['spec']['containers'][0]['resources']['limits'] = {'vendor.example/license': '1'}
        self.assertEqual(self.item(data(pod), pod)['assessment']['verdict'], 'inconclusive')

    def test_cpu_and_memory_requests_are_not_measurements(self):
        item = self.item([])
        self.assertEqual(item['requests'], {'gpu_count': 1, 'cpu_millicores': 2000, 'memory_mib': 4096})
        self.assertUnknown(item)
        self.assertIsNone(item['metrics']['cpu_cores']['mean'])

    def test_active_gpu_does_not_prove_best_resource(self):
        item = self.item(data(gpu=70))
        self.assertEqual(item['assessment']['verdict'], 'gpu-activity-observed')
        self.assertIn('does not prove', item['assessment']['reason'])

    def test_intermediate_or_idle_cpu_are_inconclusive(self):
        for series in [data(gpu=35), data(gpu=5, cpu=0)]:
            self.assertEqual(self.item(series)['assessment']['verdict'], 'inconclusive')

    def test_candidate_means_use_only_aligned_buckets(self):
        series = data()
        for timestamp_value in series[0]['values']:
            timestamp_value[1] = '1' if timestamp_value[0] < NOW - 720 else '0'
        series[2]['values'] = [sample for sample in series[2]['values'] if sample[0] >= NOW - 720]
        item = self.item(series)
        self.assertGreater(item['metrics']['cpu_cores']['mean'], .1)
        self.assertEqual(item['assessment']['verdict'], 'inconclusive')
        self.assertEqual(item['assessment']['basis']['gpu_container_cpu_mean_cores'], 0)

    def test_cpu_work_in_non_gpu_container_cannot_drive_suitability(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'].append({'name': 'logger', 'resources': {'requests': {'cpu': '1'}}})
        pod['status']['containerStatuses'].append({'name': 'logger', 'containerID': 'containerd://logger', 'state': {'running': {'startedAt': iso(NOW - 1800)}}})
        series = data(pod, cpu=0)
        for sample in series[3]['values']: sample[1] = '1'
        item = self.item(series, pod)
        self.assertEqual(item['metrics']['cpu_cores']['mean'], 1)
        self.assertEqual(item['assessment']['verdict'], 'inconclusive')
        self.assertEqual(item['assessment']['basis']['gpu_container_cpu_mean_cores'], 0)

    def test_multigpu_average_cannot_hide_single_hot_gpu(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'][0]['resources']['requests']['nvidia.com/gpu'] = '8'
        series = data(pod, gpu=12.5); series[2]['metric']['gpu_count'] = '8'
        item = self.item(series, pod)
        self.assertEqual(item['metrics']['gpu_busy_percent']['mean'], 12.5)
        self.assertEqual(item['assessment']['verdict'], 'inconclusive')
        self.assertFalse(item['assessment']['basis']['one_gpu_per_container'])

    def test_phase_metadata_is_whitelisted(self):
        for phase in [{'token': 'secret-token'}, 'secret-token', [], None, True]:
            pod = copy.deepcopy(POD); pod['status']['phase'] = phase
            item = self.item(pod=pod)
            self.assertEqual(item['phase'], 'Unknown')
            self.assertNotIn('secret-token', json.dumps(item))
            from davano.inventory import normalize
            self.assertNotIn('secret-token', json.dumps(normalize([NODE], [pod])))
            self.assertUnknown(item)

    def test_real_zero_is_preserved(self):
        self.assertEqual(self.item(data(gpu=0))['metrics']['gpu_busy_percent']['mean'], 0)
        self.assertEqual(self.item(data(gpu=0))['assessment']['verdict'], 'cpu-comparison-candidate')

    def test_brief_gpu_spike_blocks_cpu_candidate(self):
        series = data()
        series[2]['values'][10][1] = '99'
        self.assertEqual(self.item(series)['assessment']['verdict'], 'inconclusive')
        self.assertEqual(self.item(series)['metrics']['gpu_busy_percent']['peak'], 99)

    def test_missing_or_mismatched_identity_never_name_joins(self):
        for label, value in [('pod_uid', 'old'), ('container_id', 'containerd://old'), ('node', 'other'), ('namespace', 'other'), ('pod_uid', None), ('container_id', None), ('cluster', 'other')]:
            with self.subTest(label=label, value=value):
                series = data()
                series[2]['metric'][label] = value
                self.assertUnknown(self.item(series))

    def test_attribution_and_gpu_count_are_explicit(self):
        for label, value in [('scope', 'device'), ('attribution', None), ('gpu_count', '2'), ('gpu_count', 1)]:
            series = data()
            series[2]['metric'][label] = value
            self.assertUnknown(self.item(series))

    def test_stock_dcgm_pod_labels_cannot_become_workload_activity(self):
        series = data()
        series[2]['metric']['__name__'] = 'DCGM_FI_DEV_GPU_UTIL'
        self.assertUnknown(self.item(series))

    def test_duplicate_series_are_conflicts_even_if_identical(self):
        series = data()
        series.append(copy.deepcopy(series[2]))
        item = self.item(series)
        self.assertUnknown(item)
        self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'conflict')

    def test_invalid_samples_never_become_zero(self):
        for value in ['NaN', 'Inf', '-1', '101', True, None, {}, 'bad']:
            with self.subTest(value=value):
                series = data()
                series[2]['values'][20][1] = value
                item = self.item(series)
                self.assertUnknown(item)
                self.assertIsNone(item['metrics']['gpu_busy_percent']['mean'])

    def test_stale_samples_are_not_numeric(self):
        series = data()
        series[2]['values'] = series[2]['values'][:-9]
        item = self.item(series)
        self.assertUnknown(item)
        self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'stale')

    def test_future_and_invalid_timestamps_fail_closed(self):
        for timestamp in [NOW + 10, float('nan'), True, -1]:
            series = data()
            series[2]['values'][20][0] = timestamp
            self.assertUnknown(self.item(series))

    def test_minimum_samples_and_span(self):
        for values in [data()[2]['values'][::2], data()[2]['values'][-20:]]:
            series = data(); series[2]['values'] = values
            self.assertUnknown(self.item(series))

    def test_duplicate_timestamps_do_not_increase_coverage(self):
        series = data(); series[2]['values'] = [series[2]['values'][-1]] * 61
        self.assertUnknown(self.item(series))

    def test_conflicting_duplicate_timestamp_is_unknown(self):
        series = data(); series[2]['values'].append([NOW - 300, '75'])
        item = self.item(series)
        self.assertUnknown(item)
        self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'conflict')

    def test_large_hole_rejects_apparently_long_window(self):
        series = data(); del series[2]['values'][10:18]
        self.assertUnknown(self.item(series))

    def test_coverage_threshold_requires_twelve_minutes(self):
        # 41 samples over 600 seconds but only 11 occupied minute buckets.
        series = data(); series[2]['values'] = series[2]['values'][-41:]
        item = self.item(series)
        self.assertUnknown(item)
        self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'insufficient')

    def test_runtime_eligibility_is_required(self):
        for change in ['uid', 'container_id', 'start', 'pending', 'completed', 'init', 'ephemeral', 'pod-resources', 'duplicate', 'malformed-state']:
            pod = copy.deepcopy(POD)
            if change == 'uid': pod['metadata'].pop('uid')
            if change == 'container_id': pod['status']['containerStatuses'][0].pop('containerID')
            if change == 'start': pod['status']['containerStatuses'][0]['state']['running']['startedAt'] = iso(NOW - 300)
            if change == 'pending': pod['status']['phase'] = 'Pending'
            if change == 'completed': pod['status']['phase'] = 'Succeeded'
            if change == 'init': pod['spec']['initContainers'] = [{'name': 'prepare'}]
            if change == 'ephemeral': pod['spec']['ephemeralContainers'] = [{'name': 'debug'}]
            if change == 'pod-resources': pod['spec']['resources'] = {'requests': {'cpu': '4'}}
            if change == 'malformed-state': pod['status']['containerStatuses'][0]['state'] = None
            if change == 'duplicate': pod['status']['containerStatuses'].append(copy.deepcopy(pod['status']['containerStatuses'][0]))
            with self.subTest(change=change): self.assertUnknown(self.item(data(), pod))

    def test_multicontainer_requires_every_cpu_and_requested_gpu(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'].append({'name': 'second', 'resources': {'requests': {'cpu': '2', 'nvidia.com/gpu': '1'}}})
        pod['status']['containerStatuses'].append({'name': 'second', 'containerID': 'containerd://def456', 'state': {'running': {'startedAt': iso(NOW - 1800)}}})
        series = data(pod)
        item = self.item(series, pod)
        self.assertEqual(item['metrics']['cpu_cores']['mean'], 2)
        self.assertEqual(item['metrics']['gpu_busy_percent']['mean'], 5)
        self.assertEqual(item['requests']['gpu_count'], 2)
        self.assertUnknown(self.item(series[:-1], pod))
        self.assertUnknown(self.item(series[1:], pod))

    def test_shared_mig_and_unknown_mode_block_comparison(self):
        for labels in [{'nvidia.com/gpu.sharing-strategy': 'time-slicing'}, {'nvidia.com/gpu.sharing-strategy': 'none', 'nvidia.com/mig.strategy': 'single'}, {}]:
            node = {'metadata': {'name': 'gpu-node', 'labels': labels}}
            self.assertUnknown(self.item(nodes=[node]))

    def test_fractional_gpu_requests_cannot_round_to_whole(self):
        from davano.inventory import quantity
        for value in ['0.999999999999999999999999999999999', '1.0000000000000000000000000001', '1' * 101, '1e99999999', 1.0]:
            with self.subTest(value=value):
                self.assertIsNone(quantity(value))
                pod = copy.deepcopy(POD)
                pod['spec']['containers'][0]['resources']['requests']['nvidia.com/gpu'] = value
                item = self.item(pod=pod)
                self.assertIsNone(item['requests']['gpu_count'])
                self.assertUnknown(item)
        for value in ['1.0000000000000000000000000000', '1000m', '1000000000n']:
            self.assertEqual(quantity(value), 1)

    def test_unknown_gpu_count_or_overhead_cannot_be_ignored(self):
        for overhead in ['1', 'bad']:
            pod = copy.deepcopy(POD); pod['spec']['overhead'] = {'nvidia.com/gpu': overhead}
            self.assertUnknown(self.item(pod=pod))

    def test_selected_cluster_requires_exact_label(self):
        reader = Reader(data()); reader.cluster = 'primary'
        advisor = WorkloadAdvisor('kubernetes', reader)
        workloads = workload_inventory([POD], [NODE])
        self.assertUnknown(advisor.snapshot(workloads, iso(NOW), NOW)['workloads'][0])
        for item in reader.series: item['metric']['cluster'] = 'primary'
        self.assertEqual(advisor.snapshot(workloads, iso(NOW), NOW)['workloads'][0]['assessment']['verdict'], 'cpu-comparison-candidate')

    def test_malformed_envelope_and_sample_limits_reject_all(self):
        for series in [None, {}, [{'metric': {}, 'values': None}], [{}]]:
            advisor = WorkloadAdvisor('kubernetes', Reader(series))
            result = advisor.snapshot(workload_inventory([POD], [NODE]), iso(NOW), NOW)
            self.assertEqual(result['status'], 'unavailable')
            self.assertUnknown(result['workloads'][0])
        with patch('davano.workload_advisor.MAX_SAMPLES', 3):
            self.assertEqual(observe()['status'], 'unavailable')

    def test_failed_read_clears_previous_observation(self):
        reader = Reader(data()); advisor = WorkloadAdvisor('kubernetes', reader)
        workloads = workload_inventory([POD], [NODE])
        self.assertEqual(advisor.snapshot(workloads, iso(NOW), NOW)['status'], 'connected')
        reader.samples = lambda: (_ for _ in ()).throw(ObservationUnavailable('Read unavailable.'))
        result = advisor.snapshot(workloads, iso(NOW), NOW)
        self.assertEqual(result['status'], 'unavailable')
        self.assertUnknown(result['workloads'][0])

    def test_config_is_opt_in_and_transport_is_shared(self):
        with patch.dict(os.environ, {'DAVANO_PROMETHEUS_URL': 'https://metrics.test', 'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'false'}):
            self.assertIsNone(WorkloadAdvisor('kubernetes').reader)
        with patch.dict(os.environ, {'DAVANO_PROMETHEUS_URL': 'http://metrics.test', 'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true'}):
            self.assertEqual(WorkloadAdvisor('kubernetes').snapshot([], iso(NOW), NOW)['status'], 'invalid-config')
        captured = []
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, maximum): return json.dumps({'status':'success','data':{'resultType':'matrix','result':[]}}).encode()
        def opener(request, **kwargs): captured.append(request.full_url); return Response()
        WorkloadPrometheusReader('https://metrics.test', opener=opener, cluster='primary').samples()
        query = parse_qs(urlparse(captured[0]).query)['query'][0]
        self.assertIn('allotforge_container_cpu_usage_cores', query)
        self.assertIn('cluster="primary"', query)
        self.assertIn('[15m]', query)
        self.assertNotIn('DCGM', query)

    def test_deep_json_response_fails_closed(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, maximum): return b'[' * 12000 + b'0' + b']' * 12000
        reader = WorkloadPrometheusReader('https://metrics.test', opener=lambda *args, **kwargs: Response())
        with self.assertRaises(ObservationUnavailable): reader.samples()
        result = WorkloadAdvisor('kubernetes', reader).snapshot(workload_inventory([POD], [NODE]), iso(NOW), NOW)
        self.assertEqual(result['status'], 'unavailable')
        self.assertUnknown(result['workloads'][0])

    def test_source_window_and_policy_are_explicit(self):
        result = observe()
        self.assertEqual(result['window_seconds'], WINDOW)
        self.assertEqual(result['policy'], POLICY)
        self.assertEqual(result['window_end'], iso(NOW))
        self.assertEqual(result['window_start'], iso(NOW - WINDOW))
        self.assertEqual(result['source'], 'kubernetes')
        self.assertTrue(result['read_only'])

    def test_optional_readers_run_together(self):
        # No timing assertion: a barrier fails if these independent readers run serially.
        import threading
        from davano.service import FleetService
        from davano.store import Store
        barrier = threading.Barrier(2)
        class Inventory:
            source = 'kubernetes'
            def snapshot(self):
                return {'nodes': [], 'workloads': [], 'warnings': [], 'observed_at': iso(NOW)}
        class Observations:
            def snapshot(self, *args):
                barrier.wait(timeout=2)
                return {'telemetry': {'status': 'disabled', 'detail': 'test'}}
        class Advisor:
            reader = True
            def snapshot(self, *args):
                barrier.wait(timeout=2)
                return {'status': 'no-data'}
        with tempfile.TemporaryDirectory() as directory:
            store = Store(str(Path(directory) / 'test.db'), source='kubernetes')
            try:
                self.assertEqual(FleetService(Inventory(), store, Observations(), Advisor()).state()['workload_advisor']['status'], 'no-data')
            finally:
                store.close()

    def test_demo_state_includes_cpu_only_pods_and_no_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            with TestClient(create_app(db_path=str(Path(directory) / 'test.db'))) as client:
                response = client.get('/api/v1/state')
                self.assertEqual(response.status_code, 200)
                advisor = response.json()['workload_advisor']
                self.assertGreater(len(advisor['workloads']), 0)
                self.assertTrue(any(w['requests']['gpu_count'] == 0 for w in advisor['workloads']))
                self.assertTrue(all(w['metrics']['cpu_cores']['mean'] is None for w in advisor['workloads']))

    def test_execution_boundary_protects_advisor_with_inventory(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'DAVANO_EXECUTION_ENABLED': 'true'}), patch('davano.workload_api.install_workload_api'):
            with TestClient(create_app(db_path=str(Path(directory) / 'test.db'))) as client:
                self.assertEqual(client.get('/api/v1/state').status_code, 403)

if __name__ == '__main__': unittest.main()
