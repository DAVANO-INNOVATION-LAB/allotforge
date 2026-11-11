"""Synthetic cAdvisor source contracts; no live cluster or hardware measurements."""
import copy
import json
import os
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from davano import cadvisor
from davano.observability import MAX_RESPONSE_BYTES, ObservationUnavailable, iso
from davano.workload_advisor import (METRICS, WorkloadAdvisor, WorkloadPrometheusReader,
                                     workload_inventory)

NOW = 1_800_000_000
UID = '12345678-1234-5678-9abc-123456789abc'
CONTAINER = 'a1' * 32
NODE = {'metadata': {'name': 'worker-1'}}
POD = {'metadata': {'namespace': 'research', 'name': 'training', 'uid': UID},
       'spec': {'nodeName': 'worker-1', 'containers': [{'name': 'main', 'resources': {'requests': {'cpu': '2', 'memory': '4Gi', 'nvidia.com/gpu': '1'}}}]},
       'status': {'phase': 'Running', 'containerStatuses': [{'name': 'main', 'containerID': 'containerd://' + CONTAINER, 'state': {'running': {'startedAt': iso(NOW - 1800)}}}]}}
ENABLED = {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true', 'DAVANO_WORKLOAD_METRICS_SOURCE': cadvisor.SOURCE,
           'DAVANO_CADVISOR_TIMESTAMPS_PRESERVED': 'true'}


def samples(pod=None, path=None, step=15):
    pod = POD if pod is None else pod
    identifier = pod['status']['containerStatuses'][0]['containerID'].split('://')[1]
    path = path or '/kubepods/burstable/pod' + pod['metadata']['uid'] + '/' + identifier
    labels = {'namespace': pod['metadata']['namespace'], 'pod': pod['metadata']['name'],
              'container': pod['spec']['containers'][0]['name'], 'node': pod['spec']['nodeName'], 'id': path}
    return [{'metric': dict(labels, __name__=cadvisor.CPU_METRIC, cpu='total'),
             'values': [[NOW - 900 + n * step, str(100000 + n * step * 1.5)] for n in range(900 // step + 1)]},
            {'metric': dict(labels, __name__=cadvisor.MEMORY_METRIC),
             'values': [[NOW - 900 + n * step, str(1024 ** 3)] for n in range(900 // step + 1)]}]


class Reader:
    def __init__(self, series, cluster=None):
        self.series, self.cluster, self.calls = series, cluster, 0

    def samples(self):
        self.calls += 1
        return self.series


def observe(series=None, pod=None, cluster=None, source='kubernetes', env=None, reader=None):
    reader = Reader(samples() if series is None else series, cluster) if reader is None else reader
    with patch.dict(os.environ, dict(ENABLED, **(env or {})), clear=True):
        advisor = WorkloadAdvisor(source, reader)
    return advisor.snapshot(workload_inventory([POD if pod is None else pod], [NODE]), iso(NOW), NOW)


class CadvisorObservationTests(unittest.TestCase):
    def metric(self, series=None, name='cpu_cores', **kwargs):
        return observe(series, **kwargs)['workloads'][0]['metrics'][name]

    def assert_no_verdict(self, result):
        item = result['workloads'][0]
        self.assertEqual(item['assessment']['verdict'], 'insufficient-evidence')
        self.assertEqual(item['assessment']['confidence'], 'none')
        self.assertEqual(item['assessment']['performance'], 'unknown')
        self.assertEqual(item['assessment']['cost'], 'unknown')
        self.assertFalse(item['assessment']['automatic_action'])
        self.assertEqual(item['metrics']['gpu_busy_percent']['status'], 'unknown')
        self.assertIsNone(item['metrics']['gpu_busy_percent']['source_metric'])
        self.assertIn('no workload GPU evidence', item['metrics']['gpu_busy_percent']['reason'])

    def test_complete_cpu_memory_have_honest_source_and_temporal_semantics(self):
        result = observe()
        self.assertEqual(result['status'], 'connected')
        self.assertEqual(result['measurement_source'], cadvisor.MEASUREMENT_SOURCE)
        self.assertEqual(result['metrics_source'], cadvisor.SOURCE)
        cpu, memory = (result['workloads'][0]['metrics'][key] for key in ('cpu_cores', 'memory_bytes'))
        self.assertEqual(cpu['mean'], 1.5)
        self.assertEqual(cpu['p95'], 1.5)
        self.assertEqual(cpu['peak'], 1.5)
        self.assertEqual(cpu['sample_start'], iso(NOW - 885))
        self.assertEqual(cpu['sample_end'], iso(NOW))
        self.assertEqual(cpu['sample_count'], 60)
        self.assertEqual(cpu['coverage_percent'], 100)
        self.assertEqual(cpu['source_metric'], cadvisor.CPU_METRIC)
        self.assertEqual(memory['mean'], 1024 ** 3)
        self.assertEqual(memory['sample_start'], iso(NOW - 900))
        self.assertEqual(memory['sample_count'], 61)
        self.assertEqual(memory['source_metric'], cadvisor.MEMORY_METRIC)
        self.assertIn('interval-average', cpu['provenance']['temporal_semantics'])
        self.assertEqual(memory['provenance']['temporal_semantics'], 'source-timestamp-gauge')
        self.assertIn('not instantaneous', cpu['reason'])
        self.assertIn('not RSS', memory['reason'])
        self.assertEqual(result['input_requirements']['source_timestamps'], 'administrator-asserted')
        self.assertIn('not independently verified', result['input_requirements']['detail'])
        self.assertEqual(result['input_requirements']['minimum_cpu_counter_samples'], 42)
        self.assert_no_verdict(result)

    def test_cpu_requested_or_custom_accelerator_never_produces_suitability(self):
        for requests in ({}, {'example.com/accelerator': '1'}):
            pod = copy.deepcopy(POD)
            pod['spec']['containers'][0]['resources']['requests'] = requests
            self.assert_no_verdict(observe(samples(pod), pod=pod))

    def test_zero_is_valid_only_with_complete_evidence(self):
        series = samples()
        for sample in series[0]['values']: sample[1] = '100000'
        for sample in series[1]['values']: sample[1] = '0'
        self.assertEqual(self.metric(series)['mean'], 0)
        self.assertEqual(self.metric(series, name='memory_bytes')['mean'], 0)
        self.assertIsNone(self.metric([])['mean'])

    def test_custom_gpu_and_custom_cpu_metrics_are_never_consumed(self):
        series = samples()
        for key, source in METRICS.items():
            extra = copy.deepcopy(series[0])
            extra['metric'].update(__name__=source[0], pod_uid=UID, container_id='containerd://' + CONTAINER,
                                   attribution='container-runtime', scope='container', gpu_count='1')
            extra['values'] = [[t, '5'] for t, _ in extra['values']]
            series.append(extra)
        self.assert_no_verdict(observe(series))
        self.assertEqual(self.metric(series)['mean'], 1.5)
        self.assertIsNone(self.metric(series[2:])['mean'])

    def test_all_twelve_exact_runtime_path_forms(self):
        escaped = UID.replace('-', '_')
        for runtime, fs_leaf, system_leaf in [('containerd', CONTAINER, 'cri-containerd-' + CONTAINER + '.scope'),
                                              ('cri-o', 'crio-' + CONTAINER, 'crio-' + CONTAINER + '.scope')]:
            pod = copy.deepcopy(POD)
            pod['status']['containerStatuses'][0]['containerID'] = runtime + '://' + CONTAINER
            paths = [f'/kubepods/pod{UID}/{fs_leaf}', f'/kubepods/burstable/pod{UID}/{fs_leaf}',
                     f'/kubepods/besteffort/pod{UID}/{fs_leaf}',
                     f'/kubepods.slice/kubepods-pod{escaped}.slice/{system_leaf}',
                     f'/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod{escaped}.slice/{system_leaf}',
                     f'/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod{escaped}.slice/{system_leaf}']
            self.assertEqual(set(cadvisor.supported_paths(UID, runtime + '://' + CONTAINER)), set(paths))
            for path in paths:
                with self.subTest(runtime=runtime, path=path):
                    self.assertEqual(self.metric(samples(pod, path), pod=pod)['status'], 'observed')

    def test_paths_never_accept_arbitrary_roots_substrings_or_descendants(self):
        path = samples()[0]['metric']['id']
        invalid = ['/' + CONTAINER, '/docker/' + CONTAINER, path + '/child', path + '.scope', '/other' + path,
                   path.replace('/burstable/', '/custom/'), path.replace(CONTAINER, CONTAINER[:12]),
                   path.replace(UID, '22345678-1234-5678-9abc-123456789abc'),
                   path.replace(CONTAINER, 'b2' * 32), path.replace(CONTAINER, CONTAINER.upper()),
                   path.replace('pod' + UID, UID), path.replace('/kubepods/', '/kubepods//'),
                   path.replace(UID, UID.replace('-', '_')),
                   '/kubepods.slice/kubepods-burstable.slice/kubepods-pod' + UID.replace('-', '_') + '.slice/cri-containerd-' + CONTAINER + '.scope']
        for bad in invalid:
            with self.subTest(path=bad):
                self.assertIsNone(self.metric(samples(path=bad))['mean'])
                self.assertIsNone(self.metric(samples(path=bad), name='memory_bytes')['mean'])

    def test_runtime_scheme_full_id_and_uuid_are_required(self):
        for runtime in ['docker://' + CONTAINER, 'crio://' + CONTAINER, 'containerd://' + CONTAINER[:12],
                        'containerd://' + CONTAINER.upper(), CONTAINER]:
            pod = copy.deepcopy(POD)
            pod['status']['containerStatuses'][0]['containerID'] = runtime
            self.assertIsNone(self.metric(samples(), pod=pod)['mean'])
        for uid in ['pod-1', UID.upper(), UID + 'x', '', None]:
            pod = copy.deepcopy(POD); pod['metadata']['uid'] = uid
            self.assertIsNone(self.metric(samples(), pod=pod)['mean'])

    def test_crio_and_containerd_paths_cannot_cross_runtime(self):
        pod = copy.deepcopy(POD)
        pod['status']['containerStatuses'][0]['containerID'] = 'cri-o://' + CONTAINER
        self.assertIsNone(self.metric(samples(), pod=pod)['mean'])
        self.assertIsNone(self.metric(samples(path='/kubepods/burstable/pod' + UID + '/crio-' + CONTAINER))['mean'])

    def test_exact_labels_and_trusted_node_are_required_not_instance(self):
        for label in ('namespace', 'pod', 'container', 'node', 'id'):
            for value in (None, '', 'other', [], {}):
                with self.subTest(label=label, value=value):
                    series = samples()
                    for item in series:
                        item['metric'][label] = value
                        item['metric']['instance'] = 'worker-1'
                    self.assertIsNone(self.metric(series)['mean'])
                    self.assertIsNone(self.metric(series, name='memory_bytes')['mean'])

    def test_conflicting_extra_identity_labels_are_visible(self):
        for label in ('pod_uid', 'container_id'):
            series = samples(); series[0]['metric'][label] = 'wrong'
            self.assertEqual(self.metric(series)['status'], 'conflict')
            self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')
        series = samples()
        series[0]['metric'].update(pod_uid=UID, container_id='containerd://' + CONTAINER)
        self.assertEqual(self.metric(series)['status'], 'observed')

    def test_duplicate_series_and_different_valid_paths_are_conflicts(self):
        for changed in (False, True):
            series = samples(); extra = copy.deepcopy(series[0])
            if changed: extra['metric']['id'] = '/kubepods/pod' + UID + '/' + CONTAINER
            series.append(extra)
            self.assertEqual(self.metric(series)['status'], 'conflict')
            self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')
        series = samples(); series.append(copy.deepcopy(series[1]))
        self.assertEqual(self.metric(series)['status'], 'observed')
        self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'conflict')

    def test_per_cpu_only_missing_or_mixed_series_cannot_be_aggregated(self):
        for value in (None, 'cpu00', '0', '', [], 0):
            series = samples(); series[0]['metric']['cpu'] = value
            self.assertIsNone(self.metric(series)['mean'])
            self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')
            series.append(samples()[0])
            self.assertEqual(self.metric(series)['status'], 'conflict')
        series = samples(); series[0]['metric']['cpu'] = 'cpu00'
        extra = copy.deepcopy(series[0]); extra['metric']['cpu'] = 'cpu01'; series.append(extra)
        self.assertEqual(self.metric(series)['status'], 'conflict')

    def test_cluster_semantics_and_multinode_namespace_collisions(self):
        series = samples()
        self.assertIsNone(self.metric(series, cluster='primary')['mean'])
        for item in series: item['metric']['cluster'] = 'primary'
        self.assertEqual(self.metric(series, cluster='primary')['status'], 'observed')
        self.assertIsNone(self.metric(series)['mean'])
        for cluster in ('other', '', [], {}):
            for item in series: item['metric']['cluster'] = cluster
            self.assertIsNone(self.metric(series, cluster='primary')['mean'])
        series = samples()
        for label, value in [('node', 'worker-2'), ('namespace', 'other'), ('cluster', 'other')]:
            duplicate = copy.deepcopy(samples())
            for item in duplicate: item['metric'][label] = value
            series.extend(duplicate)
        self.assertEqual(self.metric(series)['status'], 'observed')
        self.assertEqual(self.metric(series)['mean'], 1.5)

    def test_gpu_labels_never_repair_missing_cadvisor_identity(self):
        series = samples()
        for item in series:
            del item['metric']['id']
            item['metric'].update(pod_uid=UID, container_id='containerd://' + CONTAINER, attribution='container-runtime', scope='container')
        self.assertIsNone(self.metric(series)['mean'])

    def test_restarts_recreated_pods_and_moved_nodes_never_reuse_old_samples(self):
        for change in ('runtime', 'uid', 'node', 'started'):
            pod = copy.deepcopy(POD)
            if change == 'runtime': pod['status']['containerStatuses'][0]['containerID'] = 'containerd://' + 'b2' * 32
            if change == 'uid': pod['metadata']['uid'] = '22345678-1234-5678-9abc-123456789abc'
            if change == 'node': pod['spec']['nodeName'] = 'worker-2'
            if change == 'started': pod['status']['containerStatuses'][0]['state']['running']['startedAt'] = iso(NOW - 899)
            self.assertIsNone(self.metric(samples(), pod=pod)['mean'])
            self.assertIsNone(self.metric(samples(), pod=pod, name='memory_bytes')['mean'])

    def test_all_existing_runtime_gates_still_apply(self):
        for change in ('phase', 'init', 'ephemeral', 'pod-resources', 'duplicate-container', 'missing-start'):
            pod = copy.deepcopy(POD)
            if change == 'phase': pod['status']['phase'] = 'Pending'
            if change == 'init': pod['spec']['initContainers'] = [{'name': 'init'}]
            if change == 'ephemeral': pod['spec']['ephemeralContainers'] = [{'name': 'debug'}]
            if change == 'pod-resources': pod['spec']['resources'] = {'requests': {'cpu': '1'}}
            if change == 'duplicate-container': pod['spec']['containers'].append(copy.deepcopy(pod['spec']['containers'][0]))
            if change == 'missing-start': del pod['status']['containerStatuses'][0]['state']['running']['startedAt']
            self.assertIsNone(self.metric(samples(), pod=pod)['mean'])

    def test_every_container_is_required_separately_for_each_resource(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'].append({'name': 'sidecar', 'resources': {}})
        pod['status']['containerStatuses'].append({'name': 'sidecar', 'containerID': 'containerd://' + 'b2' * 32,
                                                 'state': {'running': {'startedAt': iso(NOW - 1800)}}})
        series = samples()
        second = copy.deepcopy(series)
        for item in second:
            item['metric']['container'] = 'sidecar'
            item['metric']['id'] = item['metric']['id'].replace(CONTAINER, 'b2' * 32)
        series.extend(second)
        self.assertEqual(self.metric(series, pod=pod)['mean'], 3)
        self.assertEqual(self.metric(series, pod=pod, name='memory_bytes')['mean'], 2 * 1024 ** 3)
        self.assertIsNone(self.metric(series[:3], pod=pod, name='memory_bytes')['mean'])
        self.assertEqual(self.metric(series[:3], pod=pod)['mean'], 3)
        self.assertIsNone(self.metric([series[0], series[1], series[3]], pod=pod)['mean'])
        self.assertEqual(self.metric([series[0], series[1], series[3]], pod=pod, name='memory_bytes')['mean'], 2 * 1024 ** 3)

    def test_duplicate_current_runtime_identity_is_not_counted_twice(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'].append({'name': 'sidecar', 'resources': {}})
        duplicate_status = copy.deepcopy(pod['status']['containerStatuses'][0])
        duplicate_status['name'] = 'sidecar'
        pod['status']['containerStatuses'].append(duplicate_status)
        series = samples()
        duplicate = copy.deepcopy(series)
        for item in duplicate: item['metric']['container'] = 'sidecar'
        series.extend(duplicate)
        for name in ('cpu_cores', 'memory_bytes'):
            metric = self.metric(series, name=name, pod=pod)
            self.assertEqual(metric['status'], 'conflict')
            self.assertIn('counted twice', metric['reason'])
            self.assertIsNone(metric['mean'])

    def test_duplicate_runtime_identity_across_current_pods_is_conflict(self):
        other = copy.deepcopy(POD)
        other['metadata']['name'] = 'other-pod'
        other['metadata']['uid'] = '22345678-1234-5678-9abc-123456789abc'
        with patch.dict(os.environ, ENABLED, clear=True):
            advisor = WorkloadAdvisor('kubernetes', Reader(samples() + samples(other)))
        result = advisor.snapshot(workload_inventory([POD, other], [NODE]), iso(NOW), NOW)
        for item in result['workloads']:
            for name in ('cpu_cores', 'memory_bytes'):
                self.assertEqual(item['metrics'][name]['status'], 'conflict')
                self.assertIsNone(item['metrics'][name]['mean'])

    def test_cpu_counter_bounds_differ_from_rate_and_reset_is_not_corrected(self):
        series = samples()
        for sample in series[0]['values']: sample[1] = str(float(sample[1]) + 10_000_000)
        self.assertEqual(self.metric(series)['mean'], 1.5)
        series[0]['values'][20][1] = '0'
        self.assertIn('reset', self.metric(series)['reason'])
        self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')

    def test_malformed_cpu_samples_do_not_erase_memory(self):
        for bad in (None, {}, [], [NOW], [NOW, True], [NOW, -1], [NOW, 'NaN'], [NOW, 'Inf'],
                    [NOW, '1e9999'], [NOW, 10 ** 500], [NOW, {}], [NOW, []], [NOW, str(2 ** 54)],
                    [NOW + 10, '1'], ['invalid', '1'], [True, '1'], [float('inf'), '1']):
            with self.subTest(bad=str(bad)[:80]):
                series = samples(); series[0]['values'][10] = bad
                self.assertIsNone(self.metric(series)['mean'])
                self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')
        for values in (None, {}, 'not-samples'):
            series = samples(); series[0]['values'] = values
            self.assertIsNone(self.metric(series)['mean'])
            self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')

    def test_malformed_memory_samples_do_not_erase_cpu(self):
        for bad in (None, [], [NOW], [NOW, True], [NOW, -1], [NOW, 'NaN'], [NOW, '1e9999'],
                    [NOW, 10 ** 500], [NOW, {}], [NOW, 2 ** 61], [NOW + 10, '1']):
            series = samples(); series[1]['values'][10] = bad
            self.assertIsNone(self.metric(series, name='memory_bytes')['mean'])
            self.assertEqual(self.metric(series)['status'], 'observed')
        for values in (None, {}, 'not-samples'):
            series = samples(); series[1]['values'] = values
            self.assertIsNone(self.metric(series, name='memory_bytes')['mean'])
            self.assertEqual(self.metric(series)['status'], 'observed')

    def test_derived_rate_out_of_range_is_unknown(self):
        series = samples()
        series[0]['values'] = [[t, str(10_000_000 * n * 15)] for n, (t, _) in enumerate(series[0]['values'])]
        self.assertIsNone(self.metric(series)['mean'])
        self.assertIn('out-of-range', self.metric(series)['reason'])
        self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'observed')

    def test_deduplicate_same_timestamp_without_inventing_time_or_coverage(self):
        series = samples()
        series[0]['values'] *= 5
        series[1]['values'] *= 5
        self.assertEqual(self.metric(series)['sample_count'], 60)
        self.assertEqual(self.metric(series, name='memory_bytes')['sample_count'], 61)
        for item in series: item['values'] = [item['values'][0]] * 61
        self.assertIsNone(self.metric(series)['mean'])
        self.assertIsNone(self.metric(series, name='memory_bytes')['mean'])

    def test_conflicting_timestamps_are_visible_for_each_resource(self):
        for index, name, other in ((0, 'cpu_cores', 'memory_bytes'), (1, 'memory_bytes', 'cpu_cores')):
            series = samples()
            series[index]['values'].append([series[index]['values'][0][0], '0'])
            self.assertEqual(self.metric(series, name=name)['status'], 'conflict')
            self.assertEqual(self.metric(series, name=other)['status'], 'observed')

    def test_fresh_http_cached_source_timestamps_become_stale(self):
        reader = Reader(samples())
        with patch.dict(os.environ, ENABLED, clear=True): advisor = WorkloadAdvisor('kubernetes', reader)
        workloads = workload_inventory([POD], [NODE])
        first = advisor.snapshot(workloads, iso(NOW), NOW)
        second = advisor.snapshot(workloads, iso(NOW + 121), NOW + 121)
        self.assertEqual(first['workloads'][0]['metrics']['cpu_cores']['status'], 'observed')
        for name in ('cpu_cores', 'memory_bytes'):
            self.assertEqual(second['workloads'][0]['metrics'][name]['status'], 'stale')
        self.assertEqual(reader.calls, 2)

    def test_last_seen_and_fresh_gpu_timestamps_cannot_rescue_cached_cpu_or_ram(self):
        series = samples()
        for item in series: item['values'] = [[t - 300, value] for t, value in item['values']]
        for metric in ('container_last_seen', 'DCGM_FI_DEV_GPU_UTIL', 'allotforge_container_gpu_busy_percent'):
            item = copy.deepcopy(samples()[0]); item['metric']['__name__'] = metric; series.append(item)
        self.assertEqual(self.metric(series)['status'], 'stale')
        self.assertEqual(self.metric(series, name='memory_bytes')['status'], 'stale')
        self.assert_no_verdict(observe(series))

    def test_minimum_42_counter_points_and_30_second_cadence(self):
        series = samples()
        for count in (41, 42):
            series[0]['values'] = [[NOW - (count - 1 - n) * 20, str(n * 30)] for n in range(count)]
            metric = self.metric(series)
            self.assertEqual(metric['status'], 'insufficient' if count == 41 else 'observed')
            if count == 42: self.assertEqual(metric['sample_count'], 41)
        for name in ('cpu_cores', 'memory_bytes'):
            self.assertEqual(self.metric(samples(step=30), name=name)['status'], 'insufficient')

    def test_span_coverage_and_gaps_are_not_weakened(self):
        series = samples(); series[0]['values'] = [[NOW - 40 + n, str(n)] for n in range(41)]
        self.assertEqual(self.metric(series)['status'], 'insufficient')
        series = samples(); series[0]['values'] = [v for v in series[0]['values'] if v[0] >= NOW - 630]
        self.assertEqual(self.metric(series)['status'], 'insufficient')
        for index, name in ((0, 'cpu_cores'), (1, 'memory_bytes')):
            series = samples(); series[index]['values'] = [v for v in series[index]['values'] if not NOW - 600 <= v[0] <= NOW - 495]
            self.assertEqual(self.metric(series, name=name)['status'], 'insufficient')

    def test_small_future_skew_does_not_supply_samples(self):
        series = samples()
        for item in series: item['values'].append([NOW + 1, item['values'][-1][1]])
        self.assertEqual(self.metric(series)['sample_end'], iso(NOW))
        self.assertEqual(self.metric(series)['sample_count'], 60)
        self.assertEqual(self.metric(series, name='memory_bytes')['sample_count'], 61)

    def test_irregular_counter_intervals_use_elapsed_source_seconds(self):
        raw = [[NOW - 50, '10'], [NOW - 43, '24'], [NOW - 20, '47']]
        rates, error = cadvisor._counter_rates(raw, NOW, NOW - 1800, 900, 90)
        self.assertIsNone(error)
        self.assertEqual(rates, [[NOW - 43, 2], [NOW - 20, 1]])
        self.assertEqual(cadvisor._counter_rates(list(reversed(raw)), NOW, NOW - 1800, 900, 90), (rates, None))

    def test_counter_interval_never_crosses_window_or_current_start(self):
        raw = [[NOW - 905, '0'], [NOW - 895, '20'], [NOW - 880, '35']]
        self.assertEqual(cadvisor._counter_rates(raw, NOW, NOW - 1800, 900, 90), ([[NOW - 880, 1]], None))
        rates, error = cadvisor._counter_rates(raw, NOW, NOW - 890, 900, 90)
        self.assertEqual(rates, [])
        self.assertIn('predate', error[0])

    def test_known_series_malformed_envelopes_and_limits_fail_closed(self):
        for malformed in (None, {}, [None], [{}], [{'metric': None, 'values': []}]):
            result = observe(malformed)
            # None in observe means default samples; exercise it via an explicit reader.
            if malformed is None: result = observe(reader=Reader(None))
            self.assertEqual(result['status'], 'unavailable')
            self.assertIsNone(result['workloads'][0]['metrics']['cpu_cores']['mean'])
        with patch('davano.cadvisor.MAX_SAMPLES', 100): self.assertEqual(observe()['status'], 'unavailable')
        with patch('davano.cadvisor.MAX_SERIES', 1): self.assertEqual(observe()['status'], 'unavailable')

    def test_untrusted_labels_and_errors_are_not_echoed(self):
        series = samples()
        for item in series: item['metric']['secret'] = '<script>secret</script>'
        series[0]['metric']['container_id'] = '<script>secret</script>'
        result = observe(series)
        self.assertNotIn('<script>', json.dumps(result))
        self.assertEqual(result['workloads'][0]['metrics']['cpu_cores']['status'], 'conflict')


class CadvisorConfigurationTests(unittest.TestCase):
    def test_default_mode_reader_and_query_contract_are_unchanged(self):
        with patch.dict(os.environ, {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true', 'DAVANO_PROMETHEUS_URL': 'https://metrics.test'}, clear=True):
            advisor = WorkloadAdvisor('kubernetes')
        self.assertEqual(advisor.metrics_source, 'custom-adapter')
        self.assertIsInstance(advisor.reader, WorkloadPrometheusReader)
        self.assertNotIsInstance(advisor.reader, cadvisor.CadvisorPrometheusReader)

    def test_mode_selection_ignored_unless_enabled(self):
        for enabled in ('false', ''):
            with patch.dict(os.environ, dict(ENABLED, DAVANO_WORKLOAD_ADVISOR_ENABLED=enabled, DAVANO_WORKLOAD_METRICS_SOURCE='invalid'), clear=True):
                advisor = WorkloadAdvisor('kubernetes')
                self.assertIsNone(advisor.reader)
                self.assertIsNone(advisor.error)
                self.assertEqual(advisor.snapshot([], iso(NOW), NOW)['status'], 'disabled')

    def test_disabled_cadvisor_selection_never_reads_or_becomes_custom_mode(self):
        for configured in ({}, {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'false'}):
            reader = Reader(samples())
            with patch.dict(os.environ, dict(configured, DAVANO_WORKLOAD_METRICS_SOURCE=cadvisor.SOURCE), clear=True):
                advisor = WorkloadAdvisor('kubernetes', reader)
            result = advisor.snapshot(workload_inventory([POD], [NODE]), iso(NOW), NOW)
            self.assertEqual(result['status'], 'disabled')
            self.assertEqual(reader.calls, 0)
            self.assertEqual(result['metrics_source'], cadvisor.SOURCE)
            self.assertIsNone(result['measurement_source'])
            self.assert_no_gpu(result)

    def test_typed_cadvisor_reader_is_fenced_even_with_no_source_selection(self):
        for configured in ({}, {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'false'},
                           {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true'},
                           {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true', 'DAVANO_WORKLOAD_METRICS_SOURCE': cadvisor.SOURCE}):
            reader = cadvisor.CadvisorPrometheusReader('https://metrics.test')
            with patch.dict(os.environ, configured, clear=True), patch.object(reader, 'samples') as read:
                advisor = WorkloadAdvisor('kubernetes', reader)
                result = advisor.snapshot(workload_inventory([POD], [NODE]), iso(NOW), NOW)
                read.assert_not_called()
            self.assertIn(result['status'], ('disabled', 'invalid-config'))
            self.assertEqual(result['metrics_source'], cadvisor.SOURCE)
            self.assert_no_gpu(result)

    def test_unknown_enabled_source_is_invalid_and_never_reads(self):
        reader = Reader(samples())
        result = observe(reader=reader, env={'DAVANO_WORKLOAD_METRICS_SOURCE': 'arbitrary-query'})
        self.assertEqual(result['status'], 'invalid-config')
        self.assertEqual(reader.calls, 0)

    def test_timestamp_assertion_required_before_constructing_or_reading(self):
        for asserted in ('false', '', '1', 'yes'):
            reader = Reader(samples())
            result = observe(reader=reader, env={'DAVANO_CADVISOR_TIMESTAMPS_PRESERVED': asserted})
            self.assertEqual(result['status'], 'invalid-config')
            self.assertEqual(result['input_requirements']['source_timestamps'], 'not-asserted')
            self.assertEqual(result['metrics_source'], cadvisor.SOURCE)
            self.assertIsNone(result['measurement_source'])
            self.assertEqual(reader.calls, 0)
            self.assertIsNone(result['workloads'][0]['metrics']['cpu_cores']['mean'])
            self.assert_no_gpu(result)
        with patch.dict(os.environ, {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true', 'DAVANO_WORKLOAD_METRICS_SOURCE': cadvisor.SOURCE}, clear=True), patch.object(cadvisor, 'CadvisorPrometheusReader') as constructor:
            self.assertEqual(WorkloadAdvisor('kubernetes').snapshot([], iso(NOW), NOW)['status'], 'invalid-config')
            constructor.assert_not_called()

    def assert_no_gpu(self, result):
        self.assertEqual(result['workloads'][0]['assessment']['verdict'], 'insufficient-evidence')
        self.assertIsNone(result['workloads'][0]['metrics']['gpu_busy_percent']['source_metric'])

    def test_wrong_concrete_prometheus_reader_never_queries_custom_or_gpu_metrics(self):
        from davano.observability import PrometheusReader
        for reader_type in (WorkloadPrometheusReader, PrometheusReader):
            captured = []
            def opener(request, **kwargs):
                captured.append(request.full_url)
                raise AssertionError('Wrong-source reader must not execute a request.')
            reader = reader_type('https://metrics.test', opener=opener)
            result = observe(reader=reader)
            self.assertEqual(result['status'], 'invalid-config')
            self.assertEqual(result['metrics_source'], cadvisor.SOURCE)
            self.assertIsNone(result['measurement_source'])
            self.assertEqual(captured, [])
            self.assert_no_gpu(result)

    def test_selected_cadvisor_concrete_reader_uses_only_allowed_metrics(self):
        captured = []
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, size):
                return json.dumps({'status': 'success', 'data': {'resultType': 'matrix', 'result': samples()}}).encode()
        def opener(request, **kwargs):
            captured.append(request.full_url)
            return Response()
        result = observe(reader=cadvisor.CadvisorPrometheusReader('https://metrics.test', opener=opener))
        self.assertEqual(result['status'], 'connected')
        self.assertEqual(result['workloads'][0]['metrics']['cpu_cores']['status'], 'observed')
        self.assertEqual(len(captured), 1)
        self.assertEqual(parse_qs(urlparse(captured[0]).query)['query'],
                         ['{__name__=~"container_cpu_usage_seconds_total|container_memory_working_set_bytes"}[15m]'])
        self.assert_no_gpu(result)

    def test_invalid_origin_or_cluster_fails_closed(self):
        for extra in ({'DAVANO_PROMETHEUS_URL': 'http://metrics.test'}, {'DAVANO_PROMETHEUS_URL': 'https://metrics.test/api'},
                      {'DAVANO_PROMETHEUS_URL': 'https://user:pass@metrics.test'},
                      {'DAVANO_PROMETHEUS_URL': 'https://metrics.test', 'DAVANO_PROMETHEUS_CLUSTER': 'x" or secret'}):
            with patch.dict(os.environ, dict(ENABLED, **extra), clear=True): advisor = WorkloadAdvisor('kubernetes')
            self.assertEqual(advisor.snapshot([], iso(NOW), NOW)['status'], 'invalid-config')
            self.assertIsNone(advisor.reader)

    def test_assertion_is_inapplicable_to_default_custom_adapter(self):
        with patch.dict(os.environ, dict(ENABLED, DAVANO_WORKLOAD_METRICS_SOURCE='custom-adapter', DAVANO_CADVISOR_TIMESTAMPS_PRESERVED='false', DAVANO_PROMETHEUS_URL='https://metrics.test'), clear=True):
            advisor = WorkloadAdvisor('kubernetes')
        self.assertIsNone(advisor.error)
        self.assertIsInstance(advisor.reader, WorkloadPrometheusReader)

    def test_demo_never_constructs_or_reads_any_optional_source(self):
        reader = Reader(samples())
        result = observe(reader=reader, source='demo')
        self.assertEqual(result['status'], 'disabled')
        self.assertEqual(reader.calls, 0)
        self.assertIsNone(result['measurement_source'])
        self.assertIsNone(result['workloads'][0]['metrics']['cpu_cores']['mean'])
        with patch.dict(os.environ, ENABLED, clear=True), patch.object(cadvisor, 'CadvisorPrometheusReader') as constructor:
            WorkloadAdvisor('demo')
            constructor.assert_not_called()

    def test_failed_read_clears_previous_observations(self):
        reader = Reader(samples())
        with patch.dict(os.environ, ENABLED, clear=True): advisor = WorkloadAdvisor('kubernetes', reader)
        workloads = workload_inventory([POD], [NODE])
        self.assertEqual(advisor.snapshot(workloads, iso(NOW), NOW)['workloads'][0]['metrics']['cpu_cores']['status'], 'observed')
        reader.samples = lambda: (_ for _ in ()).throw(ObservationUnavailable('Read unavailable.'))
        result = advisor.snapshot(workloads, iso(NOW), NOW)
        self.assertEqual(result['status'], 'unavailable')
        for name in ('cpu_cores', 'memory_bytes'): self.assertIsNone(result['workloads'][0]['metrics'][name]['mean'])

    def test_fixed_raw_https_query_has_only_cpu_and_memory_no_rate_or_last_seen(self):
        captured = []
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, size):
                self.size = size
                return json.dumps({'status': 'success', 'data': {'resultType': 'matrix', 'result': samples()}}).encode()
        response = Response()
        def opener(request, **kwargs): captured.append((request, kwargs)); return response
        reader = cadvisor.CadvisorPrometheusReader('https://metrics.test', opener=opener, cluster='primary')
        self.assertEqual(reader.samples(), samples())
        request, args = captured[0]
        query = parse_qs(urlparse(request.full_url).query)
        self.assertEqual(query['query'], ['{__name__=~"container_cpu_usage_seconds_total|container_memory_working_set_bytes",cluster="primary"}[15m]'])
        self.assertEqual(query['timeout'], ['5s'])
        self.assertEqual(request.get_method(), 'GET')
        self.assertEqual(args['timeout'], 8)
        self.assertTrue(args['context'].check_hostname)
        self.assertEqual(response.size, MAX_RESPONSE_BYTES + 1)
        with self.assertRaises(ValueError): cadvisor.CadvisorPrometheusReader('http://metrics.test')

    def test_reader_response_errors_warnings_and_bounds_are_shared(self):
        class Response:
            def __init__(self, body): self.body = body
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, size): return self.body
        bodies = [b'[' * 12000 + b'0' + b']' * 12000, b'x' * (MAX_RESPONSE_BYTES + 1),
                  json.dumps({'status': 'success', 'warnings': ['secret'], 'data': {'resultType': 'matrix', 'result': []}}).encode(),
                  json.dumps({'status': 'success', 'data': {'resultType': 'vector', 'result': []}}).encode()]
        for body in bodies:
            reader = cadvisor.CadvisorPrometheusReader('https://metrics.test', opener=lambda *_, **__: Response(body))
            with self.assertRaises(ObservationUnavailable): reader.samples()


if __name__ == '__main__':
    unittest.main()
