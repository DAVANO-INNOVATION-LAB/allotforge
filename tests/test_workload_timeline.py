"""Bounded synthetic window views, not stored history or real hardware evidence."""
import copy
import json
import math
import os
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from davano import cadvisor
from davano.app import create_app
from davano.observability import ObservationUnavailable, iso
from davano.workload_advisor import METRICS, WINDOW, WorkloadAdvisor, workload_inventory
from test_cadvisor import (NODE as CADVISOR_NODE, POD as CADVISOR_POD,
                           samples as cadvisor_samples)
from test_workload_advisor import (NODE, NOW, POD, data as custom_samples)


class Reader:
    cluster = None

    def __init__(self, series):
        self.series, self.calls = series, 0

    def samples(self):
        self.calls += 1
        if isinstance(self.series, Exception):
            raise self.series
        return self.series


def observe(series, pod=POD, *, mode='custom-adapter', now=NOW, reader=None,
            source='kubernetes', env=None):
    reader = Reader(series) if reader is None else reader
    config = {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true',
              'DAVANO_WORKLOAD_METRICS_SOURCE': mode,
              'DAVANO_CADVISOR_TIMESTAMPS_PRESERVED': 'true'}
    config.update(env or {})
    with patch.dict(os.environ, config, clear=True):
        advisor = WorkloadAdvisor(source, reader)
    node = CADVISOR_NODE if mode == cadvisor.SOURCE else NODE
    return advisor.snapshot(workload_inventory([pod], [node]), iso(now - 2), now)


def add_container(pod, name='second', gpu=1):
    pod = copy.deepcopy(pod)
    requests = {'cpu': '2'}
    if gpu:
        requests['nvidia.com/gpu'] = str(gpu)
    pod['spec']['containers'].append({'name': name, 'resources': {'requests': requests}})
    pod['status']['containerStatuses'].append({
        'name': name, 'containerID': 'containerd://' + 'b2' * 32,
        'state': {'running': {'startedAt': iso(NOW - 1800)}}})
    return pod


def remove_bucket(item, bucket, now=NOW):
    start = now - WINDOW + bucket * 60
    item['values'] = [sample for sample in item['values'] if not start <= sample[0] < start + 60]


class WorkloadTimelineTests(unittest.TestCase):
    def metric(self, result, key='cpu_cores'):
        return result['workloads'][0]['metrics'][key]

    def values(self, metric):
        return [point['value'] for point in metric['timeline']['points']]

    def assert_no_timeline(self, metric, status=None):
        self.assertNotEqual(metric['status'], 'observed')
        if status is not None:
            self.assertEqual(metric['status'], status)
        for field in ('timeline', 'mean', 'p95', 'peak'):
            self.assertIn(field, metric)
            self.assertIsNone(metric[field])

    def assert_schema(self, result, key, now=NOW):
        metric = self.metric(result, key)
        self.assertEqual(metric['status'], 'observed')
        timeline = metric['timeline']
        self.assertEqual(set(timeline), {'bucket_seconds', 'aggregation', 'points'})
        self.assertEqual(timeline['bucket_seconds'], 60)
        self.assertEqual(timeline['aggregation'], 'aligned-minute-means')
        self.assertEqual(len(timeline['points']), 15)
        for index, point in enumerate(timeline['points']):
            self.assertEqual(set(point), {'window_start', 'window_end', 'value'})
            self.assertEqual(point['window_start'], iso(now - WINDOW + index * 60))
            self.assertEqual(point['window_end'], iso(now - WINDOW + (index + 1) * 60))
            if point['value'] is not None:
                self.assertIsInstance(point['value'], (int, float))
                self.assertNotIsInstance(point['value'], bool)
                self.assertTrue(math.isfinite(point['value']))
        self.assertEqual(timeline['points'][0]['window_start'], result['window_start'])
        self.assertEqual(timeline['points'][-1]['window_end'], result['window_end'])
        values = [value for value in self.values(metric) if value is not None]
        self.assertEqual(metric['mean'], round(sum(values) / len(values), 4))
        self.assertEqual(metric['p95'], sorted(values)[math.ceil(len(values) * .95) - 1])
        self.assertEqual(metric['coverage_percent'], round(len(values) / 15 * 100, 1))
        json.dumps(result, allow_nan=False)

    def test_custom_metrics_are_additive_bounded_views_of_the_same_summary(self):
        reader = Reader(custom_samples())
        result = observe(None, reader=reader)
        self.assertEqual(reader.calls, 1)
        self.assertEqual(set(result), {
            'status', 'detail', 'source', 'observed_at', 'metrics_source', 'measurement_source',
            'inventory_observed_at', 'window_start', 'window_end', 'read_only', 'window_seconds',
            'policy', 'workloads', 'limitations'})
        previous_fields = {'status', 'mean', 'p95', 'peak', 'unit', 'source_metric', 'scope',
                           'sample_start', 'sample_end', 'sample_count', 'coverage_percent', 'reason'}
        for key, value in [('cpu_cores', 1), ('memory_bytes', 1048576), ('gpu_busy_percent', 5)]:
            with self.subTest(metric=key):
                self.assert_schema(result, key)
                metric = self.metric(result, key)
                self.assertEqual(set(metric), previous_fields | {'timeline'})
                self.assertEqual(self.values(metric), [value] * 15)
                self.assertEqual(metric['sample_count'], 61)
                self.assertEqual(metric['sample_start'], iso(NOW - 900))
                self.assertEqual(metric['sample_end'], iso(NOW))
        item = result['workloads'][0]
        self.assertEqual(item['assessment']['verdict'], 'cpu-comparison-candidate')
        self.assertEqual(item['assessment']['basis']['aligned_minute_buckets'], 15)
        self.assertFalse(item['assessment']['automatic_action'])
        self.assertTrue(result['read_only'])

    def test_bounds_roll_with_fractional_snapshot_time_instead_of_utc_minutes(self):
        now = NOW + 17.25
        series = custom_samples()
        for item in series:
            for sample in item['values']:
                sample[0] += 17.25
            # Actual observations end before the snapshot window; bounds do not.
            item['values'] = item['values'][1:-1]
        result = observe(series, now=now)
        for key in METRICS:
            self.assert_schema(result, key, now)
            metric = self.metric(result, key)
            self.assertEqual(metric['sample_start'], iso(now - 885))
            self.assertEqual(metric['sample_end'], iso(now - 15))
            first = metric['timeline']['points'][0]
            self.assertNotEqual(first['window_start'], metric['sample_start'])
            stamp = datetime.fromisoformat(first['window_start'].replace('Z', '+00:00'))
            self.assertEqual((stamp.second, stamp.microsecond), (17, 250000))

    def test_interior_boundaries_and_final_inclusive_sample_keep_existing_mapping(self):
        series = custom_samples(cpu=0)
        series[0]['values'][0][1] = '4'     # Snapshot window start belongs to bucket zero.
        series[0]['values'][4][1] = '8'     # Exactly start + 60 belongs to bucket one.
        series[0]['values'][-1][1] = '20'   # Exactly now belongs to final bucket.
        series[0]['values'].extend([[NOW - WINDOW - 1, '100'], [NOW + 1, '100']])
        result = observe(series)
        self.assert_schema(result, 'cpu_cores')
        metric = self.metric(result)
        self.assertEqual(self.values(metric), [1, 2] + [0] * 12 + [4])
        self.assertEqual(metric['sample_count'], 61)
        self.assertEqual(metric['peak'], 20)
        self.assertEqual(metric['sample_end'], iso(NOW))

    def test_zero_and_missing_are_distinct_without_interpolation(self):
        series = custom_samples(cpu=0, gpu=0)
        for item in series:
            remove_bucket(item, 6)
        result = observe(series)
        for key in METRICS:
            self.assert_schema(result, key)
            self.assertIsNone(self.values(self.metric(result, key))[6])
        self.assertEqual(self.values(self.metric(result)), [0] * 6 + [None] + [0] * 8)
        self.assertEqual(self.metric(result)['sample_count'], 57)
        self.assertEqual(self.metric(result)['coverage_percent'], 93.3)
        self.assert_no_timeline(self.metric(observe([])))

    def test_missing_initial_and_final_buckets_do_not_change_snapshot_bounds(self):
        series = custom_samples()
        for item in series:
            item['values'] = [sample for sample in item['values'] if NOW - 720 <= sample[0] < NOW - 60]
        result = observe(series)
        # Eleven occupied minutes fail the old coverage gate. No partial plot leaks out.
        self.assert_no_timeline(self.metric(result), 'insufficient')
        for item in series:
            item['values'].append([NOW - 60, item['values'][-1][1]])
        result = observe(series)
        self.assert_schema(result, 'cpu_cores')
        self.assertEqual(self.values(self.metric(result))[:3], [None] * 3)
        self.assertEqual(self.metric(result)['sample_end'], iso(NOW - 60))
        # A fresh enough final source time can still leave the final snapshot bucket empty.
        series = custom_samples()
        for item in series:
            item['values'] = item['values'][:-5]
        result = observe(series)
        self.assert_schema(result, 'cpu_cores')
        self.assertIsNone(self.values(self.metric(result))[-1])
        self.assertEqual(self.metric(result)['sample_end'], iso(NOW - 75))

    def test_multi_container_shared_gaps_never_show_partial_cpu_or_memory(self):
        pod = add_container(POD)
        series = custom_samples(pod)
        for index in (0, 1):
            remove_bucket(series[index], 6)
        for index in (3, 4):
            remove_bucket(series[index], 9)
            for sample in series[index]['values']:
                sample[1] = str(float(sample[1]) * 2)
        result = observe(series, pod)
        for key, value in [('cpu_cores', 3), ('memory_bytes', 3 * 1048576)]:
            self.assert_schema(result, key)
            values = self.values(self.metric(result, key))
            self.assertEqual(values, [None if index in (6, 9) else value for index in range(15)])
            self.assertEqual(self.metric(result, key)['coverage_percent'], 86.7)
        self.assertEqual(self.values(self.metric(result, 'gpu_busy_percent')), [5] * 15)

    def test_container_means_are_summed_without_weighting_by_sample_count(self):
        pod = add_container(POD)
        series = custom_samples(pod, cpu=1)
        series[3]['values'] = series[3]['values'][1:]
        series[3]['values'][0][1] = '1'
        series[3]['values'][1][1] = '2'
        series[3]['values'][2][1] = '6'
        result = observe(series, pod)
        self.assert_schema(result, 'cpu_cores')
        self.assertEqual(self.values(self.metric(result))[0], 4)  # 1 + (1 + 2 + 6) / 3
        self.assertEqual(self.metric(result)['peak'], 7)
        self.assertEqual(self.metric(result)['sample_start'], iso(NOW - 885))

    def test_gpu_weighting_uses_requested_gpus_and_excludes_cpu_only_sidecars(self):
        pod = add_container(POD, gpu=3)
        series = custom_samples(pod)
        series[5]['metric']['gpu_count'] = '3'
        for sample in series[5]['values']:
            sample[1] = '25'
        result = observe(series, pod)
        self.assert_schema(result, 'gpu_busy_percent')
        self.assertEqual(self.values(self.metric(result, 'gpu_busy_percent')), [20] * 15)
        self.assertEqual(self.metric(result, 'gpu_busy_percent')['peak'], 25)
        self.assertEqual(result['workloads'][0]['assessment']['verdict'], 'inconclusive')
        pod['spec']['containers'][1]['resources']['requests'].pop('nvidia.com/gpu')
        series = series[:5]  # No GPU series for the CPU-only sidecar.
        result = observe(series, pod)
        self.assertEqual(self.values(self.metric(result, 'gpu_busy_percent')), [5] * 15)
        self.assertEqual(self.values(self.metric(result)), [2] * 15)

    def test_metric_alignment_does_not_change_comparison_advice(self):
        series = custom_samples()
        for sample in series[0]['values']:
            sample[1] = '1' if sample[0] < NOW - 720 else '0'
        series[2]['values'] = series[2]['values'][12:]
        result = observe(series)
        cpu, gpu = (self.metric(result, key) for key in ('cpu_cores', 'gpu_busy_percent'))
        self.assertEqual(self.values(cpu)[:3], [1] * 3)
        self.assertEqual(self.values(gpu)[:3], [None] * 3)
        assessment = result['workloads'][0]['assessment']
        self.assertEqual(assessment['verdict'], 'inconclusive')
        self.assertEqual(assessment['basis']['gpu_container_cpu_mean_cores'], 0)
        self.assertEqual(assessment['basis']['aligned_minute_buckets'], 12)

    def test_invalid_nonfinite_out_of_range_and_future_samples_have_null_timeline(self):
        for value in ('NaN', 'Inf', '-Inf', '-1', '1000001', True, None, {}, 'bad'):
            with self.subTest(value=value):
                series = custom_samples()
                series[0]['values'][20][1] = value
                result = observe(series)
                self.assert_no_timeline(self.metric(result))
                self.assert_schema(result, 'memory_bytes')
        for timestamp in (NOW + 6, float('nan'), -1, True):
            series = custom_samples()
            series[0]['values'][20][0] = timestamp
            self.assert_no_timeline(self.metric(observe(series)))

    def test_stale_short_sparse_and_conflicting_series_never_leak_partial_timeline(self):
        cases = []
        values = custom_samples()[0]['values']
        cases.append(('stale', values[:-9]))
        cases.append(('insufficient', values[::2]))
        cases.append(('insufficient', values[-40:]))
        cases.append(('insufficient', values[-41:]))
        cases.append(('insufficient', values[:10] + values[18:]))
        cases.append(('insufficient', [values[-1]] * 61))
        cases.append(('conflict', values + [[NOW - 300, '75']]))
        for status, samples in cases:
            with self.subTest(status=status, samples=len(samples)):
                series = custom_samples()
                series[0]['values'] = samples
                result = observe(series)
                self.assert_no_timeline(self.metric(result), status)
                self.assert_schema(result, 'memory_bytes')

    def test_identity_attribution_duplicates_and_runtime_gates_are_preserved(self):
        for label, value in [('pod_uid', 'old'), ('container_id', 'containerd://old'),
                             ('node', 'other'), ('cluster', 'other'), ('attribution', None),
                             ('scope', 'device')]:
            with self.subTest(label=label):
                series = custom_samples()
                series[0]['metric'][label] = value
                self.assert_no_timeline(self.metric(observe(series)))
        series = custom_samples()
        series.append(copy.deepcopy(series[0]))
        self.assert_no_timeline(self.metric(observe(series)), 'conflict')
        for change in ('phase', 'new-runtime', 'init', 'ephemeral', 'pod-resources'):
            with self.subTest(change=change):
                pod = copy.deepcopy(POD)
                if change == 'phase': pod['status']['phase'] = 'Pending'
                if change == 'new-runtime':
                    pod['status']['containerStatuses'][0]['state']['running']['startedAt'] = iso(NOW - 899)
                if change == 'init': pod['spec']['initContainers'] = [{'name': 'init'}]
                if change == 'ephemeral': pod['spec']['ephemeralContainers'] = [{'name': 'debug'}]
                if change == 'pod-resources': pod['spec']['resources'] = {'requests': {'cpu': '4'}}
                for metric in observe(custom_samples(pod), pod)['workloads'][0]['metrics'].values():
                    self.assert_no_timeline(metric)

    def test_missing_container_and_insufficient_shared_coverage_fail_whole_metric(self):
        pod = add_container(POD)
        series = custom_samples(pod)
        result = observe(series[:3], pod)
        for metric in result['workloads'][0]['metrics'].values():
            self.assert_no_timeline(metric)
        series = custom_samples(pod)
        for bucket in (2, 5):
            remove_bucket(series[0], bucket)
        for bucket in (8, 11):
            remove_bucket(series[3], bucket)
        result = observe(series, pod)
        self.assert_no_timeline(self.metric(result), 'insufficient')
        self.assertEqual(self.metric(result)['coverage_percent'], 73.3)
        self.assert_schema(result, 'memory_bytes')

    def test_no_gpu_and_unverified_gpu_mode_never_gain_timeline(self):
        pod = copy.deepcopy(POD)
        pod['spec']['containers'][0]['resources']['requests'].pop('nvidia.com/gpu')
        result = observe(custom_samples(pod), pod)
        self.assert_no_timeline(self.metric(result, 'gpu_busy_percent'))
        self.assert_schema(result, 'cpu_cores')
        self.assertEqual(result['workloads'][0]['assessment']['verdict'], 'cpu-requested')
        for requests in ({'nvidia.com/gpu.shared': '1'}, {'nvidia.com/mig-1g.5gb': '1'},
                         {'nvidia.com/gpu': 'bad'}, {'vendor.example/accelerator': '1'}):
            pod['spec']['containers'][0]['resources']['requests'] = requests
            result = observe(custom_samples(pod), pod)
            self.assert_no_timeline(self.metric(result, 'gpu_busy_percent'))
            self.assert_schema(result, 'cpu_cores')

    def test_malformed_excessive_or_unavailable_response_clears_every_timeline(self):
        for series in (None, {}, [{'metric': {}, 'values': None}], [{}]):
            result = observe(series)
            self.assertEqual(result['status'], 'unavailable')
            for metric in result['workloads'][0]['metrics'].values():
                self.assert_no_timeline(metric)
        for limit in ('MAX_SAMPLES', 'MAX_SERIES'):
            with patch('davano.workload_advisor.' + limit, 2):
                result = observe(custom_samples())
            self.assertEqual(result['status'], 'unavailable')
            for metric in result['workloads'][0]['metrics'].values():
                self.assert_no_timeline(metric)
        reader = Reader(custom_samples())
        with patch.dict(os.environ, {'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'true'}, clear=True):
            advisor = WorkloadAdvisor('kubernetes', reader)
        inventory = workload_inventory([POD], [NODE])
        first = advisor.snapshot(inventory, iso(NOW), NOW)
        self.assert_schema(first, 'cpu_cores')
        reader.series = ObservationUnavailable('Read unavailable.')
        later = advisor.snapshot(inventory, iso(NOW + 60), NOW + 60)
        self.assertEqual(reader.calls, 2)
        self.assertEqual(later['status'], 'unavailable')
        for metric in later['workloads'][0]['metrics'].values():
            self.assert_no_timeline(metric)
        self.assertEqual(self.values(self.metric(first)), [1] * 15)

    def test_no_data_demo_and_invalid_mode_never_gain_timeline(self):
        for kwargs in ({}, {'source': 'demo'}, {'env': {'DAVANO_WORKLOAD_METRICS_SOURCE': 'other'}}):
            reader = Reader([] if not kwargs else custom_samples())
            result = observe(None, reader=reader, **kwargs)
            for metric in result['workloads'][0]['metrics'].values():
                self.assert_no_timeline(metric)
            self.assertEqual(reader.calls, 0 if kwargs else 1)
        with patch.dict(os.environ, {}, clear=True):
            advisor = WorkloadAdvisor('kubernetes')
        result = advisor.snapshot(workload_inventory([POD], [NODE]), iso(NOW), NOW)
        self.assertEqual(result['status'], 'disabled')
        self.assert_no_timeline(self.metric(result))

    def test_cadvisor_keeps_interval_rate_semantics_and_unsupported_gpu_null(self):
        reader = Reader(cadvisor_samples())
        # Custom GPU evidence must not sneak through the source switch.
        reader.series.extend(custom_samples())
        result = observe(None, CADVISOR_POD, mode=cadvisor.SOURCE, reader=reader)
        self.assertEqual(reader.calls, 1)
        for key, value in [('cpu_cores', 1.5), ('memory_bytes', 1024 ** 3)]:
            self.assert_schema(result, key)
            self.assertEqual(self.values(self.metric(result, key)), [value] * 15)
            self.assertIn('provenance', self.metric(result, key))
        cpu = self.metric(result)
        self.assertEqual(cpu['sample_start'], iso(NOW - 885))
        self.assertEqual(cpu['timeline']['points'][0]['window_start'], iso(NOW - 900))
        self.assertEqual(cpu['sample_count'], 60)
        self.assertIn('interval-average', cpu['provenance']['temporal_semantics'])
        self.assert_no_timeline(self.metric(result, 'gpu_busy_percent'))
        self.assertIsNone(self.metric(result, 'gpu_busy_percent')['source_metric'])
        self.assertEqual(result['workloads'][0]['assessment']['verdict'], 'insufficient-evidence')

    def test_cadvisor_multi_container_shared_gaps_use_rates_not_counters(self):
        pod = add_container(CADVISOR_POD, gpu=0)
        series = cadvisor_samples()
        second = copy.deepcopy(series)
        old_id = CADVISOR_POD['status']['containerStatuses'][0]['containerID'].split('://')[1]
        for item in second:
            item['metric']['container'] = 'second'
            item['metric']['id'] = item['metric']['id'].replace(old_id, 'b2' * 32)
        series += second
        for index in (0, 1): remove_bucket(series[index], 6)
        for index in (2, 3): remove_bucket(series[index], 9)
        result = observe(series, pod, mode=cadvisor.SOURCE)
        for key, value in [('cpu_cores', 3), ('memory_bytes', 2 * 1024 ** 3)]:
            self.assert_schema(result, key)
            self.assertEqual(self.values(self.metric(result, key)),
                             [None if index in (6, 9) else value for index in range(15)])
        self.assertEqual(self.metric(result)['sample_start'], iso(NOW - 885))
        self.assertEqual(self.metric(result)['peak'], 3)
        self.assert_no_timeline(self.metric(result, 'gpu_busy_percent'))
        result = observe(series[:3], pod, mode=cadvisor.SOURCE)
        self.assert_schema(result, 'cpu_cores')
        self.assert_no_timeline(self.metric(result, 'memory_bytes'))

    def test_cadvisor_rate_changes_and_zero_do_not_become_counter_values(self):
        series = cadvisor_samples()
        for sample in series[0]['values']:
            sample[1] = '100000'
        series[0]['values'][-1][1] = '100150'
        for sample in series[1]['values']: sample[1] = '0'
        result = observe(series, CADVISOR_POD, mode=cadvisor.SOURCE)
        self.assert_schema(result, 'cpu_cores')
        self.assertEqual(self.values(self.metric(result)), [0] * 14 + [2])
        self.assertEqual(self.metric(result)['peak'], 10)
        self.assertEqual(self.values(self.metric(result, 'memory_bytes')), [0] * 15)

    def test_cadvisor_invalid_stale_counter_reset_and_identity_never_leak_timeline(self):
        for change in ('reset', 'nonfinite', 'stale', 'per-cpu', 'wrong-cgroup', 'duplicate', 'gap'):
            with self.subTest(change=change):
                series = cadvisor_samples()
                if change == 'reset': series[0]['values'][30][1] = '0'
                if change == 'nonfinite': series[0]['values'][30][1] = 'NaN'
                if change == 'stale': series[0]['values'] = series[0]['values'][:-9]
                if change == 'per-cpu': series[0]['metric']['cpu'] = 'cpu00'
                if change == 'wrong-cgroup': series[0]['metric']['id'] += '-other'
                if change == 'duplicate': series.append(copy.deepcopy(series[0]))
                if change == 'gap': del series[0]['values'][10:18]
                result = observe(series, CADVISOR_POD, mode=cadvisor.SOURCE)
                self.assert_no_timeline(self.metric(result))
                self.assert_schema(result, 'memory_bytes')
                self.assert_no_timeline(self.metric(result, 'gpu_busy_percent'))

    def test_cadvisor_configuration_gates_prevent_reads_and_all_numeric_timelines(self):
        for env in ({'DAVANO_WORKLOAD_ADVISOR_ENABLED': 'false'},
                    {'DAVANO_CADVISOR_TIMESTAMPS_PRESERVED': 'false'}):
            reader = Reader(cadvisor_samples())
            result = observe(None, CADVISOR_POD, mode=cadvisor.SOURCE, reader=reader, env=env)
            self.assertEqual(reader.calls, 0)
            for metric in result['workloads'][0]['metrics'].values():
                self.assert_no_timeline(metric)

    def test_accepted_upper_values_and_large_gpu_weights_are_finite(self):
        pod = add_container(POD, gpu=1_000_000)
        pod['spec']['containers'][0]['resources']['requests']['nvidia.com/gpu'] = '1000000'
        series = custom_samples(pod, cpu=1_000_000, gpu=100)
        for index in (1, 4):
            for sample in series[index]['values']: sample[1] = str(2 ** 60)
        for index in (2, 5): series[index]['metric']['gpu_count'] = '1000000'
        result = observe(series, pod)
        for key in METRICS: self.assert_schema(result, key)
        self.assertEqual(self.values(self.metric(result)), [2_000_000] * 15)
        self.assertEqual(self.values(self.metric(result, 'memory_bytes')), [2 ** 61] * 15)
        self.assertEqual(self.values(self.metric(result, 'gpu_busy_percent')), [100] * 15)
        json.dumps(result, allow_nan=False)

    def test_nonfinite_aggregates_fail_closed_even_if_future_bounds_are_widened(self):
        widened = dict(METRICS, cpu_cores=(METRICS['cpu_cores'][0], 'cores', 1e308))
        # Existing bounds prevent these cases. Guard widened future contracts too.
        cases = [custom_samples(cpu=1e308), custom_samples(cpu=1.3e307)]
        pod = add_container(POD)
        peak_overflow = custom_samples(pod, cpu=0)
        peak_overflow[0]['values'][8][1] = '1e308'
        peak_overflow[3]['values'][24][1] = '1e308'
        for series, current_pod in [(value, POD) for value in cases] + [(peak_overflow, pod)]:
            with self.subTest(containers=len(current_pod['spec']['containers']), value=series[0]['values'][0][1]):
                with patch.dict('davano.workload_advisor.METRICS', widened, clear=True):
                    result = observe(series, current_pod)
                self.assert_no_timeline(self.metric(result))
                self.assert_schema(result, 'memory_bytes')
                json.dumps(result, allow_nan=False)

    def test_api_demo_response_adds_null_timeline_without_new_routes_or_measurements(self):
        with tempfile.TemporaryDirectory() as directory:
            with TestClient(create_app(db_path=str(Path(directory) / 'test.db'))) as client:
                response = client.get('/api/v1/state')
        self.assertEqual(response.status_code, 200)
        result = response.json()['workload_advisor']
        self.assertTrue(result['read_only'])
        self.assertTrue(result['workloads'])
        for item in result['workloads']:
            for metric in item['metrics'].values():
                self.assert_no_timeline(metric)


if __name__ == '__main__':
    unittest.main()
