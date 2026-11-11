"""Read-only, runtime-bound workload observations and conservative experiment advice.

The optional metric names below are an explicit administrator-supplied adapter
contract, NOT a claim that stock DCGM or cAdvisor exports per-workload GPU use.
No request count, device-only signal, or name-only match can select CPU or GPU.
"""
import math
import os
import time
from collections import defaultdict
from datetime import datetime

from . import cadvisor
from .compute import pod_compute_request
from .inventory import allocation_mode, gpu_vendor, pod_phase, pod_requests, quantity, resource_requests
from .observability import (MAX_SAMPLES, MAX_SERIES, ObservationUnavailable,
                            PrometheusReader, iso, token)

WINDOW = 900
BUCKET_SECONDS = 60
BUCKET_COUNT = WINDOW // BUCKET_SECONDS
POLICY = {'min_samples': 41, 'min_span_seconds': 600, 'min_coverage_percent': 80,
          'max_gap_seconds': 90, 'freshness_seconds': 120}
METRICS = {
    'cpu_cores': ('allotforge_container_cpu_usage_cores', 'cores', 1_000_000),
    'memory_bytes': ('allotforge_container_memory_working_set_bytes', 'bytes', 2 ** 60),
    'gpu_busy_percent': ('allotforge_container_gpu_busy_percent', '%', 100),
}
COMPARISON_PLAN = {
    'steps': [
        'Confirm a CPU implementation exists and meets the same output-quality target.',
        'Use the same image/code version, input dataset, batch size, concurrency, seed and output checks on CPU and GPU.',
        'Record hardware, CPU/RAM limits, accelerator model/count, software versions and any competing work. Include GPU host CPU cost.',
        'Separate warm-up from at least five completed measured trials on each resource. Include transfers and end-to-end completion time.',
        'Compare correctness, median and tail latency or jobs per second, peak memory and total cost per successful job against your deadline.',
        'Use actual prices and elapsed billed time for both runs. Unknown prices stay unknown. Review results before changing placement.',
    ],
    'metrics': ['correctness / quality', 'end-to-end seconds', 'throughput', 'p95 latency',
                'peak CPU and accelerator memory', 'total cost per successful job'],
}


class WorkloadPrometheusReader(PrometheusReader):
    metric_names = [m[0] for m in METRICS.values()]


def _epoch(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.timestamp() if parsed.tzinfo is not None else None
    except (ValueError, AttributeError, TypeError, OverflowError):
        return None


def workload_inventory(pods, nodes):
    """Minimal per-Pod identity; never expose env, command, or full Pod objects."""
    node_labels = {n.get('metadata', {}).get('name'): n.get('metadata', {}).get('labels', {}) for n in nodes}
    out = []
    for pod in pods:
        meta, spec, status = pod.get('metadata', {}), pod.get('spec', {}), pod.get('status', {})
        req = pod_requests(pod)
        gpu_count = None if any(v is None for v in req.values()) else sum(req.values())
        statuses = status.get('containerStatuses', [])
        statuses = statuses if isinstance(statuses, list) else []
        containers = []
        for c in spec.get('containers', []):
            matches = [s for s in statuses if isinstance(s, dict) and s.get('name') == c.get('name')]
            s = matches[0] if len(matches) == 1 else {}
            state = s.get('state') if isinstance(s.get('state'), dict) else {}
            running = state.get('running') if isinstance(state.get('running'), dict) else {}
            cr = resource_requests(c)
            cg = None if any(v is None for v in cr.values()) else sum(cr.values())
            resources = c.get('resources', {})
            unclassified = any('/' in key and gpu_vendor(key) is None and quantity(value) != 0
                               for section in ('requests', 'limits')
                               for key, value in resources.get(section, {}).items())
            containers.append({'name': token(c.get('name')), 'runtime_id': token(s.get('containerID'), 256),
                               'started_at': running.get('startedAt'), 'gpu_count': cg,
                               'unclassified_extended_resources': unclassified,
                               'whole_gpu': all(allocation_mode(k, node_labels.get(spec.get('nodeName'), {})) == 'whole-gpu'
                                                for k, v in cr.items() if v != 0)})
        name, namespace = token(meta.get('name')), token(meta.get('namespace', 'default'))
        out.append({'id': f'{namespace}/{name}', 'uid': token(meta.get('uid')), 'name': name,
                    'namespace': namespace, 'node': token(spec.get('nodeName')), 'phase': pod_phase(pod),
                    'requests': {'cpu_millicores': pod_compute_request(pod, 'cpu_millicores'),
                                 'memory_mib': pod_compute_request(pod, 'memory_mib'), 'gpu_count': gpu_count},
                    'containers': containers,
                    # Init phases, sidecars and Pod-level budgets need a separate attribution contract.
                    'supported_runtime': not bool(spec.get('initContainers') or spec.get('ephemeralContainers') or spec.get('resources'))})
    return out


def unknown_metric(key, reason, status='unknown', metric_specs=None):
    metric_specs = METRICS if metric_specs is None else metric_specs
    return {'status': status, 'mean': None, 'p95': None, 'peak': None, 'unit': metric_specs[key][1],
            'source_metric': metric_specs[key][0], 'scope': 'pod-containers', 'sample_start': None,
            'sample_end': None, 'sample_count': 0, 'coverage_percent': 0, 'reason': reason,
            'timeline': None}


def _timeline(buckets, now):
    """Expose only accepted shared means, with explicit gaps and rolling bounds.

    Bounds describe the advisor snapshot window, not source sample timestamps
    or the exact Prometheus evaluation time.
    Interior buckets are left-inclusive/right-exclusive; the existing source
    sample exactly at ``now`` belongs to the final bucket ending at ``now``.
    """
    start = now - WINDOW
    return {'bucket_seconds': BUCKET_SECONDS, 'aggregation': 'aligned-minute-means',
            'points': [{'window_start': iso(start + b * BUCKET_SECONDS),
                        'window_end': iso(start + (b + 1) * BUCKET_SECONDS),
                        'value': buckets.get(b)} for b in range(BUCKET_COUNT)]}


def _series_window(samples, upper, now, started):
    if not isinstance(samples, list):
        raise ObservationUnavailable('Malformed workload sample list was rejected.')
    values = {}
    for sample in samples:
        if not isinstance(sample, list) or len(sample) != 2 or any(isinstance(v, bool) for v in sample):
            return None, 'Invalid sample in the observation window.', 'unknown'
        try:
            timestamp, value = float(sample[0]), float(sample[1])
            if not math.isfinite(timestamp) or not math.isfinite(value) or not 0 <= value <= upper or timestamp <= 0:
                raise ValueError()
        except (TypeError, ValueError, OverflowError):
            return None, 'Non-finite, malformed or out-of-range sample was rejected.', 'unknown'
        if timestamp > now + 5:
            return None, 'Future source sample; clock alignment is unknown.', 'unknown'
        if timestamp > now:
            # Do not use even small future clock skew to satisfy a window.
            continue
        if timestamp < now - WINDOW:
            continue
        if timestamp < started:
            return None, 'Samples predate the current container incarnation.', 'unknown'
        if timestamp in values and values[timestamp] != value:
            return None, 'Conflicting values at the same source timestamp.', 'conflict'
        values[timestamp] = value
    ordered = sorted(values.items())
    if not ordered:
        return None, 'No samples in the current observation window.', 'unknown'
    if now - ordered[-1][0] > POLICY['freshness_seconds']:
        return None, 'Last sample is older than the freshness limit.', 'stale'
    if len(ordered) < POLICY['min_samples'] or ordered[-1][0] - ordered[0][0] < POLICY['min_span_seconds']:
        return None, 'Too few distinct samples or too short an observation span.', 'insufficient'
    if max(b[0] - a[0] for a, b in zip(ordered, ordered[1:])) > POLICY['max_gap_seconds']:
        return None, 'A sample gap exceeds the allowed 90 seconds.', 'insufficient'
    buckets = defaultdict(list)
    for timestamp, value in ordered:
        bucket = min(BUCKET_COUNT - 1, int((timestamp - (now - WINDOW)) / BUCKET_SECONDS))
        buckets[bucket].append(value)
    means = {b: sum(v) / len(v) for b, v in buckets.items()}
    if not all(math.isfinite(value) for value in means.values()):
        return None, 'Non-finite aligned minute mean was rejected.', 'unknown'
    return {'values': ordered, 'buckets': means}, '', 'observed'


def _normalize(series, workloads, now, cluster, cadvisor_mode=False):
    metric_specs = cadvisor.METRICS if cadvisor_mode else METRICS
    def unknown(key, reason, status='unknown'):
        return unknown_metric(key, reason, status, metric_specs)
    if not isinstance(series, list) or len(series) > MAX_SERIES:
        raise ObservationUnavailable('Invalid or excessive workload metric series.')
    indexed, count = defaultdict(list), 0
    names = {v[0]: k for k, v in metric_specs.items() if v[0] is not None}
    for item in series:
        if not isinstance(item, dict) or not isinstance(item.get('metric'), dict) or not isinstance(item.get('values'), list):
            raise ObservationUnavailable('Malformed workload metric series.')
        count += len(item['values'])
        if count > MAX_SAMPLES:
            raise ObservationUnavailable('Workload sample limit exceeded; partial data was rejected.')
        labels = item['metric']
        if labels.get('__name__') not in names:
            continue
        # With no cluster configured, labelled/federated samples do not join.
        if labels.get('cluster') != cluster and (cluster is not None or labels.get('cluster') is not None):
            continue
        key = (labels.get('namespace'), labels.get('pod'), labels.get('pod_uid'),
               labels.get('container'), labels.get('container_id'), labels.get('node'), names[labels['__name__']])
        if not all(isinstance(v, str) for v in key):
            continue
        indexed[key].append(item)
    result = []
    for workload in workloads:
        out = {k: v for k, v in workload.items() if k not in ('containers', 'supported_runtime')}
        metrics, windows, gpu_container_cpu = {}, {}, {}
        containers = workload['containers']
        runtime_ok = (workload['phase'] == 'Running' and workload['uid'] and workload['node']
                      and workload['supported_runtime'] and containers
                      and len({c['name'] for c in containers}) == len(containers)
                      and all(c['name'] and c['runtime_id'] and _epoch(c['started_at']) is not None
                              and _epoch(c['started_at']) <= now - WINDOW for c in containers))
        for metric, (source_metric, unit, upper) in metric_specs.items():
            reason = 'A running Pod UID and every current container runtime ID with 15 minutes of runtime are required; init/ephemeral containers are unsupported.'
            m = unknown(metric, reason)
            relevant = containers if metric != 'gpu_busy_percent' else [c for c in containers if c['gpu_count'] != 0]
            if cadvisor_mode and metric == 'gpu_busy_percent':
                metrics[metric] = unknown(metric, cadvisor.GPU_REASON)
                continue
            if runtime_ok and relevant:
                readings = []
                for c in relevant:
                    if metric == 'gpu_busy_percent' and (c['gpu_count'] is None or not c['whole_gpu']
                            or workload['requests']['gpu_count'] is None
                            or any(x['gpu_count'] is None for x in containers)
                            or workload['requests']['gpu_count'] != sum(x['gpu_count'] for x in containers)):
                        m = unknown(metric, 'GPU request count or whole-GPU mode is unverified. Shared/MIG attribution is unsupported.')
                        break
                    key = (workload['namespace'], workload['name'], workload['uid'], c['name'], c['runtime_id'], workload['node'], metric)
                    matches = indexed.get(key, [])
                    if len(matches) != 1:
                        m = unknown(metric, ('Exactly one current-runtime cAdvisor series per container is required. Missing, duplicate or mixed CPU series cannot be merged.' if cadvisor_mode else 'Exactly one runtime-bound adapter series per container is required. Missing or duplicate series cannot be merged.'), 'conflict' if matches else 'unknown')
                        break
                    labels = matches[0]['metric']
                    if cadvisor_mode and matches[0].get('_cadvisor_error'):
                        reason, status = matches[0]['_cadvisor_error']
                        m = unknown(metric, reason, status)
                        break
                    if not cadvisor_mode and (labels.get('attribution') != 'container-runtime' or labels.get('scope') != 'container'):
                        m = unknown(metric, 'Adapter did not assert container-runtime attribution and container scope.')
                        break
                    if metric == 'gpu_busy_percent' and labels.get('gpu_count') != str(c['gpu_count']):
                        m = unknown(metric, 'GPU adapter coverage count does not match the container request.')
                        break
                    reading, reason, status = _series_window(matches[0]['values'], upper, now, _epoch(c['started_at']))
                    if reading is None:
                        m = unknown(metric, reason, status)
                        break
                    readings.append((c, reading))
                else:
                    shared = set.intersection(*(set(r['buckets']) for _, r in readings))
                    coverage = len(shared) / BUCKET_COUNT * 100
                    if coverage < POLICY['min_coverage_percent']:
                        m = unknown(metric, 'Aligned one-minute buckets cover less than 80% of the 15-minute window.', 'insufficient')
                        m['coverage_percent'] = round(coverage, 1)
                    else:
                        weights = [c['gpu_count'] for c, _ in readings] if metric == 'gpu_busy_percent' else [1] * len(readings)
                        denominator = sum(weights) if metric == 'gpu_busy_percent' else 1
                        values = [sum(r['buckets'][b] * weight for (_, r), weight in zip(readings, weights)) / denominator for b in sorted(shared)]
                        # Peak uses per-container maxima: a conservative upper bound for aggregate CPU/RAM,
                        # and any GPU container's maximum for low-activity candidate gating.
                        peaks = [max(v for _, v in r['values']) for _, r in readings]
                        mean = sum(values) / len(values)
                        peak = max(peaks) if metric == 'gpu_busy_percent' else sum(peaks)
                        if not all(math.isfinite(value) for value in [*values, mean, peak]):
                            metrics[metric] = unknown(metric, 'Non-finite workload aggregate was rejected.')
                            continue
                        windows[metric] = dict(zip(sorted(shared), values))
                        m = {'status': 'observed', 'mean': round(mean, 4),
                             'p95': sorted(values)[math.ceil(len(values) * .95) - 1],
                             'peak': peak,
                             'unit': unit, 'source_metric': source_metric, 'scope': 'pod-containers',
                             'sample_start': iso(max(r['values'][0][0] for _, r in readings)),
                             'sample_end': iso(min(r['values'][-1][0] for _, r in readings)),
                             'sample_count': min(len(r['values']) for _, r in readings),
                             'coverage_percent': round(coverage, 1),
                             'timeline': _timeline(windows[metric], now),
                             'reason': 'Runtime identity matched. Adapter-asserted attribution; equal-weight aligned minute means. Not independent device-ownership verification.'}
                        if cadvisor_mode:
                            m['reason'] = ('Exact current Pod UID and full container runtime cgroup matched. Administrator-asserted original source timestamps; equal-weight aligned minute means of adjacent CPU counter interval rates. Not exact continuous integration; peak is an interval-average bound, not instantaneous CPU.' if metric == 'cpu_cores' else 'Exact current Pod UID and full container runtime cgroup matched. Administrator-asserted original source gauge timestamps; equal-weight aligned minute means. Working set is usage minus inactive_file, not RSS, heap or guaranteed nonreclaimable memory.')
                        if metric == 'cpu_cores':
                            gpu_readings = [r for c, r in readings if c['gpu_count'] is not None and c['gpu_count'] > 0]
                            gpu_container_cpu = {b: sum(r['buckets'][b] for r in gpu_readings) for b in shared}
            elif metric == 'gpu_busy_percent' and workload['requests']['gpu_count'] == 0:
                m = unknown(metric, 'No supported GPU resource is requested. Custom accelerator resources are not classified; acceleration benefit is unmeasured.')
            metrics[metric] = m
        assessment = {'verdict': 'insufficient-evidence', 'confidence': 'none',
                      'reason': 'Collect complete, fresh runtime-attributed CPU and GPU evidence before suggesting a resource comparison.',
                      'performance': 'unknown', 'cost': 'unknown', 'automatic_action': False}
        cpu, gpu = metrics['cpu_cores'], metrics['gpu_busy_percent']
        if cadvisor_mode:
            assessment['reason'] = cadvisor.GPU_REASON
        elif workload['requests']['gpu_count'] == 0:
            assessment.update(verdict='cpu-requested', reason='No supported GPU resource is requested. Custom accelerator resources are not classified; this does not prove CPU is optimal or GPU would help.')
        elif cpu['status'] == gpu['status'] == 'observed' and len(set(windows['cpu_cores']) & set(windows['gpu_busy_percent'])) >= 12:
            shared = set(windows['cpu_cores']) & set(windows['gpu_busy_percent'])
            aligned_gpu_mean = sum(windows['gpu_busy_percent'][b] for b in shared) / len(shared)
            aligned_cpu_mean = sum(gpu_container_cpu[b] for b in shared) / len(shared)
            single_gpu_containers = all(c['gpu_count'] == 1 for c in containers if c['gpu_count'] != 0)
            unclassified = any(c['unclassified_extended_resources'] for c in containers)
            assessment.update(confidence='limited', basis={'aligned_minute_buckets': len(shared),
                              'gpu_container_cpu_mean_cores': round(aligned_cpu_mean, 4),
                              'aligned_gpu_busy_mean_percent': round(aligned_gpu_mean, 4),
                              'one_gpu_per_container': single_gpu_containers, 'recognized_resources_only': not unclassified})
            if single_gpu_containers and not unclassified and aligned_gpu_mean <= 15 and gpu['peak'] <= 25 and aligned_cpu_mean >= .1:
                assessment.update(verdict='cpu-comparison-candidate', reason='Sustained low attributed GPU activity with concurrent CPU work in the GPU-bearing containers makes a controlled CPU comparison worth testing. I/O, transfer, synchronization or workload phases can cause the same pattern; CPU superiority is unproven.')
            elif aligned_gpu_mean >= 50:
                assessment.update(verdict='gpu-activity-observed', reason='Substantial attributed GPU activity was observed. Activity alone does not prove GPU is faster, cheaper or necessary.')
            elif unclassified:
                assessment.update(verdict='inconclusive', reason='Unclassified extended resources may include accelerators; CPU comparison eligibility is unknown. Observations cover recognized resources only.')
            elif not single_gpu_containers:
                assessment.update(verdict='inconclusive', reason='Per-container multi-GPU averages can hide a saturated device. Per-device maximum evidence is unavailable, so a CPU comparison candidate is not inferred.')
            else:
                assessment.update(verdict='inconclusive', reason='Attributed activity does not identify the best resource. Measure matched CPU and GPU trials, including correctness and end-to-end cost.')
        out.update(metrics=metrics, assessment=assessment, comparison_plan=COMPARISON_PLAN)
        result.append(out)
    return result


class WorkloadAdvisor:
    def __init__(self, source, reader=None):
        self.source, self.reader, self.error = source, reader, None
        self.metrics_source, self.timestamps_preserved = 'custom-adapter', False
        enabled = os.getenv('DAVANO_WORKLOAD_ADVISOR_ENABLED', 'false').lower() == 'true'
        selected = os.getenv('DAVANO_WORKLOAD_METRICS_SOURCE', 'custom-adapter')
        # Keep the longstanding injected custom-reader test seam, but a selected
        # or typed cAdvisor reader must never bypass its own opt-in or assertion,
        # nor reinterpret unsupported GPU data as custom-adapter evidence.
        if selected == cadvisor.SOURCE or isinstance(reader, cadvisor.CadvisorPrometheusReader):
            self.metrics_source = cadvisor.SOURCE
            if source != 'kubernetes' or not enabled:
                self.reader = None
                return
            if selected != cadvisor.SOURCE:
                self.reader = None
                self.error = 'A cAdvisor reader requires DAVANO_WORKLOAD_METRICS_SOURCE=cadvisor-cpu-memory.'
                return
        if source == 'kubernetes' and enabled:
            if selected not in ('custom-adapter', cadvisor.SOURCE):
                self.reader = None
                self.error = 'DAVANO_WORKLOAD_METRICS_SOURCE must be custom-adapter or cadvisor-cpu-memory.'
                return
            self.metrics_source = selected
            if selected == cadvisor.SOURCE:
                self.timestamps_preserved = os.getenv('DAVANO_CADVISOR_TIMESTAMPS_PRESERVED', 'false').lower() == 'true'
                if not self.timestamps_preserved:
                    self.reader = None
                    self.error = ('cAdvisor reads require DAVANO_CADVISOR_TIMESTAMPS_PRESERVED=true: '
                                  'an administrator assertion that original source timestamps survive the entire pipeline. '
                                  'No Prometheus read was attempted. ' + cadvisor.TIMESTAMP_DETAIL)
                    return
                if isinstance(reader, PrometheusReader) and set(reader.metric_names) != set(cadvisor.CadvisorPrometheusReader.metric_names):
                    self.reader = None
                    self.error = 'cAdvisor mode requires a fixed raw reader for only the CPU counter and memory working-set metrics.'
                    return
            if reader is None:
                try:
                    reader_type = cadvisor.CadvisorPrometheusReader if selected == cadvisor.SOURCE else WorkloadPrometheusReader
                    self.reader = reader_type(os.getenv('DAVANO_PROMETHEUS_URL', ''), os.getenv('DAVANO_PROMETHEUS_TOKEN_PATH'), os.getenv('DAVANO_PROMETHEUS_CA'), cluster=os.getenv('DAVANO_PROMETHEUS_CLUSTER'))
                except ValueError:
                    self.error = ('Workload observation needs a verified HTTPS Prometheus origin and the documented cAdvisor source contract.'
                                  if selected == cadvisor.SOURCE else
                                  'Workload observation needs a verified HTTPS Prometheus origin and the documented runtime-attribution adapter.')

    def snapshot(self, workloads, observed_at, now_epoch=None):
        now = time.time() if now_epoch is None else now_epoch
        cadvisor_mode = self.metrics_source == cadvisor.SOURCE
        cluster = getattr(self.reader, 'cluster', None)
        status, detail, series = 'disabled', 'Optional workload metrics are disabled. Requests are not measurements; no resource moves are made.', []
        if self.source == 'demo':
            detail = 'Demo Pod requests are synthetic. No workload measurements or CPU/GPU comparison results are fabricated.'
        elif self.error:
            status, detail = 'invalid-config', self.error
        elif self.reader:
            try:
                series = self.reader.samples()
                status = 'connected' if series else 'no-data'
                detail = ('Read-only cAdvisor CPU counter and memory working-set evidence. Original source timestamps are '
                          'administrator-asserted, not independently verified. Exact current runtime cgroup identity is required. '
                          'No GPU evidence or CPU/GPU suitability verdict is available.' if cadvisor_mode else
                          'Read-only administrator-supplied adapter evidence. Attribution is asserted by the adapter and checked against current Pod/container identity.')
            except ObservationUnavailable as error:
                status, detail = 'unavailable', str(error)
        try:
            if cadvisor_mode:
                series = cadvisor.prepare_series(series, workloads, now, cluster, _epoch, WINDOW, POLICY['max_gap_seconds'])
            items = _normalize(series, workloads, now, cluster, cadvisor_mode)
        except (ObservationUnavailable, ValueError, TypeError, AttributeError, KeyError, OverflowError, RecursionError):
            status, detail = 'unavailable', 'Malformed or excessive workload evidence was rejected; measurements remain unknown.'
            items = _normalize([], workloads, now, None, cadvisor_mode)
        result = {'status': status, 'detail': detail, 'source': self.source, 'observed_at': iso(now),
                  'metrics_source': self.metrics_source,
                  'measurement_source': (cadvisor.MEASUREMENT_SOURCE if cadvisor_mode else 'prometheus-custom-adapter') if self.source == 'kubernetes' and self.reader else None,
                  'inventory_observed_at': observed_at, 'window_start': iso(now - WINDOW), 'window_end': iso(now),
                  'read_only': True, 'window_seconds': WINDOW, 'policy': POLICY, 'workloads': items,
                  'limitations': ['No automatic placement, resize, submission or migration.',
                                  'Container adapter implementation and GPU attribution have not been validated on real hardware.',
                                  'No stock device metric is attributed to a workload from Pod labels alone.',
                                  'Only the current rolling window is observed; no durable time-series history is stored.',
                                  'GPU requests include only recognized NVIDIA/AMD/Intel resource keys; custom accelerator resources are not classified.']}
        if cadvisor_mode:
            result['input_requirements'] = cadvisor.input_requirements(self.timestamps_preserved, cluster)
            result['limitations'] = ['No automatic placement, resize, submission or migration.',
                                    'cAdvisor source timestamps are administrator-asserted, not independently verified; rewritten timestamps invalidate freshness evidence.',
                                    'Only exact supported containerd and CRI-O cgroup paths are accepted. No names-only or arbitrary-root fallback.',
                                    'CPU observations are equal-weight minute means of adjacent counter interval averages, not exact continuous integration; peaks are not instantaneous.',
                                    'Memory working set is usage minus inactive_file, not RSS, heap or guaranteed nonreclaimable memory.',
                                    'No GPU evidence or CPU/GPU suitability verdict is provided by this source.',
                                    'No real-cluster or hardware validation has been performed; only the current rolling window is observed.']
            for item in items:
                for metric in ('cpu_cores', 'memory_bytes'):
                    item['metrics'][metric]['provenance'] = cadvisor.provenance(metric, self.timestamps_preserved)
        return result
