"""Narrow read-only cAdvisor CPU/RAM input contract, not GPU attribution.

Only raw source samples from an administrator-asserted timestamp-preserving
pipeline are useful. No scrape/evaluation times or container_last_seen values
are substituted. Supported cgroup paths are generated from the current inventory,
never parsed with substring matches or guessed from container names.
"""
import math
from collections import Counter
import re

from .observability import MAX_SAMPLES, MAX_SERIES, ObservationUnavailable, PrometheusReader

SOURCE = 'cadvisor-cpu-memory'
MEASUREMENT_SOURCE = 'prometheus-cadvisor-cpu-memory'
CPU_METRIC = 'container_cpu_usage_seconds_total'
MEMORY_METRIC = 'container_memory_working_set_bytes'
# A cumulative seconds counter is not a cores gauge. Bound it independently,
# below values where floating point deltas lose all useful precision.
MAX_COUNTER_SECONDS = 2 ** 53
METRICS = {'cpu_cores': (CPU_METRIC, 'cores', 1_000_000),
           'memory_bytes': (MEMORY_METRIC, 'bytes', 2 ** 60),
           'gpu_busy_percent': (None, '%', 100)}
GPU_REASON = 'cAdvisor CPU/RAM mode provides no workload GPU evidence. CPU/GPU suitability, performance and cost remain unknown.'
TIMESTAMP_DETAIL = ('The administrator must assert that the entire cAdvisor-to-Prometheus pipeline preserves original '
                    'ContainerStats.Timestamp values (honor_timestamps: true, or honorTimestamps: true). '
                    'This assertion is not independently verified. Scrape/evaluation timestamps and container_last_seen '
                    'cannot prove source freshness. Trusted node and, when selected, cluster labels must be supplied by '
                    'the scrape pipeline. A 15-second cadence is recommended; 30 seconds cannot meet the sample gate.')


class CadvisorPrometheusReader(PrometheusReader):
    """Same bounded verified-HTTPS reader, with only two fixed raw metric names."""
    metric_names = [CPU_METRIC, MEMORY_METRIC]


def input_requirements(asserted, cluster):
    return {'source_timestamps': 'administrator-asserted' if asserted else 'not-asserted',
            'detail': TIMESTAMP_DETAIL, 'required_labels': ['id', 'namespace', 'pod', 'container', 'node', 'cpu="total" (CPU only)'],
            'recommended_scrape_interval_seconds': 15, 'minimum_cpu_counter_samples': 42,
            'supported_runtimes': ['containerd', 'cri-o'], 'cluster': cluster}


def provenance(metric, asserted):
    cpu = metric == 'cpu_cores'
    return {'source': MEASUREMENT_SOURCE, 'identity': 'exact-current-pod-uid-and-container-runtime-cgroup',
            'source_timestamps': 'administrator-asserted' if asserted else 'not-asserted',
            'temporal_semantics': 'adjacent-counter-interval-average-at-later-source-timestamp' if cpu else 'source-timestamp-gauge',
            'aggregation': 'equal-weight aligned minute means',
            'peak_semantics': 'sum of per-container maximum interval-average rates' if cpu else 'sum of per-container maximum working-set gauges'}


def supported_paths(uid, runtime_id):
    """An exact allowlist for current Kubernetes UUIDs and full container IDs."""
    if not isinstance(uid, str) or not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', uid):
        return ()
    runtime = re.fullmatch(r'(containerd|cri-o)://([0-9a-f]{64})', runtime_id) if isinstance(runtime_id, str) else None
    if not runtime:
        return ()
    kind, identifier = runtime.groups()
    leaf = identifier if kind == 'containerd' else 'crio-' + identifier
    scope = ('cri-containerd-' if kind == 'containerd' else 'crio-') + identifier + '.scope'
    escaped_uid = uid.replace('-', '_')
    paths = []
    for qos in ('', 'burstable', 'besteffort'):
        parent = '/kubepods/' + (qos + '/' if qos else '') + 'pod' + uid
        paths.append(parent + '/' + leaf)
        parent = '/kubepods.slice/' + ('kubepods-' + qos + '.slice/' if qos else '')
        parent += 'kubepods-' + (qos + '-' if qos else '') + 'pod' + escaped_uid + '.slice'
        paths.append(parent + '/' + scope)
    return tuple(paths)


def _counter_rates(samples, now, started, window, max_gap):
    """Adjacent interval averages at the actual later source time, never rate()."""
    if not isinstance(samples, list):
        return [], ('Malformed cAdvisor CPU counter samples were rejected.', 'unknown')
    points = {}
    for sample in samples:
        if not isinstance(sample, list) or len(sample) != 2 or any(isinstance(v, bool) for v in sample):
            return [], ('Malformed cAdvisor CPU counter sample was rejected.', 'unknown')
        try:
            timestamp, value = float(sample[0]), float(sample[1])
            if not math.isfinite(timestamp) or timestamp <= 0 or not math.isfinite(value) or not 0 <= value <= MAX_COUNTER_SECONDS:
                raise ValueError()
        except (TypeError, ValueError, OverflowError):
            return [], ('Non-finite, malformed or out-of-range cAdvisor CPU counter was rejected.', 'unknown')
        if timestamp > now + 5:
            return [], ('Future cAdvisor source sample; clock alignment is unknown.', 'unknown')
        if timestamp > now or timestamp < now - window:
            continue
        if started is None or timestamp < started:
            return [], ('CPU counter samples predate the current container incarnation.', 'unknown')
        if timestamp in points and points[timestamp] != value:
            return [], ('Conflicting CPU counter values at the same source timestamp.', 'conflict')
        points[timestamp] = value
    ordered = sorted(points.items())
    rates = []
    for (before, previous), (after, current) in zip(ordered, ordered[1:]):
        elapsed = after - before
        if elapsed > max_gap:
            return [], ('A CPU counter interval exceeds the allowed 90 seconds.', 'insufficient')
        delta = current - previous
        if delta < 0:
            return [], ('A CPU counter reset or negative delta was rejected; no reset extrapolation is performed.', 'unknown')
        rate = delta / elapsed
        if not math.isfinite(rate) or not 0 <= rate <= METRICS['cpu_cores'][2]:
            return [], ('An out-of-range derived CPU interval rate was rejected.', 'unknown')
        rates.append([after, rate])
    return rates, None


def prepare_series(series, workloads, now, cluster, epoch, window=900, max_gap=90):
    """Prepare only exact current-runtime matches for the shared window gates.

    Failures of known CPU or memory evidence are local to that metric. Response
    shape/size limits still reject the entire response, never a partial prefix.
    A private error field contains only fixed local text, never source strings.
    """
    if not isinstance(series, list) or len(series) > MAX_SERIES:
        raise ObservationUnavailable('Invalid or excessive cAdvisor metric series.')
    identities = {}
    claims = Counter((w['node'], c['runtime_id']) for w in workloads for c in w['containers'])
    for workload in workloads:
        for container in workload['containers']:
            for path in supported_paths(workload['uid'], container['runtime_id']):
                key = (workload['namespace'], workload['name'], container['name'], workload['node'], path)
                identities[key] = (workload['uid'], container['runtime_id'], epoch(container['started_at']),
                                   claims[(workload['node'], container['runtime_id'])] != 1)
    output, count = [], 0
    for item in series:
        if not isinstance(item, dict) or not isinstance(item.get('metric'), dict):
            raise ObservationUnavailable('Malformed cAdvisor metric series.')
        labels, samples = item['metric'], item.get('values')
        if isinstance(samples, list):
            count += len(samples)
        if count > MAX_SAMPLES:
            raise ObservationUnavailable('cAdvisor sample limit exceeded; partial data was rejected.')
        name = labels.get('__name__')
        if name not in (CPU_METRIC, MEMORY_METRIC):
            continue
        if labels.get('cluster') != cluster and (cluster is not None or labels.get('cluster') is not None):
            continue
        key = tuple(labels.get(k) for k in ('namespace', 'pod', 'container', 'node', 'id'))
        if not all(isinstance(v, str) for v in key) or key not in identities:
            continue
        uid, runtime, started, ambiguous = identities[key]
        error = None
        if ambiguous:
            error = ('Ambiguous current container runtime identity appears more than once on this node; cgroup evidence cannot be counted twice.', 'conflict')
        elif ('pod_uid' in labels and labels['pod_uid'] != uid) or ('container_id' in labels and labels['container_id'] != runtime):
            error = ('Conflicting cAdvisor identity labels were rejected.', 'conflict')
        elif name == CPU_METRIC and labels.get('cpu') != 'total':
            error = ('Aggregate CPU requires cpu="total". Per-CPU-only or mixed series cannot be summed into evidence.', 'unknown')
        elif name == CPU_METRIC:
            samples, error = _counter_rates(samples, now, started, window, max_gap)
        elif not isinstance(samples, list):
            error = ('Malformed cAdvisor memory gauge samples were rejected.', 'unknown')
        canonical = {'__name__': name, 'namespace': key[0], 'pod': key[1], 'container': key[2],
                     'node': key[3], 'pod_uid': uid, 'container_id': runtime}
        if cluster is not None:
            canonical['cluster'] = cluster
        prepared = {'metric': canonical, 'values': samples if error is None else []}
        if error:
            prepared['_cadvisor_error'] = error
        output.append(prepared)
    return output
