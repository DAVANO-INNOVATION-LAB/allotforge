"""Opt-in read-only evidence from vendor exporters and Kubernetes queue status.

Inventory counts never become utilization, physical device IDs, or GPU health.
Raw Prometheus range vectors preserve scrape timestamps. Error bodies, arbitrary
labels and workload condition messages never reach the browser.
"""
import hashlib
import json
import math
import os
import re
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request

from .inventory import open_without_redirect
from .kube import ApiError, KubeClient

STALE_AFTER_SECONDS = 120
OBSERVATION_WINDOW_SECONDS = 900
MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MAX_SERIES = 10000
MAX_SAMPLES = 150000

METRICS = {
    'utilization_percent': ('%', 0, 100),
    'memory_used_bytes': ('bytes', 0, 2 ** 60),
    'memory_total_bytes': ('bytes', 0, 2 ** 60),
    'temperature_celsius': ('°C', -40, 250),
    'power_watts': ('W', 0, 10000),
    'ecc_uncorrected_errors': ('count', 0, 2 ** 53),
}
# Each mapping's scale is tied to its source name, never guessed from the value.
NVIDIA = {
    'DCGM_FI_DEV_GPU_UTIL': ('utilization_percent', 1),
    'DCGM_FI_DEV_GPU_UTIL_RATIO': ('utilization_percent', 100),
    'DCGM_FI_DEV_FB_USED': ('memory_used_bytes', 1024 ** 2),
    'DCGM_FI_DEV_FB_USED_BYTES': ('memory_used_bytes', 1),
    'DCGM_FI_DEV_FB_TOTAL': ('memory_total_bytes', 1024 ** 2),
    'DCGM_FI_DEV_FB_CAPACITY_BYTES': ('memory_total_bytes', 1),
    'DCGM_FI_DEV_GPU_TEMP': ('temperature_celsius', 1),
    'DCGM_FI_DEV_GPU_TEMP_CELSIUS': ('temperature_celsius', 1),
    'DCGM_FI_DEV_POWER_USAGE': ('power_watts', 1),
    'DCGM_FI_DEV_BOARD_POWER_WATTS': ('power_watts', 1),
    'DCGM_FI_DEV_ECC_DBE_VOL_TOTAL': ('ecc_uncorrected_errors', 1),
}
AMD = {
    'gpu_gfx_activity': ('utilization_percent', 1),
    'gpu_junction_temperature': ('temperature_celsius', 1),
    'gpu_power_usage': ('power_watts', 1),
    'gpu_ecc_uncorrect_total': ('ecc_uncorrected_errors', 1),
}
# AMD's optional custom prefix is deliberately restricted to the documented amd_.
AMD.update({'amd_' + name: mapping for name, mapping in tuple(AMD.items())})
ADAPTERS = [
    {'id': 'nvidia-dcgm', 'vendor': 'NVIDIA', 'contract': 'DCGM Exporter 4.8.x and 4.9.0 field names',
     'runtime_version': 'unknown', 'status': 'implemented-unverified-on-hardware'},
    {'id': 'amd-device-metrics', 'vendor': 'AMD', 'contract': 'Device Metrics Exporter 1.5.3 selected metrics; gpu_uuid required',
     'runtime_version': 'unknown', 'status': 'implemented-unverified-on-hardware'},
    {'id': 'intel', 'vendor': 'Intel', 'contract': None, 'runtime_version': 'unknown',
     'status': 'not-implemented'},
]


class ObservationUnavailable(RuntimeError):
    pass


def iso(timestamp):
    try:
        return datetime.fromtimestamp(timestamp, timezone.utc).isoformat().replace('+00:00', 'Z')
    except (ValueError, OverflowError, OSError, TypeError):
        return None


def token(value, limit=128):
    """Only bounded identifier-like metadata, never raw arbitrary source strings."""
    return value if isinstance(value, str) and len(value) <= limit and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:/ -]*', value) else None


def metric_unknown(name, reason='This metric was not reported by a supported exporter.'):
    return {'value': None, 'unit': METRICS[name][0], 'status': 'unknown', 'sample_at': None,
            'source_metric': None, 'scope': 'unknown', 'reason': reason}


class PrometheusReader:
    """Fixed server-side query; no caller-supplied URL, query, or credential."""
    def __init__(self, origin, token_path=None, ca_path=None, opener=None, cluster=None):
        parsed = urlparse(origin)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ('', '/') or parsed.query or parsed.fragment):
            raise ValueError('Prometheus requires an HTTPS origin without credentials or a path.')
        if cluster is not None and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', cluster):
            raise ValueError('Invalid Prometheus cluster label selector.')
        self.cluster = cluster
        self.origin = origin.rstrip('/')
        self.token_path, self.ca_path = token_path, ca_path
        self.opener = opener or open_without_redirect

    def samples(self):
        # A normal instant vector replaces sample times with evaluation time;
        # asking for a raw range vector lets us identify stale source samples.
        selector = '__name__=~"' + '|'.join(sorted(set(NVIDIA) | set(AMD))) + '"'
        if self.cluster:
            selector += ',cluster=' + json.dumps(self.cluster)
        query = '{' + selector + '}[15m]'
        path = '/api/v1/query?' + urlencode({'query': query, 'timeout': '5s'})
        try:
            headers = {'Accept': 'application/json'}
            if self.token_path:
                secret = Path(self.token_path).read_text().strip()
                if not secret or '\n' in secret or '\r' in secret:
                    raise ValueError('Invalid token file')
                headers['Authorization'] = 'Bearer ' + secret
            context = ssl.create_default_context(cafile=self.ca_path)
            request = Request(self.origin + path, headers=headers, method='GET')
            with self.opener(request, context=context, timeout=8) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
            if len(body) > MAX_RESPONSE_BYTES:
                raise ObservationUnavailable('Prometheus response exceeded the safety limit.')
            result = json.loads(body)
            data = result.get('data', {}) if isinstance(result, dict) else {}
            if result.get('status') != 'success' or data.get('resultType') != 'matrix' or not isinstance(data.get('result'), list):
                raise ObservationUnavailable('Prometheus did not return a successful raw sample matrix.')
            if result.get('warnings'):
                raise ObservationUnavailable('Prometheus returned warnings; incomplete results were not accepted.')
            series = data['result']
            if len(series) > MAX_SERIES:
                raise ObservationUnavailable('Prometheus series limit exceeded; partial data was rejected.')
            return series
        except HTTPError as error:
            status = error.code
            error.close()
            raise ObservationUnavailable(f'Prometheus read failed (HTTP {status}).') from None
        except (OSError, URLError, ValueError, TypeError, AttributeError):
            raise ObservationUnavailable('Prometheus HTTPS read unavailable; check origin, CA and credential file.') from None


def _identity(labels, vendor, nodes, expected_cluster=None):
    uuid_key = ('UUID' if labels.get('UUID') else 'uuid') if vendor == 'NVIDIA' else 'gpu_uuid'
    uuid = token(labels.get(uuid_key))
    if not uuid or uuid.lower() in ('na', 'n/a', 'unknown', 'none', 'null'):
        return None
    # GPU indices are not stable identity. UUID is always from exporter evidence.
    if not re.fullmatch(r'(?:GPU-|MIG-(?:GPU-)?)?[0-9a-fA-F-]{8,}(?:/[0-9]+/[0-9]+)?', uuid):
        return None
    hostname = token(labels.get('hostname') or labels.get('Hostname') or labels.get('node'))
    host_evidence = {token(labels.get(k)) for k in ('hostname', 'Hostname', 'node')} - {None}
    matches = {n['name'] for n in nodes if n['name'] in host_evidence or n.get('labels', {}).get('kubernetes.io/hostname') in host_evidence}
    cluster = token(labels.get('cluster'))
    # A federated source cannot join a node from another/unknown cluster simply
    # because it has the same short hostname. Keep its evidence separate.
    if cluster is not None and cluster != expected_cluster:
        matches = set()
    node_match = 'matched' if len(matches) == 1 else 'ambiguous' if len(matches) > 1 else 'unmatched'
    instance_id, compute_id, partition_id = None, None, None
    kind = 'gpu'
    if vendor == 'NVIDIA':
        instance_id = token(labels.get('GPU_I_ID'))
        compute_id = token(labels.get('GPU_CI_ID'))
        if uuid.startswith('MIG-') or instance_id is not None:
            kind = 'mig-instance'
        if compute_id is not None:
            kind = 'mig-compute-instance'
    else:
        partition_id = token(labels.get('gpu_partition_id'))
        partition = (labels.get('gpu_compute_partition_type') or '').lower()
        if partition not in ('spx', 'na', ''):
            kind = 'partition'
        if labels.get('deployment_mode') in ('vm_vf', 'hypervisor'):
            kind = 'virtual-function'
    allowed = ('UUID', 'uuid', 'gpu_uuid', 'gpu', 'gpu_id', 'hostname', 'Hostname', 'node',
               'GPU_I_ID', 'GPU_CI_ID', 'GPU_I_PROFILE', 'gpu_partition_id', 'gpu_compute_partition_type',
               'gpu_memory_partition_type', 'deployment_mode', 'pci_bus_id', 'cluster')
    identity_labels = {k: token(labels[k]) for k in allowed if token(labels.get(k))}
    identity = [cluster, vendor, uuid, hostname, instance_id, compute_id, partition_id, kind]
    return {'id': hashlib.sha256(json.dumps(identity).encode()).hexdigest()[:24], 'uuid': uuid,
            'vendor': vendor, 'cluster': cluster, 'node': next(iter(matches)) if len(matches) == 1 else None,
            'node_match': node_match, 'entity_kind': kind, 'instance_id': instance_id,
            'compute_instance_id': compute_id, 'partition_id': partition_id,
            'model': token(labels.get('modelName') or labels.get('card_model')) or 'Not reported',
            'provenance': {'source': 'prometheus', 'adapter': 'nvidia-dcgm' if vendor == 'NVIDIA' else 'amd-device-metrics',
                           'identity_labels': identity_labels, 'runtime_version': 'unknown'},
            'metrics': {name: metric_unknown(name) for name in METRICS}}


def normalize_metrics(series, nodes, now_epoch=None, expected_cluster=None):
    now_epoch = time.time() if now_epoch is None else now_epoch
    devices, candidates, warnings = {}, {}, set()
    count = 0
    if not isinstance(series, list) or len(series) > MAX_SERIES:
        raise ObservationUnavailable('Invalid or excessive metric series.')
    for item in series:
        if not isinstance(item, dict) or not isinstance(item.get('metric'), dict) or not isinstance(item.get('values'), list):
            raise ObservationUnavailable('Malformed Prometheus metric series.')
        labels, samples = item['metric'], item['values']
        count += len(samples)
        if count > MAX_SAMPLES:
            raise ObservationUnavailable('Prometheus sample limit exceeded; partial data was rejected.')
        metric_name = labels.get('__name__')
        vendor = 'NVIDIA' if metric_name in NVIDIA else 'AMD' if metric_name in AMD else None
        if vendor is None:
            continue
        if expected_cluster and labels.get('cluster') != expected_cluster:
            warnings.add('Series outside the configured cluster selector were discarded.')
            continue
        device = _identity(labels, vendor, nodes, expected_cluster)
        if device is None:
            warnings.add('Some series had no supported exporter UUID; no device identity was invented.')
            continue
        key, scale = (NVIDIA if vendor == 'NVIDIA' else AMD)[metric_name]
        existing = devices.setdefault(device['id'], device)
        if existing['model'] != device['model'] or existing['node_match'] != device['node_match'] or existing['node'] != device['node']:
            existing['node'], existing['node_match'] = None, 'ambiguous'
            warnings.add('Conflicting identity metadata was not merged into a verified node match.')
        value = metric_unknown(key)
        value.update(source_metric=metric_name, scope='device')
        valid_samples = []
        for sample in samples:
            if not isinstance(sample, list) or len(sample) != 2:
                raise ObservationUnavailable('Malformed Prometheus sample.')
            try:
                timestamp = float(sample[0])
                if not math.isfinite(timestamp) or timestamp <= 0 or iso(timestamp) is None:
                    raise ValueError()
            except (ValueError, TypeError, OverflowError):
                raise ObservationUnavailable('Invalid Prometheus sample timestamp.') from None
            valid_samples.append((timestamp, sample[1]))
        if not valid_samples:
            continue
        timestamp, raw = max(valid_samples, key=lambda sample: sample[0])
        value['sample_at'] = iso(timestamp)
        # Pod/process mappings can represent per-process totals rather than full
        # GPU activity (including values >100%). Do not call them device usage.
        workload_scope = any(labels.get(k) for k in ('pod', 'pod_name', 'container', 'container_name', 'vgpu', 'kfd_process_id', 'process_id'))
        if workload_scope and key in ('utilization_percent', 'memory_used_bytes'):
            value.update(scope='workload', reason='Only workload-attributed samples were reported; full-device usage is unknown.')
        elif vendor == 'AMD' and device['entity_kind'] != 'gpu' and key in ('utilization_percent', 'temperature_celsius', 'power_watts'):
            value.update(reason='AMD partition/virtual-function sample does not establish an independent physical sensor value.')
        elif vendor == 'NVIDIA' and device['entity_kind'] != 'gpu' and key in ('temperature_celsius', 'power_watts'):
            value.update(reason='This may be a repeated parent-GPU sensor; per-instance value is unknown.')
        elif timestamp > now_epoch + 5:
            value.update(reason='Exporter sample timestamp is in the future; clock alignment is unknown.')
        elif now_epoch - timestamp > STALE_AFTER_SECONDS:
            value.update(status='stale', reason='The last source sample is older than the freshness threshold.')
        else:
            try:
                if isinstance(raw, bool):
                    raise ValueError()
                numeric = float(raw) * scale
                _, lower, upper = METRICS[key]
                if not math.isfinite(numeric) or not lower <= numeric <= upper:
                    raise ValueError()
                if key == 'ecc_uncorrected_errors' and numeric != int(numeric):
                    raise ValueError()
                value.update(value=numeric, status='fresh', reason=None)
            except (ValueError, TypeError, OverflowError):
                value.update(reason='The exporter returned an invalid, unsupported or out-of-range value.')
        candidates.setdefault((device['id'], key), []).append(value)
    for (identifier, key), values in candidates.items():
        device_values = [v for v in values if v['scope'] == 'device'] or values
        fresh = [v for v in device_values if v['status'] == 'fresh']
        if fresh and any(not math.isclose(v['value'], fresh[0]['value'], rel_tol=1e-6, abs_tol=1e-6) for v in fresh[1:]):
            result = metric_unknown(key, 'Conflicting exporter series were not averaged or added.')
            result['status'] = 'conflict'
        elif fresh:
            result = max(fresh, key=lambda v: v['sample_at'])
        else:
            result = max(device_values, key=lambda v: v['sample_at'] or '')
        devices[identifier]['metrics'][key] = result
    for device in devices.values():
        if device['vendor'] == 'AMD':
            for key in ('memory_used_bytes', 'memory_total_bytes'):
                device['metrics'][key] = metric_unknown(key, 'AMD VRAM byte conversion is not implemented; no units are guessed.')
    return sorted(devices.values(), key=lambda d: (d['vendor'], d['node'] or '', d['uuid'], d['id'])), sorted(warnings)


def conditions(resource):
    """Do not expose arbitrary controller messages, spec, env, or annotations."""
    generation = resource.get('metadata', {}).get('generation')
    output = []
    for item in resource.get('status', {}).get('conditions', []):
        if not isinstance(item, dict):
            continue
        observed = item.get('observedGeneration')
        current = None if type(generation) is not int or type(observed) is not int else observed == generation
        output.append({'type': token(item.get('type')) or 'Unknown',
                       'status': item.get('status') if item.get('status') in ('True', 'False', 'Unknown') else 'Unknown',
                       'reason': token(item.get('reason')) or 'NotReported',
                       'observed_generation': observed if type(observed) is int else None,
                       'current_generation': current,
                       'last_transition_time': item.get('lastTransitionTime') if _valid_time(item.get('lastTransitionTime')) else None})
    return output[:50]


def _valid_time(value):
    if not isinstance(value, str) or len(value) > 40:
        return False
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).tzinfo is not None
    except ValueError:
        return False


def pending_reasons(pod):
    if pod.get('status', {}).get('phase') != 'Pending':
        return []
    result = []
    for condition in conditions(pod):
        if condition['status'] != 'True' and condition['type'] in ('PodScheduled', 'Initialized', 'PodReadyToStartContainers'):
            result.append({'source': 'pod-condition', 'reason': condition['reason'],
                           'detail': f"{condition['type']} is {condition['status']}; inspect the Pod for detailed scheduler diagnostics."})
    for item in pod.get('status', {}).get('containerStatuses', []) + pod.get('status', {}).get('initContainerStatuses', []):
        reason = token(item.get('state', {}).get('waiting', {}).get('reason'))
        if reason:
            result.append({'source': 'container-state', 'reason': reason,
                           'detail': 'A container is waiting; inspect its events and image/configuration.'})
    if not result:
        reason = 'NotBound' if not pod.get('spec', {}).get('nodeName') else 'BoundPending'
        result.append({'source': 'pod-status', 'reason': reason,
                       'detail': 'No detailed pending reason was reported. Pending does not prove GPU quota exhaustion.'})
    return result[:20]


def _condition_bool(items, key):
    condition = next((c for c in items if c['type'] == key), None)
    if condition is None or condition['status'] == 'Unknown' or condition['current_generation'] is not True:
        return None
    return condition['status'] == 'True'


def normalize_queue_resource(resource, kind):
    meta, spec, status = resource.get('metadata', {}), resource.get('spec', {}), resource.get('status', {})
    name, namespace = token(meta.get('name')) or 'unknown', token(meta.get('namespace'))
    item = {'id': f'{namespace}/{name}' if namespace else name, 'name': name, 'namespace': namespace,
            'conditions': conditions(resource), 'origin': 'kueue-api'}
    if kind == 'workloads':
        item.update(queue=token(spec.get('queueName')), cluster_queue=token(status.get('admission', {}).get('clusterQueue')),
                    admitted=_condition_bool(item['conditions'], 'Admitted'), finished=_condition_bool(item['conditions'], 'Finished'))
        item['pending_reasons'] = [{'source': 'kueue-condition', 'reason': c['reason'],
                                   'detail': f"{c['type']} is {c['status']}" + (' (current generation).' if c['current_generation'] else ' (generation not confirmed).')}
                                  for c in item['conditions'] if c['type'] in ('Admitted', 'QuotaReserved', 'PodsReady') and c['status'] != 'True']
        if item['admitted'] is not True and not item['pending_reasons']:
            item['pending_reasons'] = [{'source': 'kueue-status', 'reason': 'AdmissionUnknown',
                                       'detail': 'No current-generation admission result is available.'}]
    else:
        item.update(cluster_queue=token(spec.get('clusterQueue')),
                    active=_condition_bool(item['conditions'], 'Active'))
        for key in ('pendingWorkloads', 'reservingWorkloads', 'admittedWorkloads'):
            value = status.get(key)
            item[key] = value if type(value) is int and value >= 0 else None
    return item


class KueueReader:
    def __init__(self, client=None):
        self.client = client or KubeClient()

    def snapshot(self):
        group = self.client.get('/apis/kueue.x-k8s.io')
        versions = {v.get('version') for v in group.get('versions', [])}
        version = next((v for v in ('v1beta2', 'v1beta1') if v in versions), None)
        if version is None:
            raise ObservationUnavailable('No supported served Kueue API version was discovered.')
        root = '/apis/kueue.x-k8s.io/' + version
        discovery = self.client.get(root)
        available = {r.get('name') for r in discovery.get('resources', []) if 'list' in r.get('verbs', [])}
        required = ('localqueues', 'clusterqueues', 'workloads')
        if not set(required).issubset(available):
            raise ObservationUnavailable('Kueue discovery does not serve all required read-only resources.')
        output = {'status': 'connected', 'detail': 'Read-only Kueue conditions. Lists are not an atomic scheduling decision.', 'api_version': version}
        for resource, field in (('localqueues', 'local_queues'), ('clusterqueues', 'cluster_queues'), ('workloads', 'workloads')):
            output[field] = [normalize_queue_resource(item, resource) for item in self.client.list(root + '/' + resource)]
        return output


class ObservabilityService:
    def __init__(self, source, prometheus=None, queues=None):
        self.source, self.prometheus, self.queues = source, prometheus, queues
        self.telemetry_error = None
        self.queues_error = None
        if source == 'kubernetes' and prometheus is None and os.getenv('DAVANO_PROMETHEUS_URL'):
            try:
                self.prometheus = PrometheusReader(os.environ['DAVANO_PROMETHEUS_URL'], os.getenv('DAVANO_PROMETHEUS_TOKEN_PATH'), os.getenv('DAVANO_PROMETHEUS_CA'), cluster=os.getenv('DAVANO_PROMETHEUS_CLUSTER'))
            except ValueError:
                self.telemetry_error = 'Configure an HTTPS Prometheus origin without embedded credentials or a path.'
        if source == 'kubernetes' and queues is None and os.getenv('DAVANO_QUEUE_OBSERVABILITY_ENABLED', 'false').lower() == 'true':
            try:
                self.queues = KueueReader()
            except ApiError:
                self.queues_error = 'Configure the Kubernetes HTTPS API and read-only queue credentials.'

    def snapshot(self, nodes, observed_at):
        telemetry = {'status': 'disabled', 'detail': 'No Prometheus source configured. No device measurements are inferred from GPU requests.',
                     'stale_after_seconds': STALE_AFTER_SECONDS, 'observation_window_seconds': OBSERVATION_WINDOW_SECONDS,
                     'devices': [], 'adapters': ADAPTERS, 'warnings': [], 'coverage': 'unknown'}
        queues = {'status': 'disabled', 'detail': 'Kueue API observation is opt-in; Pod pending reasons remain available.',
                  'api_version': None, 'local_queues': [], 'cluster_queues': [], 'workloads': []}
        if self.source == 'demo':
            telemetry['detail'] = 'Demo mode has no connected exporter and fabricates no device measurements.'
            queues['detail'] = 'Demo mode has no connected Kueue API.'
        else:
            if self.telemetry_error:
                telemetry.update(status='invalid-config', detail=self.telemetry_error)
            elif self.prometheus:
                try:
                    devices, warnings = normalize_metrics(self.prometheus.samples(), nodes, expected_cluster=getattr(self.prometheus, 'cluster', None))
                    telemetry.update(status='connected' if devices else 'no-data', devices=devices, warnings=warnings,
                                     detail='Exporter-observed entities only; UUIDs do not prove exclusive allocation, completeness or GPU health.')
                except ObservationUnavailable as error:
                    telemetry.update(status='unavailable', detail=str(error))
                except (TypeError, ValueError, AttributeError, KeyError):
                    telemetry.update(status='unavailable', detail='Malformed telemetry evidence was rejected.')
            if self.queues_error:
                queues.update(status='invalid-config', detail=self.queues_error)
            elif self.queues:
                try:
                    queues = self.queues.snapshot()
                except ApiError as error:
                    queues.update(status='unavailable', detail=f'Kueue read unavailable (HTTP {error.status}). Check API installation and read-only RBAC.')
                except ObservationUnavailable as error:
                    queues.update(status='unavailable', detail=str(error))
                except (TypeError, ValueError, AttributeError, KeyError):
                    queues.update(status='unavailable', detail='Malformed Kueue evidence was rejected.')
        return {'source': self.source, 'observed_at': observed_at, 'read_only': True, 'telemetry': telemetry, 'queues': queues}
