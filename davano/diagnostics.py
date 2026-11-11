"""On-demand bounded deployment evidence, never deployment certification.

Only fixed GET requests are made. Configuration, discovery, and a successful
bounded list remain distinct from authorization, enforcement, or runtime proof.
"""
from collections import Counter
from datetime import datetime, timezone
import hmac
import json
import math
import os
from pathlib import Path
import re
import ssl
import time
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request

from fastapi import APIRouter, Header, HTTPException, Request as APIRequest
from .inventory import KubernetesInventory, open_without_redirect
from .pending_visibility import parse_targets

MAX_REQUESTS = 12
REQUEST_TIMEOUT_SECONDS = 2
REPORT_BUDGET_SECONDS = 8
MAX_RESPONSE_BYTES = 256 * 1024
MAX_TOKEN_BYTES = 64 * 1024
LIST_LIMIT = 1
STATUSES = ('observed', 'configured', 'disabled', 'unverified', 'blocked', 'unavailable')
CONFIG_KEYS = (
    'DAVANO_CONTROL_ENABLED', 'DAVANO_EXECUTION_ENABLED', 'DAVANO_QUEUE_OBSERVABILITY_ENABLED',
    'DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED', 'DAVANO_KUEUE_PENDING_LOCAL_QUEUES',
    'DAVANO_KUEUE_API_VERSION', 'DAVANO_PROMETHEUS_URL', 'DAVANO_PROMETHEUS_TOKEN_PATH',
    'DAVANO_PROMETHEUS_CA', 'DAVANO_K8S_API', 'DAVANO_K8S_TOKEN_PATH', 'DAVANO_K8S_CA',
    'KUBERNETES_SERVICE_HOST', 'KUBERNETES_SERVICE_PORT_HTTPS',
)
LIMITATIONS = [
    'This report is not a production-readiness verdict or a complete inventory.',
    'Configuration is not observation. API discovery advertises resources, not the caller’s permitted verbs.',
    'A successful bounded list proves only that particular read at that time. No write RBAC or mutations are tested.',
    'Controller correctness, admission enforcement, tenant isolation, runtime execution and physical GPU behavior remain unverified.',
    'Reads are separate, non-atomic observations; results can change immediately.',
    'The time budget is checked between transport operations; operating-system DNS and a blocking transport operation can exceed it.',
]


class ProbeFailure(Exception):
    def __init__(self, status, detail):
        self.status, self.detail = status, detail


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('Duplicate JSON member')
        value[key] = item
    return value


def reject_constant(_):
    raise ValueError('Non-finite JSON number')


def finite_float(raw):
    value = float(raw)
    if not math.isfinite(value):
        raise ValueError('Non-finite JSON number')
    return value


def check(identity, status, evidence, detail, next_step, **extra):
    return dict(id=identity, status=status, evidence=evidence, detail=detail, next_step=next_step, **extra)


def enabled(env, name):
    return env.get(name, 'false').lower() == 'true'


def configured_modes(env):
    return {**{name: 'enabled' if enabled(env, key) else 'disabled' for name, key in (
        ('control', 'DAVANO_CONTROL_ENABLED'), ('execution', 'DAVANO_EXECUTION_ENABLED'),
        ('queue_observability', 'DAVANO_QUEUE_OBSERVABILITY_ENABLED'),
        ('pending_visibility', 'DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED'))},
        'prometheus': 'configured' if env.get('DAVANO_PROMETHEUS_URL') else 'disabled'}


def origin(value):
    p = urlparse(value)
    if (p.scheme != 'https' or not p.hostname or p.username or p.password or p.path not in ('', '/')
            or p.query or p.fragment or any(c.isspace() for c in value) or p.port == 0):
        raise ValueError('Invalid origin')
    return value.rstrip('/')


def credential(path):
    # Refuse special files, bound reads, support projected-token symlinks, and
    # re-read on each report so rotated credentials are not kept by the service.
    file = Path(path)
    if not file.is_file():
        raise ValueError('Invalid credential file')
    with file.open('rb') as stream:
        raw = stream.read(MAX_TOKEN_BYTES + 1)
    value = raw.decode('utf-8').strip()
    if not value or len(raw) > MAX_TOKEN_BYTES or any(c.isspace() or ord(c) < 32 or ord(c) > 126 for c in value):
        raise ValueError('Invalid credential')
    return value


class BoundedReads:
    def __init__(self, opener=None, clock=None):
        self.opener, self.clock = opener or open_without_redirect, clock or time.monotonic
        self.started = self.clock()
        self.deadline = self.started + REPORT_BUDGET_SECONDS
        self.requests = self.bytes = 0
        self.budget_exhausted = False

    def remaining(self):
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            self.budget_exhausted = True
            raise ProbeFailure('unavailable', 'The report time budget was exhausted; no further requests were started.')
        return remaining

    def get(self, base, path, token, context):
        if self.requests >= MAX_REQUESTS:
            self.budget_exhausted = True
            raise ProbeFailure('unavailable', 'The report request limit was reached; no further requests were started.')
        timeout = min(REQUEST_TIMEOUT_SECONDS, self.remaining())
        req = Request(base + path, method='GET', headers={
            'Accept': 'application/json', **({'Authorization': 'Bearer ' + token} if token else {})})
        self.requests += 1
        try:
            with self.opener(req, context=context, timeout=timeout) as response:
                # urllib read1 yields between available chunks, unlike read(n),
                # allowing the shared deadline to be checked during large bodies.
                read = getattr(response, 'read1', response.read)
                chunks, size = [], 0
                while True:
                    self.remaining()
                    chunk = read(min(16384, MAX_RESPONSE_BYTES + 1 - size))
                    self.bytes += len(chunk)
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        raise ProbeFailure('unavailable', 'A response exceeded the byte limit; it was discarded.')
                    self.remaining()
                    if not chunk:
                        break
                    chunks.append(chunk)
                if getattr(response, 'status', 200) != 200:
                    raise ProbeFailure('unavailable', 'The endpoint did not return HTTP 200.')
                if response.headers.get('Warning'):
                    raise ProbeFailure('unavailable', 'The endpoint reported warnings; incomplete evidence was discarded.')
            data = json.loads(b''.join(chunks), object_pairs_hook=unique_object, parse_constant=reject_constant, parse_float=finite_float)
            self.remaining()
            if not isinstance(data, dict) or data.get('warnings'):
                raise ValueError('Malformed response')
            return data
        except ProbeFailure:
            raise
        except HTTPError as error:
            code = error.code
            error.close()
            if code in (401, 403):
                raise ProbeFailure('blocked', 'The endpoint denied this GET request; verify read permissions and credentials.') from None
            if code == 404:
                raise ProbeFailure('blocked', 'The requested API was not served; verify the configured dependency installation.') from None
            raise ProbeFailure('unavailable', 'The HTTPS GET did not succeed; redirects are not followed and error bodies are discarded.') from None
        except Exception:
            raise ProbeFailure('unavailable', 'The HTTPS read or response validation failed; source details were withheld.') from None


def list_evidence(data, kind):
    items, meta = data.get('items'), data.get('metadata')
    if (data.get('kind') != kind + 'List' or data.get('apiVersion') != 'v1'
            or not isinstance(items, list) or len(items) > LIST_LIMIT or not isinstance(meta, dict)
            or not isinstance(meta.get('continue', ''), str)):
        raise ValueError('Malformed list')
    for item in items:
        if (not isinstance(item, dict) or not isinstance(item.get('metadata'), dict)
                or not isinstance(item['metadata'].get('name'), str) or not 1 <= len(item['metadata']['name']) <= 253
                or item.get('kind', kind) != kind or item.get('apiVersion', 'v1') != 'v1'
                or not isinstance(item.get('spec', {}), dict) or not isinstance(item.get('status', {}), dict)):
            raise ValueError('Malformed list item')
    return {'returned_items': len(items), 'has_more': bool(meta.get('continue'))}


def api_evidence(data, group_version, required, verbs=('get', 'list')):
    resources = data.get('resources')
    if data.get('kind') != 'APIResourceList' or data.get('groupVersion') != group_version or not isinstance(resources, list):
        raise ValueError('Malformed discovery')
    advertised = {}
    for resource in resources:
        if (not isinstance(resource, dict) or not isinstance(resource.get('name'), str)
                or not re.fullmatch(r'[a-z0-9][a-z0-9./-]{0,199}', resource['name'])
                or type(resource.get('namespaced')) is not bool or not isinstance(resource.get('kind'), str)
                or not re.fullmatch(r'[A-Za-z][A-Za-z0-9]{0,127}', resource['kind'])
                or not isinstance(resource.get('verbs'), list) or len(resource['verbs']) > 64
                or not all(isinstance(v, str) and re.fullmatch('[a-z]{1,32}', v) for v in resource['verbs'])
                or len(set(resource['verbs'])) != len(resource['verbs'])
                or resource['name'] in advertised):
            raise ValueError('Malformed resource')
        advertised[resource['name']] = resource
    if any(name not in advertised or advertised[name]['namespaced'] != namespaced
           or not set(verbs).issubset(advertised[name]['verbs']) for name, namespaced in required):
        raise ProbeFailure('blocked', 'Discovery did not advertise the required resources, scopes and server-supported verbs.')


def served_version(data, group):
    versions = data.get('versions')
    if data.get('kind') != 'APIGroup' or data.get('name') != group or not isinstance(versions, list):
        raise ValueError('Malformed API group')
    found = set()
    for item in versions:
        if (not isinstance(item, dict) or not isinstance(item.get('version'), str)
                or not re.fullmatch(r'v[0-9]{1,3}(?:(?:alpha|beta)[0-9]{1,3})?', item['version'])
                or item.get('groupVersion') != group + '/' + item['version'] or item['version'] in found):
            raise ValueError('Malformed API version')
        found.add(item['version'])
    version = next((v for v in ('v1beta2', 'v1beta1') if v in found), None)
    if not version:
        raise ProbeFailure('blocked', 'No supported dependency API version was advertised.')
    return version


class DeploymentDiagnostics:
    def __init__(self, source, env=None, opener=None, clock=None, inventory=None):
        self.source = source
        settings = os.environ if env is None else env
        self.env = {key: settings[key] for key in CONFIG_KEYS if key in settings}
        # Embedders supplying a real inventory adapter must diagnose that same
        # connection, not a second cluster selected from process environment.
        if isinstance(inventory, KubernetesInventory):
            self.env.update(DAVANO_K8S_API=inventory.api, DAVANO_K8S_TOKEN_PATH=inventory.token_path, DAVANO_K8S_CA=inventory.ca_path)
            opener = opener or inventory.opener
        self.opener, self.clock = opener, clock

    def report(self):
        env, modes = self.env, configured_modes(self.env)
        reads = BoundedReads(self.opener, self.clock)
        checks = [check('mode-' + key, 'configured' if mode in ('enabled', 'configured') else 'disabled', 'configuration',
                        key.replace('_', ' ').capitalize() + ' is ' + mode + ' in server configuration; this is not observed behavior.',
                        'Validate the relevant dependency and runtime separately.' if mode != 'disabled' else 'Enable only after administrator review.')
                  for key, mode in modes.items()]
        if self.source == 'demo':
            checks.append(check('demo-source', 'configured', 'configuration', 'Demo source selected. No external requests or credential-file reads were made.', 'Select a live source deliberately before running live diagnostics.'))
        else:
            self._live(reads, checks, modes)
        for identity, detail, step in (
            ('write-authorization', 'Mutation RBAC is unverified. No writes, dry runs or access-review POSTs were attempted.', 'Review least-privilege RBAC and validate authorized mutations separately in a test namespace.'),
            ('controller-enforcement', 'Controller reconciliation and admission enforcement are unverified.', 'Inspect current-generation conditions and perform authorized negative admission tests separately.'),
            ('tenant-isolation', 'Network, credential, host and tenant isolation are unverified.', 'Validate CNI enforcement, workload security and inherited permissions on the target cluster.'),
            ('runtime-hardware', 'Job execution, exporter measurements, GPU health and physical isolation are unverified.', 'Run separately authorized runtime and hardware validation on the target deployment.'),
        ):
            checks.append(check(identity, 'unverified', 'not-checked', detail, step))
        counts = Counter(c['status'] for c in checks)
        return {'schema_version': 1, 'source': self.source, 'observed_at': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
                'mode': 'demo' if self.source == 'demo' else 'read-only', 'configured_modes': modes, 'checks': checks,
                'summary': {'status_counts': {s: counts[s] for s in STATUSES}},
                'bounds': {'requests_attempted': reads.requests, 'max_requests': MAX_REQUESTS,
                           'request_timeout_seconds': REQUEST_TIMEOUT_SECONDS, 'report_budget_seconds': REPORT_BUDGET_SECONDS,
                           'max_response_bytes': MAX_RESPONSE_BYTES, 'response_bytes_read': reads.bytes,
                           'list_limit': LIST_LIMIT, 'pagination_followed': False, 'budget_exhausted': reads.budget_exhausted},
                'limitations': LIMITATIONS.copy()}

    def _live(self, reads, checks, modes):
        env = self.env
        connection = None
        try:
            host = env.get('KUBERNETES_SERVICE_HOST', '')
            if ':' in host and not host.startswith('['):
                host = '[' + host + ']'
            base = origin(env.get('DAVANO_K8S_API') or ('https://' + host + ':' + env.get('KUBERNETES_SERVICE_PORT_HTTPS', '443') if host else ''))
            secret = credential(env.get('DAVANO_K8S_TOKEN_PATH', '/var/run/secrets/kubernetes.io/serviceaccount/token'))
            context = ssl.create_default_context(cafile=env.get('DAVANO_K8S_CA', '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'))
            connection = base, secret, context
            checks.append(check('kubernetes-configuration', 'configured', 'configuration', 'HTTPS origin, CA and nonempty credential material could be loaded. No access is established by this check.', 'Inspect the separate read observations below.'))
        except Exception:
            checks.append(check('kubernetes-configuration', 'blocked', 'configuration', 'Kubernetes HTTPS origin, CA or credential configuration could not be validated; values were withheld.', 'Check server-side HTTPS origin, CA and service-account token configuration.'))

        cache = {}
        def get(path):
            if path not in cache:
                if connection:
                    base, secret, context = connection
                    cache[path] = reads.get(base, path, secret, context)
                else:
                    raise ProbeFailure('blocked', 'Kubernetes connection configuration is unavailable; no request was made.')
            return cache[path]

        def probe(identity, evidence, function, detail, step):
            try:
                result = function()
                checks.append(check(identity, 'observed', evidence, detail, step, **({'counts': result} if isinstance(result, dict) else {})))
            except ProbeFailure as error:
                checks.append(check(identity, error.status, 'not-checked', error.detail, step))
            except Exception:
                checks.append(check(identity, 'unavailable', 'not-checked', 'Malformed or unexpected evidence was discarded; no source contents were exposed.', step))

        for resource, kind in (('nodes', 'Node'), ('pods', 'Pod')):
            probe('inventory-' + resource, 'list-read', lambda r=resource, k=kind: list_evidence(get('/api/v1/' + r + '?limit=1'), k),
                  'One bounded inventory list request succeeded. This is not full inventory, capacity, utilization or write-permission evidence.',
                  'If full inventory still fails, inspect inventory parsing, complete-list permissions and pagination separately.')

        def discover(identity, group_version, required):
            probe(identity, 'api-discovery', lambda: api_evidence(get('/apis/' + group_version), group_version, required),
                  'Required API resources, scopes and read verbs were advertised. Discovery does not establish the caller’s resource permissions or controller health.',
                  'Review resource-specific RBAC and controller installation separately.')

        managed = modes['control'] == 'enabled' or modes['execution'] == 'enabled'
        if managed:
            discover('managed-resource-api', 'fleet.davano.io/v1alpha1', [('gpupools', False), ('gpuaccesspolicies', True)])
            discover('admission-api', 'admissionregistration.k8s.io/v1', [('validatingadmissionpolicies', False), ('validatingadmissionpolicybindings', False)])
            version = env.get('DAVANO_KUEUE_API_VERSION', 'v1beta1')
            if version in ('v1beta1', 'v1beta2'):
                discover('managed-kueue-api', 'kueue.x-k8s.io/' + version, [('resourceflavors', False), ('clusterqueues', False), ('localqueues', True)])
            else:
                checks.append(check('managed-kueue-api', 'blocked', 'configuration', 'The configured Kueue API version is unsupported.', 'Configure a supported Kueue API version.'))
        if modes['execution'] == 'enabled':
            discover('batch-api', 'batch/v1', [('jobs', True)])
            discover('network-policy-api', 'networking.k8s.io/v1', [('networkpolicies', True)])
        if modes['queue_observability'] == 'enabled':
            def queue_api():
                group = 'kueue.x-k8s.io'
                version = served_version(get('/apis/' + group), group)
                gv = group + '/' + version
                api_evidence(get('/apis/' + gv), gv, [('localqueues', True), ('clusterqueues', False), ('workloads', True)], verbs=('list',))
            probe('queue-observability-api', 'api-discovery', queue_api,
                  'Supported queue-observability APIs were advertised. Queue lists and their permissions were not tested.',
                  'Validate read permissions and bounded queue observations separately.')
        if modes['pending_visibility'] == 'enabled':
            try:
                if modes['queue_observability'] != 'enabled':
                    raise ValueError('Queue observation disabled')
                targets = parse_targets(env.get('DAVANO_KUEUE_PENDING_LOCAL_QUEUES', ''))
                checks.append(check('pending-visibility-configuration', 'configured', 'configuration',
                                    'Bounded server-side queue targets are valid; names were withheld.',
                                    'Discovery does not establish target access.', counts={'configured_targets': len(targets)}))
                def pending_api():
                    group = 'visibility.kueue.x-k8s.io'
                    version = served_version(get('/apis/' + group), group)
                    gv = group + '/' + version
                    api_evidence(get('/apis/' + gv), gv, [('localqueues/pendingworkloads', True)], verbs=('get',))
                probe('pending-visibility-api', 'api-discovery', pending_api,
                      'A supported pending-visibility API was advertised. No target queue or pending-workload data was requested.',
                      'Validate permissions for the configured target queues separately.')
            except Exception:
                checks.append(check('pending-visibility-configuration', 'blocked', 'configuration',
                                    'Pending visibility requires enabled queue observation and valid bounded server-side targets.',
                                    'Review queue observation and pending visibility target settings.'))
        if modes['prometheus'] == 'configured':
            def prometheus():
                try:
                    base = origin(env['DAVANO_PROMETHEUS_URL'])
                    secret = credential(env['DAVANO_PROMETHEUS_TOKEN_PATH']) if env.get('DAVANO_PROMETHEUS_TOKEN_PATH') else None
                    context = ssl.create_default_context(cafile=env.get('DAVANO_PROMETHEUS_CA'))
                except Exception:
                    raise ProbeFailure('blocked', 'Prometheus HTTPS origin, CA or credential configuration could not be validated; values were withheld.') from None
                data = reads.get(base, '/api/v1/query?query=vector%281%29&timeout=1s', secret, context)
                result = data.get('data', {})
                if data.get('status') != 'success' or not isinstance(result, dict) or result.get('resultType') != 'vector':
                    raise ValueError('Malformed query')
                series = result.get('result')
                if not isinstance(series, list) or len(series) != 1 or not isinstance(series[0], dict) or series[0].get('metric') != {}:
                    raise ValueError('Malformed vector')
                value = series[0].get('value')
                if (not isinstance(value, list) or len(value) != 2 or type(value[0]) not in (int, float)
                        or not math.isfinite(value[0]) or value[0] < 0 or value[1] != '1'):
                    raise ValueError('Malformed value')
            probe('prometheus-query', 'read-query', prometheus,
                  'A fixed constant Prometheus query succeeded. It reads no exporter series and proves no GPU telemetry or metric coverage.',
                  'Validate exporter samples, freshness and cluster identity separately.')


def install_diagnostics_api(app, source, service=None, inventory=None):
    service = service or DeploymentDiagnostics(source, inventory=inventory)
    admin_token = os.getenv('DAVANO_API_TOKEN', '')
    router = APIRouter(tags=['Deployment diagnostics'])

    @router.get('/api/v1/diagnostics')
    def diagnostics(request: APIRequest, authorization: str | None = Header(default=None)):
        if source != 'demo':
            if not 32 <= len(admin_token) <= 4096:
                raise HTTPException(503, 'Live diagnostics require a configured administrator Bearer token of 32 to 4096 characters')
            candidate = authorization[7:] if authorization and authorization.startswith('Bearer ') else ''
            if not 32 <= len(candidate) <= 4096 or not hmac.compare_digest(candidate.encode('utf-8'), admin_token.encode('utf-8')):
                raise HTTPException(401, 'A valid administrator Bearer token is required', headers={'WWW-Authenticate': 'Bearer'})
        if request.url.query:
            raise HTTPException(400, 'Diagnostics accept no query parameters or browser-supplied targets')
        return service.report()

    app.include_router(router)
    app.state.diagnostics = service
    return service
