"""Synthetic evidence and fail-closed transport tests, never live cluster claims."""
from copy import deepcopy
import io
import json
import os
import shutil
import ssl
import subprocess
import threading
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlparse

from fastapi.testclient import TestClient
from davano.app import create_app
from davano.diagnostics import (BoundedReads, DeploymentDiagnostics, MAX_REQUESTS,
                                MAX_RESPONSE_BYTES, MAX_TOKEN_BYTES, ProbeFailure,
                                REPORT_BUDGET_SECONDS, STATUSES)
from davano.inventory import InventoryUnavailable, KubernetesInventory

ADMIN = 'administrator-secret-' + 'a' * 32
SECRET = 'private-upstream-token'


def listing(kind, items=None, continuation=''):
    return {'apiVersion': 'v1', 'kind': kind + 'List', 'metadata': {'continue': continuation},
            'items': [] if items is None else items}


def discovery(group, resources):
    return {'kind': 'APIResourceList', 'groupVersion': group,
            'resources': [{'name': name, 'namespaced': scoped, 'kind': 'Synthetic',
                           'verbs': ['get', 'list', 'create', 'update', 'delete']} for name, scoped in resources]}


def group(name, versions=('v1beta1',)):
    return {'kind': 'APIGroup', 'name': name, 'versions': [
        {'groupVersion': name + '/' + v, 'version': v} for v in versions]}


def all_payloads():
    result = {'/api/v1/nodes?limit=1': listing('Node'), '/api/v1/pods?limit=1': listing('Pod')}
    result['/apis/fleet.davano.io/v1alpha1'] = discovery('fleet.davano.io/v1alpha1', [('gpupools', False), ('gpuaccesspolicies', True)])
    result['/apis/admissionregistration.k8s.io/v1'] = discovery('admissionregistration.k8s.io/v1', [('validatingadmissionpolicies', False), ('validatingadmissionpolicybindings', False)])
    result['/apis/batch/v1'] = discovery('batch/v1', [('jobs', True)])
    result['/apis/networking.k8s.io/v1'] = discovery('networking.k8s.io/v1', [('networkpolicies', True)])
    for name in ('kueue.x-k8s.io', 'visibility.kueue.x-k8s.io'):
        result['/apis/' + name] = group(name)
        resources = [('localqueues/pendingworkloads', True)] if name.startswith('visibility') else [('resourceflavors', False), ('clusterqueues', False), ('localqueues', True), ('workloads', True)]
        result['/apis/' + name + '/v1beta1'] = discovery(name + '/v1beta1', resources)
    result['/api/v1/query?query=vector%281%29&timeout=1s'] = {'status': 'success', 'data': {'resultType': 'vector', 'result': [{'metric': {}, 'value': [1, '1']}]}}
    return result


class Response(io.BytesIO):
    def __init__(self, value, headers=None, status=200):
        super().__init__(value if isinstance(value, bytes) else json.dumps(value).encode())
        self.headers, self.status = headers or {}, status


class Transport:
    def __init__(self, payloads=None):
        self.payloads = all_payloads() if payloads is None else payloads
        self.calls = []

    def __call__(self, request, *, context, timeout):
        parsed = urlparse(request.full_url)
        path = parsed.path + ('?' + parsed.query if parsed.query else '')
        self.calls.append((request, timeout, path))
        value = self.payloads[path]
        if isinstance(value, Exception):
            raise value
        return value if isinstance(value, Response) else Response(value)


class DiagnosticsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.token = Path(self.temp.name) / 'private-token-file'
        self.token.write_text(SECRET)
        self.env = {'DAVANO_K8S_API': 'https://private-cluster.example', 'DAVANO_K8S_TOKEN_PATH': str(self.token),
                    'DAVANO_K8S_CA': '/private/cluster-ca'}
        self.transport = Transport()
        self.tls = patch('davano.diagnostics.ssl.create_default_context', return_value=object())
        self.tls.start()
        self.addCleanup(self.tls.stop)

    def report(self, **kwargs):
        return DeploymentDiagnostics('kubernetes', env=self.env, opener=self.transport, **kwargs).report()

    def checks(self, report):
        return {c['id']: c for c in report['checks']}

    def enable_all(self):
        self.env.update({name: 'true' for name in ('DAVANO_CONTROL_ENABLED', 'DAVANO_EXECUTION_ENABLED',
            'DAVANO_QUEUE_OBSERVABILITY_ENABLED', 'DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED')})
        self.env.update(DAVANO_KUEUE_PENDING_LOCAL_QUEUES='secret-project/secret-queue',
                        DAVANO_PROMETHEUS_URL='https://private-prometheus.example',
                        DAVANO_PROMETHEUS_TOKEN_PATH=str(self.token), DAVANO_PROMETHEUS_CA='/private/prometheus-ca')

    def test_demo_never_reads_credentials_constructs_tls_or_opens_transport(self):
        self.enable_all()
        with patch('davano.diagnostics.credential', side_effect=AssertionError('credential read')), patch('davano.diagnostics.ssl.create_default_context', side_effect=AssertionError('TLS read')):
            report = DeploymentDiagnostics('demo', env=self.env, opener=lambda *_a, **_k: self.fail('external call')).report()
        self.assertEqual(report['bounds']['requests_attempted'], 0)
        self.assertNotIn('observed', {c['status'] for c in report['checks']})
        self.assertEqual(report['mode'], 'demo')
        self.assertEqual(report['configured_modes']['execution'], 'enabled')

    def test_minimum_inventory_only_no_pagination_no_payload_or_credential_leaks(self):
        self.transport.payloads['/api/v1/nodes?limit=1'] = listing('Node', [{'metadata': {'name': 'secret-node', 'annotations': {'private': SECRET}}}], 'secret-continuation')
        report = self.report()
        self.assertEqual([call[2] for call in self.transport.calls], ['/api/v1/nodes?limit=1', '/api/v1/pods?limit=1'])
        self.assertEqual(self.checks(report)['inventory-nodes']['counts'], {'returned_items': 1, 'has_more': True})
        for request, timeout, _ in self.transport.calls:
            self.assertEqual(request.get_method(), 'GET')
            self.assertIsNone(request.data)
            self.assertEqual(request.get_header('Authorization'), 'Bearer ' + SECRET)
            self.assertLessEqual(timeout, 2)
        raw = json.dumps(report)
        for secret in (SECRET, str(self.token), '/private/cluster-ca', 'private-cluster.example', 'secret-node', 'secret-continuation'):
            self.assertNotIn(secret, raw)
        self.assertFalse(report['bounds']['pagination_followed'])
        self.assertNotIn('ready', report['summary'])

    def test_all_configured_dependencies_are_bounded_and_only_observational(self):
        self.enable_all()
        report = self.report()
        checks = self.checks(report)
        self.assertEqual(checks['managed-resource-api']['status'], 'observed')
        self.assertEqual(checks['pending-visibility-api']['status'], 'observed')
        self.assertEqual(checks['prometheus-query']['status'], 'observed')
        self.assertEqual(len(self.transport.calls), 11)
        self.assertLessEqual(len(self.transport.calls), MAX_REQUESTS)
        self.assertTrue(all(c[0].get_method() == 'GET' for c in self.transport.calls))
        for identity in ('write-authorization', 'controller-enforcement', 'tenant-isolation', 'runtime-hardware'):
            self.assertEqual(checks[identity]['status'], 'unverified')
        self.assertNotIn('secret-project', json.dumps(report))
        self.assertNotIn('secret-queue', json.dumps(report))
        self.assertEqual(set(report['summary']['status_counts']), set(STATUSES))

    def test_missing_or_invalid_configuration_never_calls_upstream(self):
        for changes in ({'DAVANO_K8S_API': ''}, {'DAVANO_K8S_API': 'http://private.example'},
                        {'DAVANO_K8S_API': 'https://user:password@private.example'}, {'DAVANO_K8S_API': 'https://private.example/path'},
                        {'DAVANO_K8S_API': 'https://private.example:invalid'}, {'DAVANO_K8S_TOKEN_PATH': '/no-private-token'}):
            with self.subTest(changes=changes):
                env = {**self.env, **changes}
                report = DeploymentDiagnostics('kubernetes', env=env, opener=self.transport).report()
                self.assertEqual(self.checks(report)['kubernetes-configuration']['status'], 'blocked')
                self.assertEqual(report['bounds']['requests_attempted'], 0)
        self.assertEqual(self.transport.calls, [])

    def test_empty_oversized_and_control_character_tokens_fail_closed(self):
        for secret in ('', 'x' * (MAX_TOKEN_BYTES + 1), 'secret\nsecond', 'secret\rsecond', 'secret\x00'):
            self.token.write_text(secret)
            report = self.report()
            self.assertEqual(self.checks(report)['kubernetes-configuration']['status'], 'blocked')
            self.assertEqual(report['bounds']['requests_attempted'], 0)

    def test_credentials_rotate_between_reports(self):
        self.report()
        self.token.write_text('new-rotated-secret')
        self.report()
        self.assertEqual(self.transport.calls[-1][0].get_header('Authorization'), 'Bearer new-rotated-secret')

    def test_http_failures_discard_bodies_urls_messages_and_keep_other_checks(self):
        for status, expected in ((401, 'blocked'), (403, 'blocked'), (404, 'blocked'), (302, 'unavailable'), (500, 'unavailable')):
            with self.subTest(status=status):
                body = io.BytesIO(b'private-controller-message')
                self.transport.payloads['/api/v1/nodes?limit=1'] = HTTPError('https://secret-url', status, SECRET, {}, body)
                report = self.report()
                self.assertEqual(self.checks(report)['inventory-nodes']['status'], expected)
                self.assertEqual(self.checks(report)['inventory-pods']['status'], 'observed')
                self.assertTrue(body.closed)
                self.assertNotIn('private-controller-message', json.dumps(report))
                self.assertNotIn(SECRET, json.dumps(report))
                self.assertNotIn('secret-url', json.dumps(report))

    def test_malformed_list_responses_never_claim_observed(self):
        valid = listing('Node')
        cases = [[], {}, {**valid, 'kind': 'PodList'}, {**valid, 'items': {}}, {**valid, 'metadata': None},
                 {**valid, 'metadata': {'continue': {}}}, {**valid, 'items': [None]},
                 {**valid, 'items': [{'metadata': {'name': SECRET}, 'status': []}]},
                 {**valid, 'items': [{'metadata': {'name': 'a'}}, {'metadata': {'name': 'b'}}]},
                 {**valid, 'warnings': [SECRET]}, b'not json', b'{"a":NaN}', b'{"kind":"NodeList","apiVersion":"v1","metadata":{},"items":[],"ignored":1e999}', b'{"kind":"NodeList","kind":"NodeList","apiVersion":"v1","metadata":{},"items":[]}']
        for value in cases:
            with self.subTest(value=value):
                self.transport.payloads['/api/v1/nodes?limit=1'] = value
                report = self.report()
                self.assertEqual(self.checks(report)['inventory-nodes']['status'], 'unavailable')
                self.assertNotIn(SECRET, json.dumps(report))

    def test_malformed_or_missing_discovery_never_claims_supported(self):
        self.env['DAVANO_CONTROL_ENABLED'] = 'true'
        path = '/apis/fleet.davano.io/v1alpha1'
        valid = deepcopy(self.transport.payloads[path])
        cases = [{}, {**valid, 'groupVersion': 'other/v1'}, {**valid, 'resources': None},
                 {**valid, 'resources': [None]}, {**valid, 'resources': []}]
        for field, value in (('name', 'secret <script>'), ('namespaced', 'false'), ('verbs', 'get'), ('verbs', []), ('kind', {})):
            item = deepcopy(valid)
            item['resources'][0][field] = value
            cases.append(item)
        duplicate = deepcopy(valid)
        duplicate['resources'].append(deepcopy(duplicate['resources'][0]))
        cases.append(duplicate)
        for value in cases:
            with self.subTest(value=value):
                self.transport.payloads[path] = value
                self.assertIn(self.checks(self.report())['managed-resource-api']['status'], ('blocked', 'unavailable'))

    def test_optional_disabled_features_do_not_read_optional_files_or_endpoints(self):
        self.env.update(DAVANO_PROMETHEUS_TOKEN_PATH='/private/not-read', DAVANO_PROMETHEUS_CA='/private/not-read',
                        DAVANO_KUEUE_PENDING_LOCAL_QUEUES='bad target')
        report = self.report()
        self.assertEqual(report['bounds']['requests_attempted'], 2)
        self.assertNotIn('pending-visibility-configuration', self.checks(report))

    def test_pending_visibility_requires_queue_enablement_and_valid_server_targets(self):
        self.env['DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED'] = 'true'
        self.env['DAVANO_KUEUE_PENDING_LOCAL_QUEUES'] = 'secret-project/secret-queue'
        self.assertEqual(self.checks(self.report())['pending-visibility-configuration']['status'], 'blocked')
        self.env['DAVANO_QUEUE_OBSERVABILITY_ENABLED'] = 'true'
        for target in ('', '../bad', 'a/b,a/b', ','.join('a/b' + str(n) for n in range(11))):
            self.env['DAVANO_KUEUE_PENDING_LOCAL_QUEUES'] = target
            report = self.report()
            self.assertEqual(self.checks(report)['pending-visibility-configuration']['status'], 'blocked')
            self.assertNotIn('pending-visibility-api', self.checks(report))

    def test_prometheus_constant_query_is_not_metrics_and_malformed_result_fails(self):
        self.env['DAVANO_PROMETHEUS_URL'] = 'https://private-prometheus.example'
        report = self.report()
        self.assertEqual(self.checks(report)['prometheus-query']['evidence'], 'read-query')
        request = self.transport.calls[-1][0]
        self.assertIsNone(request.get_header('Authorization'))
        self.assertNotIn('DCGM', request.full_url)
        path = self.transport.calls[-1][2]
        for payload in ({'status': 'error', 'error': SECRET}, {'status': 'success', 'data': None},
                        {'status': 'success', 'data': {'resultType': 'vector', 'result': []}},
                        {'status': 'success', 'data': {'resultType': 'vector', 'result': [{'metric': {SECRET: SECRET}, 'value': [1, '1']}]}}):
            self.transport.payloads[path] = payload
            report = self.report()
            self.assertEqual(self.checks(report)['prometheus-query']['status'], 'unavailable')
            self.assertNotIn(SECRET, json.dumps(report))

    def test_discovery_only_does_not_claim_resource_authorization(self):
        self.env['DAVANO_CONTROL_ENABLED'] = 'true'
        self.transport.payloads['/api/v1/nodes?limit=1'] = HTTPError('secret', 403, SECRET, {}, io.BytesIO())
        report = self.report()
        checks = self.checks(report)
        self.assertEqual(checks['inventory-nodes']['status'], 'blocked')
        self.assertEqual(checks['managed-resource-api']['status'], 'observed')
        self.assertIn('does not establish', checks['managed-resource-api']['detail'])
        self.assertEqual(checks['write-authorization']['status'], 'unverified')

    def test_response_byte_and_warning_limits(self):
        for response in (Response(b'x' * (MAX_RESPONSE_BYTES + 1)), Response(listing('Node'), headers={'Warning': SECRET}), Response(listing('Node'), status=204)):
            self.transport.payloads['/api/v1/nodes?limit=1'] = response
            report = self.report()
            self.assertEqual(self.checks(report)['inventory-nodes']['status'], 'unavailable')
            self.assertNotIn(SECRET, json.dumps(report))
            self.assertTrue(response.closed)

    def test_request_limit_is_enforced_without_starting_extra_call(self):
        reads = BoundedReads(opener=self.transport)
        for _ in range(MAX_REQUESTS):
            reads.get('https://example.test', '/api/v1/nodes?limit=1', None, object())
        with self.assertRaises(ProbeFailure):
            reads.get('https://example.test', '/api/v1/nodes?limit=1', None, object())
        self.assertEqual(len(self.transport.calls), MAX_REQUESTS)
        self.assertTrue(reads.budget_exhausted)

    def test_budget_expiry_discards_late_result_and_starts_no_later_reads(self):
        clock = [0.0]
        def slow(request, **kwargs):
            response = self.transport(request, **kwargs)
            clock[0] = REPORT_BUDGET_SECONDS + 1
            return response
        report = DeploymentDiagnostics('kubernetes', env=self.env, opener=slow, clock=lambda: clock[0]).report()
        self.assertEqual(len(self.transport.calls), 1)
        self.assertTrue(report['bounds']['budget_exhausted'])
        self.assertEqual(self.checks(report)['inventory-nodes']['status'], 'unavailable')
        self.assertEqual(self.checks(report)['inventory-pods']['status'], 'unavailable')

    def test_inventory_explicit_connection_overrides_environment(self):
        inventory = KubernetesInventory(api='https://explicit.example', token_path=str(self.token), ca_path='/explicit-ca', opener=self.transport)
        report = DeploymentDiagnostics('kubernetes', env={'DAVANO_K8S_API': 'https://wrong.example'}, inventory=inventory).report()
        self.assertEqual(report['bounds']['requests_attempted'], 2)
        self.assertTrue(all(call[0].full_url.startswith('https://explicit.example/') for call in self.transport.calls))

    def test_largest_configured_request_plan_fits_exact_budget(self):
        self.enable_all()
        for name in ('kueue.x-k8s.io', 'visibility.kueue.x-k8s.io'):
            self.transport.payloads['/apis/' + name] = group(name, ('v1beta2', 'v1beta1'))
            newer = deepcopy(self.transport.payloads['/apis/' + name + '/v1beta1'])
            newer['groupVersion'] = name + '/v1beta2'
            self.transport.payloads['/apis/' + name + '/v1beta2'] = newer
        report = self.report()
        self.assertEqual(report['bounds']['requests_attempted'], MAX_REQUESTS)
        self.assertEqual(self.checks(report)['prometheus-query']['status'], 'observed')
        self.assertFalse(report['bounds']['budget_exhausted'])

    def test_malformed_dependency_versions_cannot_supply_a_path(self):
        self.env['DAVANO_QUEUE_OBSERVABILITY_ENABLED'] = 'true'
        path = '/apis/kueue.x-k8s.io'
        for payload in ({}, group('other'), group('kueue.x-k8s.io', ('../../private',)),
                        group('kueue.x-k8s.io', ('v1beta1', 'v1beta1')), group('kueue.x-k8s.io', ('v9',)),
                        {**group('kueue.x-k8s.io'), 'versions': [None]}):
            self.transport.calls.clear()
            self.transport.payloads[path] = payload
            report = self.report()
            self.assertIn(self.checks(report)['queue-observability-api']['status'], ('blocked', 'unavailable'))
            self.assertEqual(len(self.transport.calls), 3)
            self.assertNotIn('../../private', json.dumps(report))

    def test_expired_budget_during_stream_closes_response_and_discards_evidence(self):
        clock = [0.0]
        class SlowStream(Response):
            def read1(self, size):
                result = super().read1(size)
                clock[0] += REPORT_BUDGET_SECONDS + 1
                return result
        response = SlowStream(listing('Node'))
        self.transport.payloads['/api/v1/nodes?limit=1'] = response
        report = self.report(clock=lambda: clock[0])
        self.assertTrue(response.closed)
        self.assertEqual(report['bounds']['requests_attempted'], 1)
        self.assertTrue(report['bounds']['budget_exhausted'])
        self.assertEqual(self.checks(report)['inventory-nodes']['status'], 'unavailable')

    def test_ui_contract_is_fixed_bounded_and_has_no_aggregate_readiness(self):
        self.enable_all()
        report = self.report()
        self.assertEqual(len({c['id'] for c in report['checks']}), len(report['checks']))
        for item in report['checks']:
            self.assertIn(item['status'], STATUSES)
            self.assertIn(item['evidence'], ('configuration', 'list-read', 'api-discovery', 'read-query', 'not-checked'))
            for key in ('detail', 'next_step'):
                self.assertTrue(0 < len(item[key]) <= 700)
        self.assertEqual(sum(report['summary']['status_counts'].values()), len(report['checks']))


class FailedInventory:
    source = 'kubernetes'
    def snapshot(self):
        raise InventoryUnavailable('Inventory unavailable')


class DiagnosticsAPITests(unittest.TestCase):
    def app(self, source='kubernetes', **env):
        with patch.dict(os.environ, env, clear=True), patch('davano.workload_api.install_workload_api'):
            return create_app(source=source, db_path=':memory:', inventory=FailedInventory() if source == 'kubernetes' else None)

    def test_live_auth_is_required_with_execution_disabled(self):
        app = self.app(DAVANO_API_TOKEN=ADMIN)
        with TestClient(app) as client, patch.object(app.state.diagnostics, 'report', return_value={'safe': True}) as report:
            for headers in ({}, {'Authorization': 'Bearer project-token-' + 'p' * 32}, {'Authorization': 'Basic ' + ADMIN}, {'Authorization': 'Bearer short'}):
                response = client.get('/api/v1/diagnostics', headers=headers)
                self.assertEqual(response.status_code, 401)
                self.assertEqual(response.headers['WWW-Authenticate'], 'Bearer')
            report.assert_not_called()
            self.assertEqual(client.get('/api/v1/diagnostics', headers={'Authorization': 'Bearer ' + ADMIN}).json(), {'safe': True})
            report.assert_called_once()

    def test_diagnostics_independent_admin_auth_with_execution_enabled(self):
        app = self.app(DAVANO_API_TOKEN=ADMIN, DAVANO_EXECUTION_ENABLED='true')
        with TestClient(app) as client, patch.object(app.state.diagnostics, 'report', return_value={'safe': True}):
            self.assertEqual(client.get('/api/v1/state').status_code, 403)
            self.assertEqual(client.get('/api/v1/diagnostics').status_code, 401)
            self.assertEqual(client.get('/api/v1/diagnostics', headers={'Authorization': 'Bearer ' + ADMIN}).status_code, 200)

    def test_missing_short_or_excessive_admin_config_fails_before_report(self):
        for token in ('', 'x' * 31, 'x' * 4097):
            app = self.app(DAVANO_API_TOKEN=token)
            with TestClient(app) as client, patch.object(app.state.diagnostics, 'report') as report:
                response = client.get('/api/v1/diagnostics', headers={'Authorization': 'Bearer ' + ADMIN})
                self.assertEqual(response.status_code, 503)
                report.assert_not_called()

    def test_runtime_inventory_failure_does_not_block_diagnostics_or_change_health(self):
        app = self.app(DAVANO_API_TOKEN=ADMIN)
        with TestClient(app) as client:
            self.assertEqual(client.get('/healthz').status_code, 200)
            self.assertEqual(client.get('/readyz').status_code, 503)
            self.assertEqual(client.get('/api/v1/state').status_code, 503)
            response = client.get('/api/v1/diagnostics', headers={'Authorization': 'Bearer ' + ADMIN})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['source'], 'kubernetes')
            self.assertEqual(response.json()['bounds']['requests_attempted'], 0)
            self.assertEqual(response.headers['Cache-Control'], 'no-store')

    def test_invalid_origin_still_fails_startup(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(InventoryUnavailable):
                create_app(source='kubernetes', db_path=':memory:')

    def test_no_browser_targets_or_mutation_methods(self):
        app = self.app(DAVANO_API_TOKEN=ADMIN)
        headers = {'Authorization': 'Bearer ' + ADMIN}
        with TestClient(app) as client, patch.object(app.state.diagnostics, 'report') as report:
            for query in ('?url=https://untrusted.test', '?namespace=private', '?limit=9999', '?refresh=true'):
                response = client.get('/api/v1/diagnostics' + query, headers=headers)
                self.assertEqual(response.status_code, 400)
                self.assertNotIn('untrusted.test', response.text)
            self.assertEqual(client.post('/api/v1/diagnostics', json={}, headers=headers).status_code, 405)
            report.assert_not_called()

    def test_demo_endpoint_public_and_zero_external_calls(self):
        with TestClient(self.app(source='demo')) as client, patch('davano.diagnostics.open_without_redirect', side_effect=AssertionError('network call')):
            response = client.get('/api/v1/diagnostics')
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()['bounds']['requests_attempted'], 0)
            self.assertEqual(response.json()['source'], 'demo')


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL CLI required for ephemeral loopback TLS')
class DiagnosticsHTTPSTests(unittest.TestCase):
    """Real local HTTPS only, not a Kubernetes or production validation."""
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.ca, key = root / 'tls.crt', root / 'tls.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(cls.ca), '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.token = root / 'token'
        cls.token.write_text(SECRET)
        cls.mode, cls.requests = 'success', []
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.requests.append((self.path, self.headers.get('Authorization')))
                if cls.mode == 'redirect':
                    self.send_response(302)
                    self.send_header('Location', f'https://localhost:{cls.server.server_port}/private-target')
                    self.end_headers()
                    return
                if cls.mode == 'forbidden':
                    body = b'private-controller-message'
                    self.send_response(403)
                else:
                    body = json.dumps(listing('Node' if self.path.startswith('/api/v1/nodes') else 'Pod')).encode()
                    self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            def log_message(self, *_):
                pass
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cls.ca, key)
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)
        cls.temp.cleanup()

    def setUp(self):
        type(self).mode = 'success'
        type(self).requests.clear()
        self.env = {'DAVANO_K8S_API': f'https://localhost:{self.server.server_port}',
                    'DAVANO_K8S_TOKEN_PATH': str(self.token), 'DAVANO_K8S_CA': str(self.ca)}

    def report(self):
        return DeploymentDiagnostics('kubernetes', env=self.env).report()

    def test_trusted_ca_authenticated_bounded_gets(self):
        report = self.report()
        self.assertEqual(report['bounds']['requests_attempted'], 2)
        self.assertEqual(self.requests, [('/api/v1/nodes?limit=1', 'Bearer ' + SECRET), ('/api/v1/pods?limit=1', 'Bearer ' + SECRET)])
        self.assertTrue(all(c['status'] == 'observed' for c in report['checks'] if c['id'].startswith('inventory-')))

    def test_redirect_does_not_forward_credentials_or_follow_target(self):
        type(self).mode = 'redirect'
        report = self.report()
        self.assertEqual(len(self.requests), 2)
        self.assertTrue(all('private-target' not in p for p, _ in self.requests))
        self.assertNotIn('private-target', json.dumps(report))
        self.assertTrue(all(c['status'] == 'unavailable' for c in report['checks'] if c['id'].startswith('inventory-')))

    def test_untrusted_ca_rejects_before_authenticated_request(self):
        self.env['DAVANO_K8S_CA'] = None
        report = self.report()
        self.assertEqual(self.requests, [])
        self.assertTrue(all(c['status'] == 'unavailable' for c in report['checks'] if c['id'].startswith('inventory-')))

    def test_upstream_error_body_never_reaches_report(self):
        type(self).mode = 'forbidden'
        report = self.report()
        self.assertNotIn('private-controller-message', json.dumps(report))
        self.assertNotIn(SECRET, json.dumps(report))
        self.assertTrue(all(c['status'] == 'blocked' for c in report['checks'] if c['id'].startswith('inventory-')))


if __name__ == '__main__':
    unittest.main()
