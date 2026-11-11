"""Real loopback TLS transport tests against a synthetic Prometheus HTTP server.

These verify local protocol behavior only, never real GPU measurements.
"""
import json
import shutil
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from davano.observability import ObservationUnavailable, PrometheusReader, normalize_metrics


@unittest.skipUnless(shutil.which('openssl'), 'OpenSSL CLI needed to create an ephemeral loopback test certificate')
class LoopbackPrometheusTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        root = Path(cls.temp.name)
        cls.ca, key = root / 'tls.crt', root / 'tls.key'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-keyout', str(key), '-out', str(cls.ca), '-days', '1',
                        '-subj', '/CN=localhost', '-addext', 'subjectAltName=DNS:localhost'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        cls.secret = root / 'token'
        cls.secret.write_text('synthetic-loopback-test-token')
        cls.requests = []
        cls.mode = 'success'
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                cls.requests.append({'path': self.path, 'authorization': self.headers.get('Authorization')})
                if cls.mode == 'redirect':
                    self.send_response(302)
                    self.send_header('Location', f'https://localhost:{cls.server.server_port}/redirected')
                    self.end_headers()
                    return
                if cls.mode == 'forbidden':
                    self.send_response(403)
                    self.end_headers()
                    self.wfile.write(b'synthetic-server-secret-should-never-be-echoed')
                    return
                timestamp = time.time() - (300 if cls.mode == 'stale' else 1)
                body = json.dumps({'status': 'success', 'data': {'resultType': 'matrix', 'result': [{
                    'metric': {'__name__': 'DCGM_FI_DEV_GPU_UTIL_RATIO', 'UUID': 'GPU-12345678-1234-1234-1234-123456789abc', 'hostname': 'gpu-node'},
                    'values': [[timestamp, '0.61']]}]}}).encode()
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
        self.reader = PrometheusReader(f'https://localhost:{self.server.server_port}', token_path=str(self.secret), ca_path=str(self.ca))

    def test_validated_https_get_and_source_timestamp(self):
        devices, _ = normalize_metrics(self.reader.samples(), [{'name': 'gpu-node', 'labels': {}}])
        self.assertEqual(devices[0]['metrics']['utilization_percent']['value'], 61)
        self.assertEqual(devices[0]['metrics']['utilization_percent']['status'], 'fresh')
        self.assertEqual(self.requests[0]['authorization'], 'Bearer synthetic-loopback-test-token')
        self.assertTrue(parse_qs(urlparse(self.requests[0]['path']).query)['query'][0].endswith('[15m]'))

    def test_stale_source_stays_stale_despite_successful_live_http(self):
        type(self).mode = 'stale'
        devices, _ = normalize_metrics(self.reader.samples(), [])
        self.assertIsNone(devices[0]['metrics']['utilization_percent']['value'])
        self.assertEqual(devices[0]['metrics']['utilization_percent']['status'], 'stale')

    def test_redirect_is_not_followed_or_forwarded_credentials(self):
        type(self).mode = 'redirect'
        with self.assertRaisesRegex(ObservationUnavailable, 'HTTP 302'):
            self.reader.samples()
        self.assertEqual(len(self.requests), 1)
        self.assertNotIn('/redirected', self.requests[0]['path'])

    def test_untrusted_ca_fails_before_authenticated_request(self):
        reader = PrometheusReader(f'https://localhost:{self.server.server_port}', token_path=str(self.secret))
        with self.assertRaises(ObservationUnavailable):
            reader.samples()
        self.assertEqual(self.requests, [])

    def test_error_response_body_never_reaches_consumer(self):
        type(self).mode = 'forbidden'
        with self.assertRaises(ObservationUnavailable) as error:
            self.reader.samples()
        self.assertEqual(str(error.exception), 'Prometheus read failed (HTTP 403).')


if __name__ == '__main__':
    unittest.main()
