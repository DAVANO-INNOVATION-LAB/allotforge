"""Polling controller with Kubernetes Lease leadership and health probes."""
import json
import logging
import os
import signal
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from .kube import KubeClient, ApiError, segment
from .reconciler import Reconciler

LOG = logging.getLogger('davano.operator')

class Lease:
    def __init__(self, client, namespace, name='davano-controller', duration=30, clock=time.monotonic):
        self.client, self.clock, self.duration = client, clock, duration
        self.collection = f'/apis/coordination.k8s.io/v1/namespaces/{segment(namespace)}/leases'
        self.path = self.collection + '/' + segment(name)
        self.name = name
        self.identity = socket.gethostname() + '-' + uuid.uuid4().hex[:8]
        self.deadline = 0
        self.observed_record = None
        self.observed_at = 0

    @property
    def held(self):
        return self.clock() < self.deadline

    def renew(self):
        now = datetime.now(timezone.utc)
        timestamp = now.isoformat(timespec='microseconds').replace('+00:00', 'Z')
        try:
            current = self.client.get(self.path)
        except ApiError as error:
            if error.status != 404:
                self.deadline = 0
                return False
            current = {'apiVersion': 'coordination.k8s.io/v1', 'kind': 'Lease', 'metadata': {'name': self.name}, 'spec': {}}
        spec = current.get('spec', {})
        if spec.get('holderIdentity') not in (None, '', self.identity):
            record = (spec.get('holderIdentity'), spec.get('renewTime'), spec.get('leaseDurationSeconds'))
            # Compare locally observed monotonic time, not another machine's
            # wall clock. Clock skew must not create two active leaders.
            if record != self.observed_record:
                self.observed_record, self.observed_at = record, self.clock()
            duration = spec.get('leaseDurationSeconds')
            if not isinstance(duration, int) or duration <= 0 or not spec.get('renewTime') or self.clock() - self.observed_at <= duration:
                self.deadline = 0
                return False
        new = {**current, 'spec': {**spec, 'holderIdentity': self.identity, 'leaseDurationSeconds': self.duration, 'renewTime': timestamp, 'acquireTime': spec.get('acquireTime', timestamp), 'leaseTransitions': spec.get('leaseTransitions', 0) + (spec.get('holderIdentity') != self.identity)}}
        started = self.clock()
        try:
            self.client.request('PUT' if current['metadata'].get('resourceVersion') else 'POST', self.path if current['metadata'].get('resourceVersion') else self.collection, new)
            self.deadline = started + self.duration * .6  # Stop well before expiry.
            return self.held
        except ApiError:
            self.deadline = 0
            return False

class LeaderClient:
    def __init__(self, client, lease):
        self.client, self.lease = client, lease
    def request(self, method, path, body=None):
        if method not in ('GET', 'HEAD') and not self.lease.held:
            raise ApiError(503, 'Controller leadership expired; refusing mutation')
        return self.client.request(method, path, body)
    get = KubeClient.get
    list = KubeClient.list
    apply_owned = KubeClient.apply_owned
    delete_owned = KubeClient.delete_owned


def main():
    logging.basicConfig(level=os.getenv('LOG_LEVEL', 'INFO'), format='%(asctime)s %(levelname)s %(message)s')
    namespaces = {v.strip() for v in os.getenv('DAVANO_MANAGED_NAMESPACES', '').split(',') if v.strip()}
    if not namespaces or any(ns in ('default', 'davano') or ns.startswith(('kube-', 'openshift')) for ns in namespaces):
        raise SystemExit('Set DAVANO_MANAGED_NAMESPACES to dedicated tenant namespaces before starting')
    client = KubeClient()
    namespace = os.getenv('POD_NAMESPACE', 'davano')
    lease = Lease(client, namespace)
    stop = threading.Event()
    state = {'last_success': 0., 'last_attempt': 0., 'reconcile_errors': 0}
    def terminate(*_):
        stop.set()
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    def heartbeat():
        while not stop.is_set():
            lease.renew()
            stop.wait(5)
    threading.Thread(target=heartbeat, daemon=True).start()
    class Health(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                body = (f'davano_controller_leader {int(lease.held)}\ndavano_controller_last_success_timestamp_seconds {state["last_success"]}\ndavano_controller_reconcile_errors_total {state["reconcile_errors"]}\n').encode()
                code, content_type = 200, 'text/plain; version=0.0.4'
            elif self.path in ('/healthz', '/readyz'):
                good = self.path == '/healthz' or (lease.held and time.time() - state['last_success'] < 120)
                body, code, content_type = json.dumps({'ready': good, 'leader': lease.held}).encode(), 200 if good else 503, 'application/json'
            else:
                body, code, content_type = b'Not found', 404, 'text/plain'
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def log_message(self, *_):
            pass
    server = ThreadingHTTPServer(('0.0.0.0', int(os.getenv('OPERATOR_PORT', '8081'))), Health)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    reconciler = Reconciler(LeaderClient(client, lease), namespaces)
    backoff = 2
    while not stop.is_set():
        if not lease.held:
            stop.wait(2)
            continue
        state['last_attempt'] = time.time()
        try:
            results = reconciler.cycle()
            state['last_success'] = time.time()
            failures = sum(reason in ('OwnershipConflict', 'ReconcileFailed', 'LegacyGrantRevocationPending') for _, reason in results)
            state['reconcile_errors'] += failures
            LOG.info('Reconciliation complete: resources=%d failed=%d', len(results), failures)
            backoff = 2
            stop.wait(10)
        except ApiError as error:
            state['reconcile_errors'] += 1
            LOG.warning('Reconciliation read failed: HTTP %s; preserving existing guards', error.status)
            stop.wait(backoff)
            backoff = min(60, backoff * 2)
    server.shutdown()

if __name__ == '__main__':
    main()
