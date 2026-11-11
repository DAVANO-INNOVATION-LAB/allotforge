#!/usr/bin/env python3
"""Exercise diagnostic parsers against the disposable CI API server.

Restricted to kind-davano-ci. Fixed GETs run through kubectl using CI's existing
identity. This does not validate the app's HTTPS transport, administrator HTTP
authentication, production RBAC, Kueue, admission enforcement, or hardware.
"""
import base64
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.error import HTTPError
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from davano.diagnostics import DeploymentDiagnostics


CONTEXT = 'kind-davano-ci'
ALLOWED_PATHS = frozenset({
    '/api/v1/nodes?limit=1',
    '/api/v1/pods?limit=1',
    '/apis/fleet.davano.io/v1alpha1',
    '/apis/admissionregistration.k8s.io/v1',
    '/apis/kueue.x-k8s.io/v1beta1',
})


def kubectl(*args, timeout=10):
    return subprocess.run(['kubectl', '--context', CONTEXT, *args],
                          capture_output=True, text=True, timeout=timeout)


class RawResponse(io.BytesIO):
    status = 200
    headers = {}


def raw_opener(request, *, context, timeout):
    parsed = urlsplit(request.full_url)
    path = parsed.path + ('?' + parsed.query if parsed.query else '')
    if (request.get_method() != 'GET' or parsed.scheme != 'https'
            or parsed.netloc != 'diagnostics-ci.invalid' or path not in ALLOWED_PATHS):
        raise AssertionError('Diagnostic smoke attempted a non-allowlisted GET')
    # No Authorization header or dummy token is forwarded to kubectl.
    result = kubectl('get', '--raw', path, '--request-timeout=2s', timeout=3)
    if result.returncode:
        if '(NotFound)' in result.stderr or 'could not find the requested resource' in result.stderr:
            raise HTTPError(request.full_url, 404, 'NotFound', {}, io.BytesIO())
        raise RuntimeError('The fixed CI API read failed; source details withheld')
    return RawResponse(result.stdout.encode('utf-8'))


def main():
    if os.environ.get('DAVANO_TEST_CONTEXT') != CONTEXT:
        raise SystemExit('Refusing API reads: DAVANO_TEST_CONTEXT must be kind-davano-ci')
    # The selected CI context must point at the expected disposable cluster.
    result = kubectl('config', 'view', '--minify', '-o', 'json')
    if result.returncode:
        raise SystemExit('The fixed CI context could not be inspected')
    config = json.loads(result.stdout)
    contexts = config.get('contexts', [])
    if (len(contexts) != 1 or contexts[0].get('name') != CONTEXT
            or contexts[0].get('context', {}).get('cluster') != CONTEXT):
        raise SystemExit('Refusing reads outside the expected disposable kind cluster')
    # Read only public CA material, never kubeconfig client credentials.
    ca = kubectl('config', 'view', '--raw', '--minify', '--flatten',
                 '-o', 'jsonpath={.clusters[0].cluster.certificate-authority-data}')
    if ca.returncode:
        raise SystemExit('Could not read the disposable cluster public CA')
    with tempfile.TemporaryDirectory(prefix='allotforge-diagnostic-smoke-') as directory:
        root = Path(directory)
        (root / 'ca.crt').write_bytes(base64.b64decode(ca.stdout, validate=True))
        (root / 'dummy-token').write_text('synthetic-ci-credential-not-sent')
        report = DeploymentDiagnostics('kubernetes', env={
            'DAVANO_K8S_API': 'https://diagnostics-ci.invalid',
            'DAVANO_K8S_CA': str(root / 'ca.crt'),
            'DAVANO_K8S_TOKEN_PATH': str(root / 'dummy-token'),
            'DAVANO_CONTROL_ENABLED': 'true',
        }, opener=raw_opener).report()
    checks = {item['id']: item for item in report['checks']}
    for identity in ('inventory-nodes', 'inventory-pods', 'managed-resource-api', 'admission-api'):
        assert checks[identity]['status'] == 'observed', (identity, checks[identity])
    assert checks['managed-kueue-api']['status'] == 'blocked', checks['managed-kueue-api']
    for identity in ('write-authorization', 'controller-enforcement', 'tenant-isolation', 'runtime-hardware'):
        assert checks[identity]['status'] == 'unverified', checks[identity]
    assert report['bounds']['requests_attempted'] == len(ALLOWED_PATHS), report['bounds']
    assert report['bounds']['pagination_followed'] is False
    assert 'ready' not in report and 'ready' not in report['summary']
    print('Real API diagnostic parser smoke passed: bounded Node/Pod lists, native/managed discovery, and absent Kueue.')
    print('Scope: CI identity via fixed kubectl GETs; not app authentication, transport, target RBAC, enforcement, or GPU validation.')


if __name__ == '__main__':
    main()
