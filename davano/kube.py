"""Small HTTPS Kubernetes client. No kubeconfig execution, redirects, or token caching."""
import json
import os
import ssl
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request
from .inventory import open_without_redirect
from .quantities import normalize_quota_hard

GROUP = 'fleet.davano.io'
VERSION = 'v1alpha1'
ROOT = f'/apis/{GROUP}/{VERSION}'
OWNER_LABEL = 'fleet.davano.io/owner-uid'
MANAGER_LABEL = 'app.kubernetes.io/managed-by'

class ApiError(RuntimeError):
    def __init__(self, status, message):
        self.status = status
        super().__init__(message)

class OwnershipConflict(ApiError):
    def __init__(self, message):
        super().__init__(409, message)

def segment(value):
    return quote(str(value), safe='')

class KubeClient:
    def __init__(self, api=None, token_path=None, ca_path=None, opener=None):
        host = os.getenv('KUBERNETES_SERVICE_HOST', '')
        if ':' in host and not host.startswith('['):
            host = f'[{host}]'
        self.api = (api or os.getenv('DAVANO_K8S_API') or (f'https://{host}:{os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")}' if host else '')).rstrip('/')
        p = urlparse(self.api)
        if p.scheme != 'https' or not p.hostname or p.username or p.password or p.path or p.query or p.fragment:
            raise ApiError(503, 'An HTTPS Kubernetes API origin is required')
        self.token_path = token_path or os.getenv('DAVANO_K8S_TOKEN_PATH', '/var/run/secrets/kubernetes.io/serviceaccount/token')
        self.ca_path = ca_path or os.getenv('DAVANO_K8S_CA', '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt')
        self.opener = opener or open_without_redirect

    def request(self, method, path, body=None):
        if not path.startswith('/') or path.startswith('//') or '..' in path.split('/'):
            raise ValueError('Invalid Kubernetes API path')
        try:
            token = Path(self.token_path).read_text().strip()
            if not token:
                raise ValueError('Empty service account token')
            context = ssl.create_default_context(cafile=self.ca_path)
            raw = None if body is None else json.dumps(body, allow_nan=False).encode()
            req = Request(self.api + path, data=raw, method=method, headers={'Authorization': f'Bearer {token}', 'Accept': 'application/json', 'Content-Type': 'application/json'})
            with self.opener(req, context=context, timeout=10) as response:
                payload = response.read(8 * 1024 * 1024 + 1)
            if len(payload) > 8 * 1024 * 1024:
                raise ApiError(502, 'Kubernetes response exceeded size limit')
            data = json.loads(payload) if payload else {}
            if not isinstance(data, dict):
                raise ApiError(502, 'Unexpected Kubernetes response type')
            return data
        except HTTPError as error:
            # Server response bodies can contain credentials or submitted data.
            raise ApiError(error.code, f'Kubernetes API returned HTTP {error.code}') from None
        except (OSError, URLError, ValueError) as error:
            raise ApiError(503, 'Kubernetes HTTPS transport unavailable') from None

    def get(self, path):
        return self.request('GET', path)

    def list(self, path):
        items, continuation = [], ''
        for _ in range(100):
            query = {'limit': 500}
            if continuation:
                query['continue'] = continuation
            page = self.get(path + '?' + urlencode(query))
            if not isinstance(page.get('items'), list):
                raise ApiError(502, 'Invalid Kubernetes list response')
            items.extend(page['items'])
            continuation = page.get('metadata', {}).get('continue', '')
            if not continuation:
                return items
        raise ApiError(502, 'Incomplete Kubernetes list; pagination safety limit exceeded')

    def apply_owned(self, collection, desired, owner):
        """Create or CAS-replace only our own UID-labeled resource; never adopt."""
        desired = json.loads(json.dumps(desired))
        metadata = desired.setdefault('metadata', {})
        metadata.setdefault('labels', {}).update({OWNER_LABEL: owner['metadata']['uid'], MANAGER_LABEL: 'davano-controller'})
        path = collection + '/' + segment(metadata['name'])
        try:
            current = self.get(path)
        except ApiError as error:
            if error.status != 404:
                raise
            return self.request('POST', collection, desired)
        if current['metadata'].get('labels', {}).get(OWNER_LABEL) != owner['metadata']['uid'] or current['metadata'].get('labels', {}).get(MANAGER_LABEL) != 'davano-controller':
            raise OwnershipConflict(f'Refusing to modify unowned {desired["kind"]} {metadata["name"]}')
        for key in ('uid', 'resourceVersion', 'creationTimestamp', 'finalizers', 'ownerReferences'):
            if key in current['metadata']:
                metadata[key] = current['metadata'][key]
        # Preserve unrelated metadata and status while replacing owned spec.
        metadata['annotations'] = {**current['metadata'].get('annotations', {}), **metadata.get('annotations', {})}
        metadata['labels'] = {**current['metadata'].get('labels', {}), **metadata['labels']}
        if 'status' in current:
            desired['status'] = current['status']
        def comparable(obj):
            value = json.loads(json.dumps({k: v for k, v in obj.items() if k not in ('metadata', 'status')}))
            if value.get('kind') == 'ResourceQuota' and 'hard' in value.get('spec', {}):
                value['spec']['hard'] = normalize_quota_hard(value['spec']['hard'])
            return value
        if comparable(current) == comparable(desired) and all(current['metadata'].get('labels', {}).get(k) == v for k, v in metadata['labels'].items()):
            return current
        return self.request('PUT', path, desired)

    def delete_owned(self, collection, name, owner):
        path = collection + '/' + segment(name)
        try:
            current = self.get(path)
        except ApiError as error:
            if error.status == 404:
                return
            raise
        labels = current['metadata'].get('labels', {})
        if labels.get(OWNER_LABEL) != owner['metadata']['uid'] or labels.get(MANAGER_LABEL) != 'davano-controller':
            raise OwnershipConflict(f'Refusing to delete unowned resource {name}')
        return self.request('DELETE', path, {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': {'uid': current['metadata']['uid'], 'resourceVersion': current['metadata']['resourceVersion']}, 'propagationPolicy': 'Foreground'})
