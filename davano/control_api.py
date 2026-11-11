"""Explicitly enabled, authenticated API for persisted Kubernetes desired state."""
import hmac
import os
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from .allocation_models import AllocationUpdate
from .allocation_service import AllocationService
from .control_models import ManagedPoolCreate, ManagedPolicyCreate, DeletePreconditions
from .kube import KubeClient, ApiError, ROOT, segment

LIMITATIONS = [
    'Requires Kubernetes 1.30+ (or a compatible OpenShift release), Kueue and the Davano controller.',
    'Ready reflects current-generation native-controller observation, not production certification.',
    'Namespace quota and Pod admission guardrails do not provide global tenant or physical GPU isolation.',
    'Only administrator-verified whole-GPU resources are supported; no MIG or shared-device reconfiguration.',
    'Automatic tenant workload grants are disabled. Policy subjects receive no permissions; native guards are not a tenant execution boundary.',
    'Existing Pods are not retroactively evicted; inherited RBAC grants are not revoked.',
]

def install_control_api(app, source, client=None):
    enabled = os.getenv('DAVANO_CONTROL_ENABLED', 'false').lower() == 'true'
    token = os.getenv('DAVANO_API_TOKEN', '')
    if enabled and source != 'kubernetes':
        raise ValueError('Control API requires DAVANO_SOURCE=kubernetes')
    if enabled and len(token) < 32:
        raise ValueError('Control API requires a DAVANO_API_TOKEN of at least 32 characters')
    if enabled:
        client = client or KubeClient()
    router = APIRouter(prefix='/api/v1/control', tags=['Kubernetes control'])
    def authorize(authorization: str | None = Header(default=None)):
        if not enabled:
            raise HTTPException(409, 'Kubernetes control is disabled; no cluster mutation attempted')
        supplied = authorization[7:] if authorization and authorization.startswith('Bearer ') else ''
        if not hmac.compare_digest(supplied.encode("utf-8"), token.encode("utf-8")):
            raise HTTPException(401, 'A valid administrator Bearer token is required', headers={'WWW-Authenticate': 'Bearer'})
    def call(method, path, body=None):
        try:
            return client.request(method, path, body)
        except ApiError as error:
            raise HTTPException(error.status if error.status in (400, 403, 404, 409, 422) else 503, str(error)) from None
    @router.get('/status')
    def status():
        return {'enabled': enabled, 'source': source, 'mode': 'kubernetes' if enabled else 'disabled', 'capabilities': [{'id': name, 'level': 'configured' if enabled else 'disabled', 'detail': detail} for name, detail in [
            ('declarative-pools', 'GPUPool and GPUAccessPolicy desired state is persisted in Kubernetes'),
            ('kueue', 'Controller reconciles owned ResourceFlavor, ClusterQueue and LocalQueue objects'),
            ('namespace-allocation-edit', 'Administrator preview and compare-and-swap GPU quota/per-Pod ceiling updates; no cross-pool policy switch or atomic transfer'),
            ('namespace-guardrails', 'ResourceQuota and native GPU admission guards only; automatic workload RBAC grants are disabled'),
        ]], 'limitations': LIMITATIONS}
    @router.get('/pools', dependencies=[Depends(authorize)])
    def pools():
        try:
            return {'items': client.list(ROOT + '/gpupools')}
        except ApiError as error:
            raise HTTPException(503, str(error)) from None
    @router.get('/policies', dependencies=[Depends(authorize)])
    def policies():
        try:
            return {'items': client.list(ROOT + '/gpuaccesspolicies')}
        except ApiError as error:
            raise HTTPException(503, str(error)) from None
    @router.post('/pools', status_code=201, dependencies=[Depends(authorize)])
    def create_pool(body: ManagedPoolCreate):
        return call('POST', ROOT + '/gpupools', {'apiVersion': 'fleet.davano.io/v1alpha1', 'kind': 'GPUPool', 'metadata': {'name': body.name}, 'spec': body.spec.model_dump()})
    @router.post('/policies', status_code=201, dependencies=[Depends(authorize)])
    def create_policy(body: ManagedPolicyCreate):
        return call('POST', ROOT + '/namespaces/' + segment(body.namespace) + '/gpuaccesspolicies', {'apiVersion': 'fleet.davano.io/v1alpha1', 'kind': 'GPUAccessPolicy', 'metadata': {'name': body.name, 'namespace': body.namespace}, 'spec': body.spec.model_dump(exclude_none=True)})
    def delete(path, body):
        return call('DELETE', path, {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': body.model_dump(), 'propagationPolicy': 'Foreground'})
    @router.delete('/pools/{name}', dependencies=[Depends(authorize)])
    def delete_pool(name: str, body: DeletePreconditions):
        return delete(ROOT + '/gpupools/' + segment(name), body)
    @router.delete('/policies/{namespace}/{name}', dependencies=[Depends(authorize)])
    def delete_policy(namespace: str, name: str, body: DeletePreconditions):
        return delete(ROOT + '/namespaces/' + segment(namespace) + '/gpuaccesspolicies/' + segment(name), body)
    def allocation(namespace, name, body, apply):
        try:
            return AllocationService(client).update(namespace, name, body, apply=apply)
        except ApiError as error:
            raise HTTPException(error.status if error.status in (400, 403, 404, 409, 422) else 503, str(error)) from None
        except (KeyError, TypeError, ValueError, AttributeError):
            raise HTTPException(503, 'Allocation prerequisites could not be validated; no automatic retry was made') from None
    @router.post('/policies/{namespace}/{name}/allocation/preview', dependencies=[Depends(authorize)])
    def preview_allocation(namespace: str, name: str, body: AllocationUpdate):
        return allocation(namespace, name, body, False)
    @router.post('/policies/{namespace}/{name}/allocation/apply', dependencies=[Depends(authorize)])
    def apply_allocation(namespace: str, name: str, body: AllocationUpdate):
        return allocation(namespace, name, body, True)
    app.include_router(router)
    return client
