"""Single-policy allocation edits, never physical movement or cross-policy transfer.

Preflight reads are observations, not a multi-object lock. Only the policy PUT
is compare-and-swap atomic. Native controllers enforce the new desired state
asynchronously; existing Pods are never resized, stopped or evicted.
"""
from copy import deepcopy
from pydantic import ValidationError
from .control_models import ManagedPolicyCreate, PoolSpec
from .kube import ApiError, ROOT, OWNER_LABEL, MANAGER_LABEL, segment
from .reconciler import access_resources, active
from .workload_service import normalized_guard_spec
from .quantities import normalize_quota_hard

FIELDS = ('poolRef', 'gpuQuota', 'maxGPUsPerPod')
LIMITATIONS = [
    'Updates desired GPU ceilings for this namespace, not reserved capacity or physical devices.',
    'Native quota and admission changes become effective asynchronously after controller reconciliation.',
    'Existing workloads are not resized, stopped or evicted; reduced ceilings apply to future admission.',
    'No atomic transfer between policies; reducing one namespace does not reserve capacity for another.',
    'Cross-pool reassignment is blocked until a durable admission-quiescing workflow is available.',
    'CPU and memory limits remain defined by approved workload profiles; this policy edits GPU limits only.',
]


def condition(obj, name):
    generation = obj.get('metadata', {}).get('generation')
    return generation is not None and any(c.get('type') == name and c.get('status') == 'True' and
        c.get('observedGeneration') == generation for c in obj.get('status', {}).get('conditions', []))


class AllocationService:
    def __init__(self, client):
        self.client = client

    def update(self, namespace, name, request, apply=False):
        path = ROOT + '/namespaces/' + segment(namespace) + '/gpuaccesspolicies/' + segment(name)
        current = self.client.get(path)
        meta = current.get('metadata', {})
        if meta.get('uid') != request.uid or meta.get('resourceVersion') != request.resourceVersion or meta.get('deletionTimestamp'):
            raise ApiError(409, 'Policy changed or is terminating; reload and preview again')
        if meta.get('namespace') != namespace or meta.get('name') != name:
            raise ApiError(409, 'Policy identity does not match the requested namespace and name')
        try:
            ManagedPolicyCreate.model_validate({'name': name, 'namespace': namespace, 'spec': current['spec']})
        except (KeyError, ValidationError):
            raise ApiError(409, 'Existing policy is invalid; administrator repair is required') from None
        if request.poolRef != current['spec']['poolRef']:
            raise ApiError(409, 'Cross-pool reassignment requires admission quiescing and is not supported; no change made')
        policies = self.client.list(ROOT + '/gpuaccesspolicies')
        if sum(p.get('metadata', {}).get('namespace') == namespace for p in policies) != 1:
            raise ApiError(409, 'Exactly one GPUAccessPolicy must exist in the namespace')
        pool = self.client.get(ROOT + '/gpupools/' + segment(request.poolRef))
        try:
            PoolSpec.model_validate(pool['spec'])
        except (KeyError, ValidationError):
            raise ApiError(409, 'Referenced pool specification is invalid') from None
        if pool.get('metadata', {}).get('deletionTimestamp') or not condition(pool, 'Ready'):
            raise ApiError(409, 'Referenced pool has not reconciled its current generation')
        if request.gpuQuota > pool['spec']['nominalQuota']:
            raise ApiError(422, 'Namespace GPU quota exceeds pool nominal quota')
        if not condition(current, 'GuardrailsReady') or current.get('status', {}).get('workloadGrants') != 'disabled':
            raise ApiError(409, 'Current-generation native guardrails and disabled legacy grants are required')
        for collection, desired in access_resources(current, pool):
            obj = self.client.get(collection + '/' + segment(desired['metadata']['name']))
            metadata = obj.get('metadata', {})
            labels = metadata.get('labels', {})
            if metadata.get('deletionTimestamp') or labels.get(OWNER_LABEL) != request.uid or labels.get(MANAGER_LABEL) != 'davano-controller' or normalized_guard_spec(obj) != normalized_guard_spec(desired):
                raise ApiError(409, 'Native guardrail ownership or configuration drift detected')
            status = obj.get('status', {})
            if desired['kind'] == 'ValidatingAdmissionPolicy' and (status.get('observedGeneration') != metadata.get('generation') or status.get('typeChecking', {}).get('expressionWarnings')):
                raise ApiError(409, 'Native admission policy is not type-checked for its current generation')
            if desired['kind'] == 'ResourceQuota' and normalize_quota_hard(status.get('hard')) != normalize_quota_hard(desired['spec']['hard']):
                raise ApiError(409, 'ResourceQuota controller has not observed current limits')
            if desired['kind'] == 'LocalQueue' and not active(obj):
                raise ApiError(409, 'Kueue LocalQueue is not active')
        result = {'operation': 'namespace-allocation', 'status': 'preview', 'eligible': True,
                  'before': {k: current['spec'][k] for k in FIELDS},
                  'desired': {k: getattr(request, k) for k in FIELDS},
                  'preconditions': {'uid': request.uid, 'resourceVersion': request.resourceVersion},
                  'limitations': LIMITATIONS}
        if apply:
            updated = deepcopy(current)
            updated['spec'].update(result['desired'])
            # UID plus resourceVersion bind this PUT to the exact object previewed.
            # Never retry writes: a transport failure can have an uncertain outcome.
            result['policy'] = self.client.request('PUT', path, updated)
            result['status'] = 'pending-reconciliation'
        return result
