"""Declarative Kueue/namespace guardrail reconciliation, without node/device writes.

Only resources labeled with the CR's exact UID can be changed. Native admission
and quota enforce future Pod creates; existing workloads are never evicted.
"""
from copy import deepcopy
import os
from datetime import datetime, timezone
from .control_models import PoolSpec, ManagedPolicyCreate
from .inventory import allocation_mode, quantity
from .quantities import decimal_quantity, quantity_string, normalize_quota_hard
from .kube import ApiError, OwnershipConflict, GROUP, ROOT, segment

FINALIZER = GROUP + '/cleanup'
KUEUE_VERSION = os.getenv('DAVANO_KUEUE_API_VERSION', 'v1beta1')
if KUEUE_VERSION not in ('v1beta1', 'v1beta2'):
    raise ValueError('DAVANO_KUEUE_API_VERSION must be v1beta1 or v1beta2')
KUEUE_API = 'kueue.x-k8s.io/' + KUEUE_VERSION
KUEUE = '/apis/' + KUEUE_API
ADMISSION = '/apis/admissionregistration.k8s.io/v1'
RBAC = '/apis/rbac.authorization.k8s.io/v1'

class GrantRevocationPending(ApiError):
    pass

def stamp():
    return datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

def pool_name(pool):
    return 'dgp-' + pool['metadata']['uid'][:12]

def policy_name(policy):
    return 'dga-' + policy['metadata']['uid'][:12]

def cr_path(obj):
    m = obj['metadata']
    return ROOT + (f'/namespaces/{segment(m["namespace"])}/gpuaccesspolicies/' if obj['kind'] == 'GPUAccessPolicy' else '/gpupools/') + segment(m['name'])

def object_for(api_version, kind, name, spec=None, namespace=None, **kwargs):
    result = {'apiVersion': api_version, 'kind': kind, 'metadata': {'name': name}}
    if namespace:
        result['metadata']['namespace'] = namespace
    if spec is not None:
        result['spec'] = spec
    return {**result, **kwargs}

def native_policy(policy, pool):
    """Fail-closed native CEL guardrails. No webhook or signing keys required.

    The per-Pod limit conservatively sums app and *all* init-container requests.
    This can reject sequential-init layouts that would fit, never undercounts.
    """
    name, spec = policy_name(policy), policy['spec']
    resource = pool['spec']['resourceName']
    # All interpolated fields were validated by Pydantic and Kubernetes schemas.
    import json
    literal = json.dumps
    r = literal(resource)
    gpu_key = "k.startsWith('nvidia.com/') || k.startsWith('amd.com/') || k.startsWith('gpu.intel.com/')"
    selected = ' && '.join(f'{literal(k)} in object.spec.nodeSelector && object.spec.nodeSelector[{literal(k)}] == {literal(v)}' for k, v in sorted(pool['spec']['nodeSelector'].items()))
    variables = [
        {'name': 'containers', 'expression': 'object.spec.containers + (has(object.spec.initContainers) ? object.spec.initContainers : [])'},
        {'name': 'usesGPU', 'expression': f'variables.containers.exists(c, has(c.resources) && ((has(c.resources.limits) && c.resources.limits.exists(k, {gpu_key})) || (has(c.resources.requests) && c.resources.requests.exists(k, {gpu_key}))))'},
        {'name': 'gpuContainers', 'expression': f'variables.containers.filter(c, has(c.resources) && has(c.resources.limits) && {r} in c.resources.limits)'},
    ]
    validations = [
        {'expression': f'!variables.usesGPU || variables.containers.all(c, !has(c.resources) || ((!has(c.resources.limits) || c.resources.limits.all(k, !({gpu_key}) || k == {r})) && (!has(c.resources.requests) || c.resources.requests.all(k, !({gpu_key}) || (k == {r} && has(c.resources.limits) && {r} in c.resources.limits && string(c.resources.requests[k]) == string(c.resources.limits[k]))))))', 'message': 'GPU resource must match the assigned pool and requests must equal limits'},
        {'expression': f'!variables.usesGPU || variables.gpuContainers.all(c, string(c.resources.limits[{r}]).matches("^[0-9]+$"))', 'message': 'GPU limits must be nonnegative plain integers'},
        {'expression': f'!variables.usesGPU || variables.gpuContainers.map(c, int(c.resources.limits[{r}])).sum() <= {spec["maxGPUsPerPod"]}', 'message': 'Conservative total of GPU limits across app and init containers exceeds maxGPUsPerPod'},
        {'expression': f'!variables.usesGPU || (has(object.spec.nodeSelector) && ({selected}) && !has(object.spec.nodeName))', 'message': 'GPU Pods require the assigned protected pool node selector and must use scheduler placement'},
        {'expression': f'!has(object.spec.overhead) || !object.spec.overhead.exists(k, {gpu_key})', 'message': 'GPU Pod overhead is unsupported'},
    ]
    vap = object_for('admissionregistration.k8s.io/v1', 'ValidatingAdmissionPolicy', name, {
        'failurePolicy': 'Fail',
        'matchConstraints': {'resourceRules': [{'apiGroups': [''], 'apiVersions': ['v1'], 'operations': ['CREATE'], 'resources': ['pods']}], 'matchPolicy': 'Equivalent'},
        'matchConditions': [{'name': 'tenant-namespace', 'expression': 'request.namespace == ' + literal(policy['metadata']['namespace'])}],
        'variables': variables, 'validations': validations,
    })
    binding = object_for('admissionregistration.k8s.io/v1', 'ValidatingAdmissionPolicyBinding', name, {'policyName': name, 'validationActions': ['Deny']})
    return vap, binding

def job_policy(policy, pool):
    """GPU batch Jobs must enter the assigned Kueue queue suspended."""
    import json
    name = policy_name(policy) + '-jobs'
    gpu_key = "k.startsWith('nvidia.com/') || k.startsWith('amd.com/') || k.startsWith('gpu.intel.com/')"
    vap = object_for('admissionregistration.k8s.io/v1', 'ValidatingAdmissionPolicy', name, {
        'failurePolicy': 'Fail',
        'matchConstraints': {'resourceRules': [{'apiGroups': ['batch'], 'apiVersions': ['v1'], 'operations': ['CREATE'], 'resources': ['jobs']}], 'matchPolicy': 'Equivalent'},
        'matchConditions': [{'name': 'tenant-namespace', 'expression': 'request.namespace == ' + json.dumps(policy['metadata']['namespace'])}],
        'variables': [
            {'name': 'containers', 'expression': 'object.spec.template.spec.containers + (has(object.spec.template.spec.initContainers) ? object.spec.template.spec.initContainers : [])'},
            {'name': 'usesGPU', 'expression': f'variables.containers.exists(c, has(c.resources) && ((has(c.resources.limits) && c.resources.limits.exists(k, {gpu_key})) || (has(c.resources.requests) && c.resources.requests.exists(k, {gpu_key}))))'},
        ],
        'validations': [
            {'expression': '!variables.usesGPU || (has(object.metadata.labels) && "kueue.x-k8s.io/queue-name" in object.metadata.labels && object.metadata.labels["kueue.x-k8s.io/queue-name"] == ' + json.dumps(policy_name(policy)) + ')', 'message': 'GPU Jobs must use the assigned Kueue LocalQueue'},
            {'expression': '!variables.usesGPU || (has(object.spec.suspend) && object.spec.suspend)', 'message': 'GPU Jobs must be created suspended for Kueue admission'},
        ],
    })
    binding = object_for('admissionregistration.k8s.io/v1', 'ValidatingAdmissionPolicyBinding', name, {'policyName': name, 'validationActions': ['Deny']})
    return vap, binding

def pool_resources(pool, namespaces, cpu_quota="0", memory_quota="0"):
    name, spec = pool_name(pool), pool['spec']
    flavor = object_for(KUEUE_API, 'ResourceFlavor', name, {'nodeLabels': spec['nodeSelector']})
    # Empty namespaceSelector means all namespaces. An impossible In+NotIn
    # conjunction denies everyone without relying on a supposedly absent name.
    selector = {'matchExpressions': [{'key': 'kubernetes.io/metadata.name', 'operator': 'In', 'values': sorted(namespaces) or ['davano-deny-all'] }]}
    if not namespaces:
        selector['matchExpressions'].append({'key': 'kubernetes.io/metadata.name', 'operator': 'NotIn', 'values': ['davano-deny-all']})
    queue = object_for(KUEUE_API, 'ClusterQueue', name, {'namespaceSelector': selector, 'queueingStrategy': 'StrictFIFO', 'stopPolicy': 'None', 'resourceGroups': [{'coveredResources': ['cpu', 'memory', spec['resourceName']], 'flavors': [{'name': name, 'resources': [{'name': 'cpu', 'nominalQuota': cpu_quota}, {'name': 'memory', 'nominalQuota': memory_quota}, {'name': spec['resourceName'], 'nominalQuota': str(spec['nominalQuota'])}]}]}]})
    return [(KUEUE + '/resourceflavors', flavor), (KUEUE + '/clusterqueues', queue)]

def access_resources(policy, pool):
    name, ns, spec = policy_name(policy), policy['metadata']['namespace'], policy['spec']
    base = f'/api/v1/namespaces/{segment(ns)}'
    vap, binding = native_policy(policy, pool)
    job_vap, job_binding = job_policy(policy, pool)
    return [
        # This release reconciles guardrails only. It never grants workload creation.
        (base + '/resourcequotas', object_for('v1', 'ResourceQuota', name, {'hard': {'requests.' + pool['spec']['resourceName']: str(spec['gpuQuota'])}}, namespace=ns)),
        (ADMISSION + '/validatingadmissionpolicies', vap),
        (ADMISSION + '/validatingadmissionpolicybindings', binding),
        (ADMISSION + '/validatingadmissionpolicies', job_vap),
        (ADMISSION + '/validatingadmissionpolicybindings', job_binding),
        (KUEUE + f'/namespaces/{segment(ns)}/localqueues', object_for(KUEUE_API, 'LocalQueue', name, {'clusterQueue': pool_name(pool)}, namespace=ns)),
    ]

def active(obj):
    return any(c.get('type') == 'Active' and c.get('status') == 'True' and c.get('observedGeneration') == obj['metadata'].get('generation') for c in obj.get('status', {}).get('conditions', []))

class Reconciler:
    def __init__(self, client, namespaces):
        self.client = client
        self.namespaces = set(namespaces)
        self.pools = []
        self.policies = []
        self.nodes = []

    def current_object(self, obj):
        """Fence a stale snapshot before writing its status, finalizer or children."""
        current = self.client.get(cr_path(obj))
        old_meta, current_meta = obj['metadata'], current['metadata']
        if current_meta.get('uid') != old_meta.get('uid'):
            raise ApiError(409, 'Resource UID changed during reconciliation; refusing stale writes')
        if current_meta.get('generation', 1) != old_meta.get('generation', 1):
            raise ApiError(409, 'Resource generation changed during reconciliation; retry latest desired state')
        if bool(current_meta.get('deletionTimestamp')) != bool(old_meta.get('deletionTimestamp')):
            raise ApiError(409, 'Resource deletion state changed during reconciliation; refusing stale writes')
        return current

    def save_status(self, obj, ready, reason, message, managed=None):
        path = cr_path(obj)
        current = self.current_object(obj)
        old = current.get('status', {}).get('conditions', [])
        state = 'True' if ready else 'False'
        prev = next((c for c in old if c.get('type') == 'Ready'), {})
        condition = {'type': 'Ready', 'status': state, 'reason': reason, 'message': message[:1024], 'observedGeneration': obj['metadata'].get('generation', 1), 'lastTransitionTime': prev.get('lastTransitionTime') if prev.get('status') == state and prev.get('reason') == reason else stamp()}
        status = {'observedGeneration': obj['metadata'].get('generation', 1), 'conditions': [condition], 'managedResources': managed or [], 'enforcement': 'kubernetes-native' if ready else 'pending', 'lastReconcileTime': stamp()}
        if obj['kind'] == 'GPUAccessPolicy':
            status['queueName'] = policy_name(obj)
            revoking = reason == 'LegacyGrantRevocationPending'
            status['workloadGrants'] = 'revocation-pending' if revoking else 'disabled'
            status['conditions'].append({'type': 'WorkloadAccess', 'status': 'Unknown' if revoking else 'False', 'reason': 'LegacyGrantRevocationPending' if revoking else 'AutomaticGrantsDisabled', 'message': 'Legacy workload permissions may remain until owned binding deletion is verified' if revoking else 'Subject declarations do not grant workload permissions in this release', 'observedGeneration': obj['metadata'].get('generation', 1), 'lastTransitionTime': stamp()})
            if reason == 'WorkloadGrantsDisabled':
                status['conditions'].append({'type': 'GuardrailsReady', 'status': 'True', 'reason': 'Reconciled', 'message': 'Native quota, placement, per-Pod limit and GPU Job queue guards reconciled; no workload access granted', 'observedGeneration': obj['metadata'].get('generation', 1), 'lastTransitionTime': stamp()})
        else:
            status['clusterQueueName'] = pool_name(obj)
        self.client.request('PUT', path + '/status', {**current, 'status': status})

    def finalize(self, obj):
        self.current_object(obj)
        finalizers = obj['metadata'].get('finalizers', [])
        if FINALIZER not in finalizers:
            return False
        if obj['kind'] == 'GPUPool':
            if any(p['spec']['poolRef'] == obj['metadata']['name'] for p in self.policies):
                raise ApiError(409, 'Delete dependent GPUAccessPolicies before deleting the pool')
            resources = pool_resources(obj, [])
            try:
                queue = self.client.get(KUEUE + '/clusterqueues/' + pool_name(obj))
            except ApiError as error:
                if error.status != 404:
                    raise
                queue = {}
            if queue.get('status', {}).get('admittedWorkloads', 0) or queue.get('status', {}).get('pendingWorkloads', 0) or queue.get('status', {}).get('reservingWorkloads', 0):
                raise ApiError(409, 'Drain admitted, reserving and pending Kueue workloads before deletion')
        else:
            ns = obj['metadata']['namespace']
            # Removing guards while Pods exist can widen access; require a fully
            # drained namespace, not just a best-effort GPU request parse.
            pods = self.client.list(f'/api/v1/namespaces/{segment(ns)}/pods')
            if any(p.get('status', {}).get('phase') not in ('Succeeded', 'Failed') for p in pods):
                raise ApiError(409, 'Drain active Pods before deleting namespace GPU guardrails')
            try:
                queue = self.client.get(KUEUE + f'/namespaces/{segment(ns)}/localqueues/' + policy_name(obj))
            except ApiError as error:
                if error.status != 404:
                    raise
                queue = {}
            if queue.get('status', {}).get('admittedWorkloads', 0) or queue.get('status', {}).get('pendingWorkloads', 0) or queue.get('status', {}).get('reservingWorkloads', 0):
                raise ApiError(409, 'Drain admitted, reserving and pending LocalQueue workloads before deleting access')
            pool = next((p for p in self.pools if p['metadata']['name'] == obj['spec']['poolRef']), None)
            if pool is None:
                raise ApiError(409, 'Referenced pool missing; retain guardrails for manual recovery')
            resources = access_resources(obj, pool)
        # An accepted DELETE is not completed deletion. Keep the parent and all
        # remaining guards until each descendant is actually absent (404).
        for collection, resource in reversed(resources):
            self.current_object(obj)
            name = resource['metadata']['name']
            self.client.delete_owned(collection, name, obj)
            try:
                self.client.get(collection + '/' + segment(name))
            except ApiError as error:
                if error.status != 404:
                    raise
            else:
                raise ApiError(409, 'Waiting for owned resource deletion before removing cleanup finalizer')
        current = self.current_object(obj)
        current['metadata']['finalizers'] = [f for f in current['metadata'].get('finalizers', []) if f != FINALIZER]
        self.client.request('PUT', cr_path(obj), current)
        return True

    def ensure_finalizer(self, obj):
        self.current_object(obj)
        if FINALIZER not in obj['metadata'].get('finalizers', []):
            current = self.current_object(obj)
            if FINALIZER in current['metadata'].get('finalizers', []):
                return
            current['metadata']['finalizers'] = current['metadata'].get('finalizers', []) + [FINALIZER]
            self.client.request('PUT', cr_path(obj), current)

    def reconcile_pool(self, pool):
        spec = PoolSpec.model_validate(pool['spec']).model_dump()
        for other in self.pools:
            if other['metadata']['uid'] == pool['metadata']['uid']:
                continue
            a, b = spec['nodeSelector'], other['spec']['nodeSelector']
            if not any(k in b and b[k] != v for k, v in a.items()):
                raise ApiError(409, 'Pool selectors may overlap; use distinct protected label values')
        matches = [n for n in self.nodes if all(n['metadata'].get('labels', {}).get(k) == v for k, v in spec['nodeSelector'].items())]
        if not matches:
            raise ApiError(409, 'No nodes match the protected pool selector')
        capacity = 0
        cpu_capacity = memory_capacity = 0
        for node in matches:
            key = spec['resourceName']
            count = quantity(node.get('status', {}).get('allocatable', {}).get(key))
            if count is None or allocation_mode(key, node['metadata'].get('labels', {})) != 'whole-gpu':
                raise ApiError(409, 'All selected nodes must advertise verified whole-GPU scheduling units')
            capacity += count
            try:
                cpu_capacity += decimal_quantity(node['status']['allocatable']['cpu'])
                memory_capacity += decimal_quantity(node['status']['allocatable']['memory'])
            except (ValueError, KeyError):
                raise ApiError(409, 'Selected node CPU/memory allocatable counts are unavailable') from None
        if spec['nominalQuota'] > capacity:
            raise ApiError(409, 'Pool quota exceeds observed allocatable GPU units')
        namespaces = {p['metadata']['namespace'] for p in self.policies if p['spec']['poolRef'] == pool['metadata']['name'] and p['metadata']['namespace'] in self.namespaces and not p['metadata'].get('deletionTimestamp')}
        resources = pool_resources(pool, namespaces, quantity_string(cpu_capacity), quantity_string(memory_capacity))
        objects = []
        for path, body in resources:
            self.current_object(pool)
            objects.append(self.client.apply_owned(path, body, pool))
        ready = active(objects[-1])
        return ready, 'Reconciled' if ready else 'WaitingForKueue', 'Owned ResourceFlavor and ClusterQueue applied; waiting for Kueue Active condition' if not ready else 'Kueue ClusterQueue is Active; quota applies to admitted queued workloads', resources

    def reconcile_policy(self, policy):
        ns = policy['metadata']['namespace']
        ManagedPolicyCreate.model_validate({'name': policy['metadata']['name'], 'namespace': ns, 'spec': policy['spec']})
        if ns not in self.namespaces:
            raise ApiError(403, 'Namespace is not in DAVANO_MANAGED_NAMESPACES')
        if sum(p['metadata']['namespace'] == ns for p in self.policies) != 1:
            raise ApiError(409, 'Exactly one GPUAccessPolicy is permitted per namespace')
        pool = next((p for p in self.pools if p['metadata']['name'] == policy['spec']['poolRef'] and not p['metadata'].get('deletionTimestamp')), None)
        if pool is None:
            raise ApiError(409, 'Referenced GPUPool does not exist or is terminating')
        # Always re-read the pool status after this cycle's pool reconciliation.
        pool = self.client.get(cr_path(pool))
        if not any(c.get('type') == 'Ready' and c.get('status') == 'True' and c.get('observedGeneration') == pool['metadata'].get('generation', 1) for c in pool.get('status', {}).get('conditions', [])):
            raise ApiError(409, 'Referenced GPUPool has not reconciled its current generation')
        if policy['spec']['gpuQuota'] > pool['spec']['nominalQuota']:
            raise ApiError(409, 'Namespace GPU quota exceeds pool nominal quota')
        self.client.get('/api/v1/namespaces/' + segment(ns))
        resources = access_resources(policy, pool)
        objects = []
        for path, body in resources:
            self.current_object(policy)
            obj = self.client.apply_owned(path, body, policy)
            objects.append(obj)
            if body['kind'] == 'ValidatingAdmissionPolicy':
                status = obj.get('status', {})
                if status.get('observedGeneration') != obj['metadata'].get('generation') or status.get('typeChecking', {}).get('expressionWarnings'):
                    # Do not grant role bindings or advertise admission enforcement
                    # until the API server has type-checked this policy generation.
                    return False, 'WaitingForAdmission', 'Waiting for API-server CEL policy type checking; automatic workload access remains disabled', resources[:len(objects)]
        quota = objects[0]
        quota_ready = normalize_quota_hard(quota.get('status', {}).get('hard', {})) == normalize_quota_hard(quota['spec']['hard'])
        queue_ready = active(next(o for o in objects if o['kind'] == 'LocalQueue'))
        ready = quota_ready and queue_ready
        return False, 'WorkloadGrantsDisabled' if ready else 'WaitingForControllers', 'Native namespace guardrails reconciled; automatic workload access is disabled because arbitrary Job templates can borrow namespace credentials or host privileges' if ready else 'Waiting for ResourceQuota and Kueue LocalQueue controller observation; workload access remains disabled', resources

    def hold_pool(self, pool):
        """Request no-new-admission on an existing owned queue; never evict."""
        self.current_object(pool)
        collection = KUEUE + '/clusterqueues'
        try:
            current = self.client.get(collection + '/' + pool_name(pool))
        except ApiError as error:
            if error.status == 404:
                return False
            raise
        current['spec']['stopPolicy'] = 'Hold'
        self.current_object(pool)
        self.client.apply_owned(collection, current, pool)
        return True

    def revoke_legacy_grant(self, policy):
        """Remove only our previous workload grant, independent of pool readiness.

        Workload create permissions can borrow arbitrary namespace credentials
        even with restricted Pod Security. Do not recreate or adopt bindings.
        """
        namespace = policy['metadata']['namespace']
        self.current_object(policy)
        collection = RBAC + f'/namespaces/{segment(namespace)}/rolebindings'
        self.client.delete_owned(collection, policy_name(policy), policy)
        try:
            self.client.get(collection + '/' + policy_name(policy))
        except ApiError as error:
            if error.status != 404:
                raise
        else:
            raise GrantRevocationPending(409, 'Legacy workload binding deletion is pending; old permissions may remain')

    def cycle(self):
        self.policies = self.client.list(ROOT + '/gpuaccesspolicies')
        revocation_errors = {}
        for policy in self.policies:
            if policy['metadata']['namespace'] in self.namespaces:
                try:
                    self.revoke_legacy_grant(policy)
                except ApiError as error:
                    revocation_errors[policy['metadata']['uid']] = GrantRevocationPending(error.status, 'Legacy workload-grant removal could not be verified; existing permissions may remain')
                    try:
                        self.save_status(policy, False, 'LegacyGrantRevocationPending', str(revocation_errors[policy['metadata']['uid']]))
                    except ApiError:
                        pass
        # Only safety revocation is allowed before complete inventory snapshots.
        # Desired-state creation/update remains blocked on all required reads.
        self.pools = self.client.list(ROOT + '/gpupools')
        try:
            self.nodes = self.client.list('/api/v1/nodes')
        except ApiError:
            # Complete pool identities are available. Pause their owned queues
            # when fresh device eligibility cannot be observed, if API writes work.
            for pool in self.pools:
                if not pool['metadata'].get('deletionTimestamp'):
                    try:
                        held = self.hold_pool(pool)
                        self.save_status(pool, False, 'InventoryUnavailable', 'Fresh node eligibility is unavailable; owned queue admission Hold requested' if held else 'Fresh node eligibility is unavailable; no owned queue exists')
                    except ApiError:
                        pass
            raise
        results = []
        for obj in self.pools + self.policies:
            try:
                if obj['kind'] == 'GPUAccessPolicy' and obj['metadata']['namespace'] not in self.namespaces:
                    continue
                if obj['metadata']['uid'] in revocation_errors:
                    raise revocation_errors[obj['metadata']['uid']]
                if obj['metadata'].get('deletionTimestamp'):
                    self.finalize(obj)
                    results.append((obj['metadata']['name'], 'deleting'))
                    continue
                self.ensure_finalizer(obj)
                ready, reason, message, resources = self.reconcile_pool(obj) if obj['kind'] == 'GPUPool' else self.reconcile_policy(obj)
                managed = [{'kind': body['kind'], 'name': body['metadata']['name'], **({'namespace': body['metadata']['namespace']} if 'namespace' in body['metadata'] else {})} for _, body in resources]
                self.save_status(obj, ready, reason, message, managed)
                results.append((obj['metadata']['name'], reason))
            except Exception as error:
                reason = 'LegacyGrantRevocationPending' if isinstance(error, GrantRevocationPending) else 'OwnershipConflict' if isinstance(error, OwnershipConflict) else 'ReconcileFailed'
                # Never echo untrusted transport payloads, credentials, or stack traces.
                message = str(error) if isinstance(error, ApiError) else 'Invalid resource configuration or reconciliation failure'
                if obj['kind'] == 'GPUPool' and not obj['metadata'].get('deletionTimestamp'):
                    try:
                        if self.hold_pool(obj):
                            message += '; Kueue no-new-admission Hold requested, without eviction'
                    except ApiError:
                        message += '; unable to verify an admission Hold on the owned queue'
                try:
                    self.save_status(obj, False, reason, message)
                except ApiError:
                    pass  # A conflict is retried on the next full reconciliation.
                results.append((obj['metadata']['name'], reason))
        return results
