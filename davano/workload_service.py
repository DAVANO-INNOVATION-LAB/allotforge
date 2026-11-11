"""Constrained Job broker. It never grants Kubernetes permissions to tenants.

Only administrator-pinned profiles can run. The local durable registry is an
additional ownership boundary, not a Kubernetes scheduler or reservation ledger.
"""
from copy import deepcopy
import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timezone
from .kube import ApiError, ROOT, OWNER_LABEL, MANAGER_LABEL, segment
from .control_models import PoolSpec, ManagedPolicyCreate
from .reconciler import KUEUE, access_resources, policy_name, pool_name, active
from .inventory import allocation_mode, quantity
from .quantities import normalize_quota_hard

PROJECT_LABEL = 'fleet.davano.io/project'
PROFILE_LABEL = 'fleet.davano.io/profile'
OPERATION_ANNOTATION = 'fleet.davano.io/execution-operation'
MANAGER = 'davano-execution'


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def stamp():
    return datetime.now(timezone.utc).isoformat()


def build_job(project, profile, request, policy, pool, operation_id):
    labels = {MANAGER_LABEL: MANAGER, PROJECT_LABEL: project.id, PROFILE_LABEL: profile.id,
              'kueue.x-k8s.io/queue-name': policy_name(policy)}
    resources = {'cpu': f'{profile.cpu_millicores}m', 'memory': f'{profile.memory_mib}Mi', pool['spec']['resourceName']: str(request.gpu_count)}
    return {'apiVersion': 'batch/v1', 'kind': 'Job', 'metadata': {
        'name': request.name, 'namespace': project.namespace, 'labels': labels,
        'annotations': {OPERATION_ANNOTATION: operation_id}}, 'spec': {
        'suspend': True, 'parallelism': 1, 'completions': 1, 'backoffLimit': 0,
        'activeDeadlineSeconds': profile.max_runtime_seconds,
        'template': {'metadata': {'labels': {MANAGER_LABEL: MANAGER, PROJECT_LABEL: project.id, PROFILE_LABEL: profile.id}}, 'spec': {
            'serviceAccountName': project.service_account, 'automountServiceAccountToken': False,
            'restartPolicy': 'Never', 'enableServiceLinks': False,
            'hostNetwork': False, 'hostPID': False, 'hostIPC': False,
            'nodeSelector': pool['spec']['nodeSelector'],
            'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}},
            'containers': [{'name': 'workload', 'image': profile.image, 'imagePullPolicy': 'IfNotPresent',
                            'command': profile.command, 'resources': {'requests': resources, 'limits': resources},
                            'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}}}]
        }}}}


def normalized_guard_spec(obj):
    """Normalize only documented admission/Kueue API defaults, never broad subsets."""
    spec = deepcopy(obj.get('spec', {}))
    if obj.get('kind') == 'ValidatingAdmissionPolicy':
        constraints = spec.get('matchConstraints', {})
        for key in ('namespaceSelector', 'objectSelector'):
            if constraints.get(key) == {}:
                constraints.pop(key)
        for rule in constraints.get('resourceRules', []):
            if rule.get('scope') == '*':
                rule.pop('scope')
    if obj.get('kind') == 'ResourceQuota' and 'hard' in spec:
        spec['hard'] = normalize_quota_hard(spec['hard'])
    if obj.get('kind') == 'LocalQueue' and spec.get('stopPolicy') == 'None':
        spec.pop('stopPolicy')
    return spec


def summarize(job, project):
    meta, status, spec = job['metadata'], job.get('status', {}), job['spec']
    conditions = [{'type': c.get('type'), 'status': c.get('status'), 'reason': c.get('reason', '')} for c in status.get('conditions', [])]
    true = {c['type'] for c in conditions if c['status'] == 'True'}
    phase = ('Deleting' if meta.get('deletionTimestamp') else 'Succeeded' if 'Complete' in true else 'Failed' if 'Failed' in true else
             'Running' if status.get('active', 0) else 'Suspended' if spec.get('suspend') else 'Pending')
    limits = spec['template']['spec']['containers'][0].get('resources', {}).get('limits', {})
    gpu_count = sum(int(v) for k, v in limits.items() if k.startswith(('nvidia.com/', 'amd.com/', 'gpu.intel.com/')))
    return {'name': meta['name'], 'uid': meta['uid'], 'resourceVersion': meta['resourceVersion'], 'project': project.id,
            'namespace': project.namespace, 'profile': meta.get('labels', {}).get(PROFILE_LABEL), 'gpu_count': gpu_count,
            'phase': phase, 'conditions': conditions, 'created_at': meta.get('creationTimestamp'),
            'started_at': status.get('startTime'), 'completed_at': status.get('completionTime'),
            'allocation_confirmed': False}


class ExecutionService:
    def __init__(self, client, store, config):
        self.client, self.store, self.config = client, store, config
        with store.lock, store.db:
            store.db.executescript('''
            CREATE TABLE IF NOT EXISTS execution_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL,
                project TEXT NOT NULL, operation TEXT NOT NULL, workload TEXT NOT NULL,
                outcome TEXT NOT NULL, operation_id TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS execution_jobs (
                project TEXT NOT NULL, name TEXT NOT NULL, operation_id TEXT NOT NULL,
                request_hash TEXT NOT NULL, uid TEXT, desired_hash TEXT, reservation_id TEXT, PRIMARY KEY(project,name));
            ''')
            if 'desired_hash' not in {row[1] for row in store.db.execute('PRAGMA table_info(execution_jobs)')}:
                store.db.execute('ALTER TABLE execution_jobs ADD COLUMN desired_hash TEXT')
            if 'reservation_id' not in {row[1] for row in store.db.execute('PRAGMA table_info(execution_jobs)')}:
                store.db.execute('ALTER TABLE execution_jobs ADD COLUMN reservation_id TEXT')

    def audit(self, actor, project, operation, workload, outcome, operation_id):
        # Reserve both intent/result records before mutation. Local records are
        # bounded, not tamper-evident; only a pending result can be completed.
        try:
            with self.store.lock, self.store.db:
                self.store.db.execute('BEGIN IMMEDIATE')
                if outcome == 'intent':
                    count = self.store.db.execute('SELECT count(*) FROM execution_audit WHERE project=?', (project,)).fetchone()[0]
                    if count + 2 > 10000:
                        raise ApiError(409, 'Project audit capacity reached; administrator archival is required before more mutations')
                    for state in ('intent', 'pending'):
                        self.store.db.execute('INSERT INTO execution_audit(at,actor,project,operation,workload,outcome,operation_id) VALUES (?,?,?,?,?,?,?)',
                                              (stamp(), actor, project, operation, workload, state, operation_id))
                else:
                    updated = self.store.db.execute('UPDATE execution_audit SET at=?,outcome=? WHERE operation_id=? AND project=? AND outcome=?',
                                                    (stamp(), outcome, operation_id, project, 'pending'))
                    if updated.rowcount != 1:
                        raise ApiError(503, 'Execution audit result reservation missing; mutation outcome requires reconciliation')
        except sqlite3.Error:
            raise ApiError(503, 'Execution audit persistence unavailable; mutation outcome may require reconciliation') from None

    def registry(self, project, name):
        with self.store.lock:
            row = self.store.db.execute('SELECT operation_id,request_hash,uid,desired_hash,reservation_id FROM execution_jobs WHERE project=? AND name=?', (project.id, name)).fetchone()
        return dict(zip(('operation_id', 'request_hash', 'uid', 'desired_hash', 'reservation_id'), row)) if row else None

    def record_uid(self, project, name, uid):
        with self.store.lock, self.store.db:
            self.store.db.execute('UPDATE execution_jobs SET uid=? WHERE project=? AND name=? AND (uid IS NULL OR uid=?)', (uid, project.id, name, uid))

    @staticmethod
    def job_path(project, name=None):
        return '/apis/batch/v1/namespaces/' + segment(project.namespace) + '/jobs' + ('/' + segment(name) if name else '')

    def verify_namespace(self, project):
        namespace = self.client.get('/api/v1/namespaces/' + segment(project.namespace))
        meta = namespace.get('metadata', {})
        if meta.get('uid') != project.namespace_uid or meta.get('deletionTimestamp'):
            raise ApiError(409, 'Execution namespace identity changed or is terminating')
        return namespace

    def owned(self, project, name):
        self.verify_namespace(project)
        record = self.registry(project, name)
        if record is None:
            raise ApiError(404, 'Workload not found in this project execution registry')
        job = self.client.get(self.job_path(project, name))
        meta = job.get('metadata', {})
        labels = meta.get('labels', {})
        if meta.get('namespace') != project.namespace or labels.get(MANAGER_LABEL) != MANAGER or labels.get(PROJECT_LABEL) != project.id or meta.get('annotations', {}).get(OPERATION_ANNOTATION) != record['operation_id'] or (record['uid'] and record['uid'] != meta.get('uid')):
            raise ApiError(409, 'Workload ownership changed; refusing access or mutation')
        if not meta.get('uid') or not meta.get('resourceVersion'):
            raise ApiError(503, 'Incomplete Kubernetes workload identity')
        if not record['uid']:
            self.record_uid(project, name, meta['uid'])
        return job

    def list(self, project):
        self.verify_namespace(project)
        with self.store.lock:
            names = [r[0] for r in self.store.db.execute('SELECT name FROM execution_jobs WHERE project=? ORDER BY name LIMIT 1001', (project.id,))]
        if len(names) > 1000:
            raise ApiError(503, 'Execution history exceeds bounded listing limit; archive completed registry entries administratively')
        result = []
        for name in names:
            try:
                result.append(summarize(self.owned(project, name), project))
            except ApiError as error:
                if error.status != 404:
                    raise
        return result

    @staticmethod
    def condition(obj, name):
        return any(c.get('type') == name and c.get('status') == 'True' and c.get('observedGeneration') == obj['metadata'].get('generation', 1) for c in obj.get('status', {}).get('conditions', []))

    def preflight(self, project, request):
        profile = next((p for p in self.config.profiles if p.id == request.profile and p.id in project.profiles), None)
        if profile is None:
            raise ApiError(403, 'Execution profile is not allowed for this project')
        namespace = self.verify_namespace(project)
        labels = namespace['metadata'].get('labels', {})
        if labels.get('pod-security.kubernetes.io/enforce') != 'restricted' or labels.get('pod-security.kubernetes.io/enforce-version') != 'latest' or labels.get(PROJECT_LABEL) != project.id:
            raise ApiError(409, 'Dedicated project namespace requires restricted/latest Pod Security and the configured project label')
        base = '/api/v1/namespaces/' + segment(project.namespace)
        sa = self.client.get(base + '/serviceaccounts/' + segment(project.service_account))
        if sa.get('metadata', {}).get('uid') != project.service_account_uid or sa.get('metadata', {}).get('deletionTimestamp') or sa.get('automountServiceAccountToken') is not False or sa.get('imagePullSecrets') or sa.get('secrets'):
            raise ApiError(409, 'Runtime ServiceAccount must match its pinned UID, disable token mounting and reference no secrets')
        bounds = self.client.get(base + '/resourcequotas/' + segment(project.execution_quota))
        if bounds.get('metadata', {}).get('deletionTimestamp') or bounds.get('spec') != {'hard': {'count/jobs.batch': str(project.max_jobs)}} or bounds.get('status', {}).get('hard') != bounds.get('spec', {}).get('hard'):
            raise ApiError(409, 'Observed namespace Job-count quota must match the configured execution bound')
        policies = self.client.list('/apis/networking.k8s.io/v1/namespaces/' + segment(project.namespace) + '/networkpolicies')
        if len(policies) != 1 or policies[0]['metadata']['name'] != project.network_policy or policies[0]['metadata'].get('deletionTimestamp'):
            raise ApiError(409, 'Project requires exactly its reviewed default-deny NetworkPolicy; additive allow policies are unsupported')
        net_spec = policies[0].get('spec', {})
        if net_spec.get('podSelector') != {} or set(net_spec.get('policyTypes', [])) != {'Ingress', 'Egress'} or net_spec.get('ingress', []) or net_spec.get('egress', []):
            raise ApiError(409, 'Project NetworkPolicy must deny all ingress and egress')
        policy = self.client.get(ROOT + '/namespaces/' + segment(project.namespace) + '/gpuaccesspolicies/' + segment(project.policy))
        if policy.get('metadata', {}).get('uid') != project.policy_uid or policy['metadata'].get('deletionTimestamp'):
            raise ApiError(409, 'Configured GPUAccessPolicy identity changed or is terminating')
        ManagedPolicyCreate.model_validate({'name': project.policy, 'namespace': project.namespace, 'spec': policy['spec']})
        pool = self.client.get(ROOT + '/gpupools/' + segment(policy['spec']['poolRef']))
        if pool.get('metadata', {}).get('uid') != project.pool_uid or pool['metadata'].get('deletionTimestamp'):
            raise ApiError(409, 'Configured GPUPool identity changed or is terminating')
        PoolSpec.model_validate(pool['spec'])
        if not self.condition(policy, 'GuardrailsReady') or policy.get('status', {}).get('workloadGrants') != 'disabled' or not self.condition(pool, 'Ready'):
            raise ApiError(409, 'Current-generation GPU guardrails and pool readiness are required; legacy grants must be disabled')
        for obj in (policy, pool):
            try:
                reconciled = datetime.fromisoformat(obj['status']['lastReconcileTime'].replace('Z', '+00:00'))
                age = (datetime.now(timezone.utc) - reconciled).total_seconds()
                if not 0 <= age <= 300:
                    raise ValueError()
            except (KeyError, TypeError, ValueError):
                raise ApiError(409, 'Guardrail controller observation is missing or older than five minutes') from None
        if request.gpu_count > min(profile.gpu_max, policy['spec']['maxGPUsPerPod'], policy['spec']['gpuQuota'], pool['spec']['nominalQuota']):
            raise ApiError(422, 'Requested GPU count exceeds the approved profile or project policy')
        # Check actual native objects as well as CR conditions. This is still not
        # proof of CNI enforcement, cache activation, GPU isolation or scheduling.
        for collection, desired in access_resources(policy, pool):
            observed = self.client.get(collection + '/' + segment(desired['metadata']['name']))
            metadata = observed.get('metadata', {})
            if metadata.get('deletionTimestamp') or metadata.get('labels', {}).get(OWNER_LABEL) != project.policy_uid or metadata.get('labels', {}).get(MANAGER_LABEL) != 'davano-controller' or normalized_guard_spec(observed) != normalized_guard_spec(desired):
                raise ApiError(409, 'Native guardrail ownership or configuration drift detected')
            if observed['kind'] == 'ValidatingAdmissionPolicy' and (observed.get('status', {}).get('observedGeneration') != metadata.get('generation') or observed.get('status', {}).get('typeChecking', {}).get('expressionWarnings')):
                raise ApiError(409, 'Native admission policy is not type-checked for its current generation')
            if observed['kind'] == 'ResourceQuota' and normalize_quota_hard(observed.get('status', {}).get('hard')) != normalize_quota_hard(desired['spec']['hard']):
                raise ApiError(409, 'ResourceQuota controller has not observed current limits')
            if observed['kind'] == 'LocalQueue' and not active(observed):
                raise ApiError(409, 'Kueue LocalQueue is not active')
        queue = self.client.get(KUEUE + '/clusterqueues/' + pool_name(pool))
        if queue.get('metadata', {}).get('labels', {}).get(OWNER_LABEL) != project.pool_uid or queue.get('metadata', {}).get('deletionTimestamp') or queue.get('spec', {}).get('stopPolicy', 'None') != 'None' or not active(queue):
            raise ApiError(409, 'Owned Kueue ClusterQueue is not active or is on hold')
        nodes = self.client.list('/api/v1/nodes')
        matches = [n for n in nodes if all(n.get('metadata', {}).get('labels', {}).get(k) == v for k, v in pool['spec']['nodeSelector'].items())]
        if not matches or any(allocation_mode(pool['spec']['resourceName'], n['metadata'].get('labels', {})) != 'whole-gpu' or quantity(n.get('status', {}).get('allocatable', {}).get(pool['spec']['resourceName'])) is None for n in matches):
            raise ApiError(409, 'Fresh whole-GPU eligibility could not be verified')
        if not any((quantity(n.get('status', {}).get('allocatable', {}).get(pool['spec']['resourceName'])) or 0) >= request.gpu_count for n in matches):
            raise ApiError(409, 'Requested GPUs exceed every matching node capacity; no single-node fit is possible')
        return profile, policy, pool, namespace

    def preview(self, project, request):
        profile, policy, pool, _ = self.preflight(project, request)
        return {'manifest': build_job(project, profile, request, policy, pool, 'preview-only'), 'submitted': False,
                'checks': ['project-scope', 'pinned-identities', 'restricted-profile', 'native-guardrails', 'active-kueue', 'fresh-whole-gpu-inventory'],
                'limitations': ['API objects were checked, not CNI enforcement or scheduler admission. No resources reserved.']}

    def submit(self, project, identity, request, reservation_id=None):
        # Read-only retries resolve against durable ownership before rechecking
        # admission prerequisites; they never submit a second name automatically.
        digest = hashlib.sha256(canonical(request.model_dump()).encode()).hexdigest()
        record = self.registry(project, request.name)
        if record:
            if record.get('reservation_id') != reservation_id:
                raise ApiError(409, 'Workload name is reserved by a reassignment operation; advance that operation instead')
            if record['request_hash'] != digest:
                raise ApiError(409, 'Workload name already belongs to a different submission; use a new name')
            try:
                return {'workload': summarize(self.owned(project, request.name), project), 'submitted': False, 'allocated': False, 'idempotent': True}
            except ApiError as error:
                if error.status != 404:
                    raise
                if record['uid']:
                    raise ApiError(409, 'A previously submitted workload used this name; choose a new name') from None
        profile, policy, pool, namespace = self.preflight(project, request)
        op = record['operation_id'] if record else str(uuid.uuid4())
        job = build_job(project, profile, request, policy, pool, op)
        effective = {'manifest': build_job(project, profile, request, policy, pool, 'configuration-fingerprint'),
                     'project': project.model_dump(), 'policy': policy['spec'], 'pool': pool['spec']}
        desired_hash = hashlib.sha256(canonical(effective).encode()).hexdigest()
        if record and record.get('desired_hash') != desired_hash:
            raise ApiError(409, 'Effective execution configuration changed after the first attempt; use a new workload name')
        if not record:
            try:
                with self.store.lock, self.store.db:
                    self.store.db.execute('BEGIN IMMEDIATE')
                    count = self.store.db.execute('SELECT count(*) FROM execution_jobs WHERE project=?', (project.id,)).fetchone()[0]
                    if count >= project.history_limit:
                        raise ApiError(409, 'Project workload history limit reached; administrator archival is required before new submissions')
                    self.store.db.execute('INSERT INTO execution_jobs(project,name,operation_id,request_hash,uid,desired_hash) VALUES (?,?,?,?,NULL,?)', (project.id, request.name, op, digest, desired_hash))
            except sqlite3.IntegrityError:
                raise ApiError(409, 'Concurrent submission for this name; reload before retrying') from None
        audit_op = str(uuid.uuid4())
        self.audit(identity.id, project.id, 'submit', request.name, 'intent', audit_op)
        # Fence parent replacement or changed desired state immediately before
        # create. Kubernetes admission/quota remain the final authority.
        self.verify_namespace(project)
        for obj, path in ((policy, ROOT + '/namespaces/' + segment(project.namespace) + '/gpuaccesspolicies/' + segment(project.policy)), (pool, ROOT + '/gpupools/' + segment(pool['metadata']['name']))):
            current = self.client.get(path)
            if current['metadata'].get('uid') != obj['metadata']['uid'] or current['metadata'].get('generation', 1) != obj['metadata'].get('generation', 1) or current['metadata'].get('deletionTimestamp'):
                raise ApiError(409, 'Project guardrails changed during submission; preview again')
        try:
            created = self.client.request('POST', self.job_path(project), job)
        except ApiError as error:
            self.audit(identity.id, project.id, 'submit', request.name, 'uncertain' if error.status >= 500 else 'rejected', audit_op)
            raise
        try:
            self.record_uid(project, request.name, created['metadata']['uid'])
            self.audit(identity.id, project.id, 'submit', request.name, 'accepted', audit_op)
        except (sqlite3.Error, ApiError):
            raise ApiError(503, 'Kubernetes accepted this Job, but recording the result failed. Reload the same workload name before retrying; do not create a new name.') from None
        return {'workload': summarize(created, project), 'submitted': True, 'allocated': False, 'idempotent': False}

    def cancel(self, project, identity, name, preconditions, before_delete=None, after_delete=None):
        job = self.owned(project, name)
        if any(job['metadata'].get(k) != v for k, v in preconditions.model_dump().items()):
            raise ApiError(409, 'Workload changed; reload its UID and resourceVersion before canceling')
        op = str(uuid.uuid4())
        self.audit(identity.id, project.id, 'cancel', name, 'intent', op)
        if before_delete is not None:
            before_delete()
        try:
            self.client.request('DELETE', self.job_path(project, name), {'apiVersion': 'v1', 'kind': 'DeleteOptions', 'preconditions': preconditions.model_dump(), 'propagationPolicy': 'Foreground'})
        except ApiError as error:
            self.audit(identity.id, project.id, 'cancel', name, 'uncertain' if error.status >= 500 else 'rejected', op)
            raise
        if after_delete is not None:
            after_delete()
        try:
            self.audit(identity.id, project.id, 'cancel', name, 'accepted', op)
        except (sqlite3.Error, ApiError):
            raise ApiError(503, 'Kubernetes accepted deletion, but recording the result failed. Reload the same workload before retrying.') from None
        return {'deletion_requested': True, 'deleted': False, 'workload': summarize(job, project)}
