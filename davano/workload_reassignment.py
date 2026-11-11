"""Durable queued-Job replacement; never live migration or device relabeling."""
from datetime import datetime, timezone
import hashlib
import json
import sqlite3
import threading
import uuid
from .control_models import DeletePreconditions
from .kube import ApiError, segment
from .reconciler import KUEUE
from .workload_models import WorkloadCreate
from .workload_service import build_job, canonical, summarize, stamp

WARNINGS = [
    'This cancels the source and creates a new queued Job; it does not move physical GPUs or preserve process, filesystem or checkpoint state.',
    'Only currently suspended Jobs without execution or admission evidence are eligible. Running or admitted workloads must be handled separately.',
    'Source deletion and target submission are not atomic. Target failure after source deletion leaves the operation resumable; no automatic rollback or substitute workload is created.',
    'CPU and memory change only by selecting an approved target profile. GPU requests remain whole-GPU units, not device identity or MIG configuration.',
]


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


class WorkloadReassignments:
    def __init__(self, execution):
        self.execution = execution
        self.store, self.client = execution.store, execution.client
        self.lock = threading.RLock()
        with self.store.lock, self.store.db:
            self.store.db.executescript('''
            CREATE TABLE IF NOT EXISTS execution_reassignments (
                id TEXT PRIMARY KEY, source_project TEXT NOT NULL, source_name TEXT NOT NULL,
                source_uid TEXT NOT NULL, target_project TEXT NOT NULL, target_name TEXT NOT NULL,
                input_hash TEXT NOT NULL, plan TEXT NOT NULL, phase TEXT NOT NULL,
                source_deleted INTEGER NOT NULL DEFAULT 0, deletion_state TEXT NOT NULL DEFAULT 'not-requested', target_submitted INTEGER NOT NULL DEFAULT 0,
                detail TEXT NOT NULL, workload TEXT, updated_at TEXT NOT NULL,
                UNIQUE(source_project,source_uid));
            ''')
            if 'deletion_state' not in {r[1] for r in self.store.db.execute('PRAGMA table_info(execution_reassignments)')}:
                self.store.db.execute("ALTER TABLE execution_reassignments ADD COLUMN deletion_state TEXT NOT NULL DEFAULT 'not-requested'")

    def row(self, operation_id):
        with self.store.lock:
            result = self.store.db.execute('SELECT * FROM execution_reassignments WHERE id=?', (operation_id,))
            row = result.fetchone()
            if row is None:
                raise ApiError(404, 'Reassignment operation not found')
            return dict(zip([c[0] for c in result.description], row))

    @staticmethod
    def public(row):
        return {k: row[k] for k in ('id', 'phase', 'source_project', 'source_name', 'target_project', 'target_name', 'detail', 'updated_at')} | {
            'source_deleted': bool(row['source_deleted']), 'source_deletion_state': row['deletion_state'], 'target_submitted': bool(row['target_submitted']),
            'workload': json.loads(row['workload']) if row['workload'] else None,
            'semantics': 'cancel-and-recreate', 'allocated': False}

    def set_phase(self, operation_id, phase, detail, source_deleted=None, workload=None):
        with self.store.lock, self.store.db:
            self.store.db.execute('UPDATE execution_reassignments SET phase=?,detail=?,updated_at=? WHERE id=? AND target_submitted=0', (phase, detail, stamp(), operation_id))
            if source_deleted is not None:
                self.store.db.execute('UPDATE execution_reassignments SET source_deleted=? WHERE id=?', (int(source_deleted), operation_id))
            if workload is not None:
                self.store.db.execute('UPDATE execution_reassignments SET workload=?,target_submitted=1,phase=? WHERE id=?', (canonical(workload), 'TargetSubmitted', operation_id))
        return self.public(self.row(operation_id))

    def mark_deletion(self, operation_id, state):
        with self.store.lock, self.store.db:
            self.store.db.execute('UPDATE execution_reassignments SET deletion_state=?,updated_at=? WHERE id=?', (state, stamp(), operation_id))

    @staticmethod
    def owned_children(items, source_uid):
        return [item for item in items if any(ref.get('uid') == source_uid and ref.get('kind') == 'Job' for ref in item.get('metadata', {}).get('ownerReferences', []))]

    def children(self, project, source_uid):
        pods = self.client.list('/api/v1/namespaces/' + segment(project.namespace) + '/pods')
        workloads = self.client.list(KUEUE + '/namespaces/' + segment(project.namespace) + '/workloads')
        return self.owned_children(pods, source_uid), self.owned_children(workloads, source_uid)

    def source_eligible(self, project, name, uid, resource_version):
        job = self.execution.owned(project, name)
        meta, status = job['metadata'], job.get('status', {})
        if meta.get('uid') != uid or meta.get('resourceVersion') != resource_version or meta.get('deletionTimestamp'):
            raise ApiError(409, 'Source Job identity or revision changed; refresh and preview again')
        if job['spec'].get('suspend') is not True or status.get('startTime') or status.get('completionTime') or any(status.get(k, 0) for k in ('active', 'succeeded', 'failed', 'ready', 'terminating')):
            raise ApiError(409, 'Only suspended Jobs without execution evidence can be reassigned; running Jobs are not migrated')
        if any(c.get('type') != 'Suspended' or c.get('status') != 'True' for c in status.get('conditions', [])) or status.get('uncountedTerminatedPods', {}).get('succeeded') or status.get('uncountedTerminatedPods', {}).get('failed'):
            raise ApiError(409, 'Source Job has execution or terminal-condition history; replacement is not permitted')
        pods, workloads = self.children(project, uid)
        if pods:
            raise ApiError(409, 'Source Job still owns Pods; no automatic eviction or restart is performed')
        for workload in workloads:
            state = workload.get('status', {})
            if state.get('admission') or state.get('requeueState') or state.get('reclaimablePods') or state.get('admissionChecks') or any(c.get('type') not in ('Admitted', 'QuotaReserved') or c.get('status') != 'False' for c in state.get('conditions', [])):
                raise ApiError(409, 'Kueue admission, reservation or execution history blocks reassignment')
        return job

    def target_plan(self, target_project, request):
        profile, policy, pool, _ = self.execution.preflight(target_project, request)
        manifest = build_job(target_project, profile, request, policy, pool, 'preview-only')
        effective = {'manifest': build_job(target_project, profile, request, policy, pool, 'configuration-fingerprint'),
                     'project': target_project.model_dump(), 'policy': policy['spec'], 'pool': pool['spec']}
        return manifest, fingerprint(effective)

    def preview(self, source_project, name, target_project, body):
        if source_project.id == target_project.id and name == body.target.name:
            raise ApiError(422, 'Replacement in the same project requires a new workload name')
        job = self.source_eligible(source_project, name, body.uid, body.resourceVersion)
        if self.execution.registry(target_project, body.target.name) is not None:
            raise ApiError(409, 'Target workload name already exists or is reserved; choose a new name')
        try:
            self.client.get(self.execution.job_path(target_project, body.target.name))
        except ApiError as error:
            if error.status != 404:
                raise
        else:
            raise ApiError(409, 'Target namespace already contains that Job name; it cannot be adopted')
        manifest, target_hash = self.target_plan(target_project, body.target)
        plan = {'source_project': source_project.model_dump(), 'source_name': name, 'source_uid': body.uid,
                'source_resource_version': body.resourceVersion, 'source_spec': job['spec'],
                'target_project': target_project.model_dump(), 'target': body.target.model_dump(), 'target_hash': target_hash}
        return {'source': summarize(job, source_project), 'target': {'project': target_project.id, 'namespace': target_project.namespace, 'manifest': manifest},
                'plan_fingerprint': fingerprint(plan), 'eligible': True, 'semantics': 'cancel-and-recreate', 'warnings': WARNINGS, '_plan': plan}

    def create(self, source_project, name, target_project, identity, body):
        request_hash = fingerprint(body.model_dump())
        with self.lock:
            with self.store.lock:
                existing = self.store.db.execute('SELECT id,input_hash FROM execution_reassignments WHERE source_project=? AND source_uid=?', (source_project.id, body.uid)).fetchone()
            if existing:
                if existing[1] != request_hash:
                    raise ApiError(409, 'This source Job already has a different reassignment operation')
                return self.advance(existing[0], source_project, target_project, identity)
            preview = self.preview(source_project, name, target_project, body)
            if preview['plan_fingerprint'] != body.plan_fingerprint:
                raise ApiError(409, 'The reviewed reassignment plan changed; preview again')
            plan = preview['_plan']
            op = str(uuid.uuid4())
            target_op = str(uuid.uuid4())
            try:
                with self.store.lock, self.store.db:
                    self.store.db.execute('BEGIN IMMEDIATE')
                    count = self.store.db.execute('SELECT count(*) FROM execution_reassignments WHERE source_project=?', (source_project.id,)).fetchone()[0]
                    target_count = self.store.db.execute('SELECT count(*) FROM execution_jobs WHERE project=?', (target_project.id,)).fetchone()[0]
                    if count >= 1000 or target_count >= target_project.history_limit:
                        raise ApiError(409, 'Project reassignment or workload history limit reached; administrator archival is required')
                    self.store.db.execute('INSERT INTO execution_jobs(project,name,operation_id,request_hash,uid,desired_hash,reservation_id) VALUES (?,?,?,?,NULL,?,?)',
                                          (target_project.id, body.target.name, target_op, fingerprint(body.target.model_dump()), plan['target_hash'], op))
                    self.store.db.execute('INSERT INTO execution_reassignments(id,source_project,source_name,source_uid,target_project,target_name,input_hash,plan,phase,detail,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                                          (op, source_project.id, name, body.uid, target_project.id, body.target.name, request_hash, canonical(plan), 'Prepared', 'Reviewed replacement plan and target name reserved; source not yet changed', stamp()))
            except sqlite3.IntegrityError:
                raise ApiError(409, 'Source or target was concurrently reserved; reload before retrying') from None
            return self.advance(op, source_project, target_project, identity)

    def advance(self, operation_id, source_project, target_project, identity):
        # The supported broker deployment is one replica/one worker. The
        # durable target-name reservation and Kubernetes UID/RV protect retries.
        with self.lock:
            row = self.row(operation_id)
            if row['source_project'] != source_project.id or row['target_project'] != target_project.id:
                raise ApiError(404, 'Reassignment operation not found')
            if row['target_submitted']:
                return self.public(row)
            plan = json.loads(row['plan'])
            if row['source_deleted']:
                record = self.execution.registry(target_project, row['target_name'])
                if record and record.get('reservation_id') == operation_id:
                    try:
                        existing = self.execution.owned(target_project, row['target_name'])
                    except ApiError as error:
                        if error.status != 404:
                            return self.set_phase(operation_id, 'NeedsAttention', str(error))
                    else:
                        return self.set_phase(operation_id, 'TargetSubmitted', 'Previously accepted target Job recovered; no duplicate submission performed.', source_deleted=True, workload=summarize(existing, target_project))
            if source_project.model_dump() != plan['source_project'] or target_project.model_dump() != plan['target_project']:
                return self.set_phase(operation_id, 'NeedsAttention', 'Project configuration changed. No further mutation attempted; administrator review is required.')
            try:
                self.execution.verify_namespace(source_project)
                _, target_hash = self.target_plan(target_project, WorkloadCreate.model_validate(plan['target']))
                if target_hash != plan['target_hash']:
                    return self.set_phase(operation_id, 'NeedsAttention', 'Target execution configuration changed from the reviewed plan. No further mutation attempted.')
                target_record = self.execution.registry(target_project, row['target_name'])
                if not target_record or target_record.get('reservation_id') != operation_id:
                    raise ApiError(409, 'Target name reservation changed; refusing reassignment')
                if not row['source_deleted']:
                    try:
                        source = self.execution.owned(source_project, row['source_name'])
                    except ApiError as error:
                        if error.status != 404:
                            raise
                        source = None
                    authorized_deletion = row['deletion_state'] == 'accepted'
                    if source is None and not authorized_deletion:
                        raise ApiError(409, 'Source disappeared without this operation recording an accepted validated cancellation; execution history is uncertain and administrator review is required')
                    if source is not None:
                        # Only this operation's durable validated DELETE attempt
                        # can justify recovery from an absent/deleting source.
                        if source['metadata']['uid'] != row['source_uid']:
                            raise ApiError(409, 'Source UID changed; refusing replacement')
                        if source['metadata'].get('deletionTimestamp') and not authorized_deletion:
                            raise ApiError(409, 'Source was externally marked for deletion; reassignment cannot infer that it remained unexecuted')
                        if not source['metadata'].get('deletionTimestamp'):
                            self.source_eligible(source_project, row['source_name'], plan['source_uid'], plan['source_resource_version'])
                            try:
                                self.execution.cancel(source_project, identity, row['source_name'], DeletePreconditions(uid=plan['source_uid'], resourceVersion=plan['source_resource_version']),
                                                      before_delete=lambda: self.mark_deletion(operation_id, 'attempted'),
                                                      after_delete=lambda: self.mark_deletion(operation_id, 'accepted'))
                            except ApiError as error:
                                if error.status < 500:
                                    self.mark_deletion(operation_id, 'rejected')
                                raise
                            self.mark_deletion(operation_id, 'accepted')
                        self.set_phase(operation_id, 'WaitingForSourceDeletion', 'Source deletion requested. Target remains reserved and unsubmitted until source and owned children are absent.')
                    try:
                        self.client.get(self.execution.job_path(source_project, row['source_name']))
                    except ApiError as error:
                        if error.status != 404:
                            raise
                    else:
                        return self.set_phase(operation_id, 'WaitingForSourceDeletion', 'Waiting for the source Job to be absent; target not submitted.')
                    pods, workloads = self.children(source_project, row['source_uid'])
                    if pods or workloads:
                        return self.set_phase(operation_id, 'WaitingForSourceDeletion', 'Source Job is absent but owned Pods or Kueue Workloads remain. Target not submitted.')
                    self.set_phase(operation_id, 'SubmittingTarget', 'Source and its owned children are absent; submitting the reviewed target Job.', source_deleted=True)
                result = self.execution.submit(target_project, identity, WorkloadCreate.model_validate(plan['target']), reservation_id=operation_id)
                return self.set_phase(operation_id, 'TargetSubmitted', 'Replacement Job accepted by Kubernetes. Queue admission and GPU allocation are not confirmed.', source_deleted=True, workload=result['workload'])
            except ApiError as error:
                return self.set_phase(operation_id, 'NeedsAttention', str(error) + ' Resume only this same operation after checking the cause; do not create a substitute name.')
