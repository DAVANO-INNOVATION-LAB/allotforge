"""Opt-in project-token API for the constrained Kubernetes Job broker."""
import hmac
import json
import os
import sqlite3
from pathlib import Path
from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import ValidationError
from .control_models import DeletePreconditions
from .kube import ApiError, KubeClient
from .workload_models import ExecutionConfig, WorkloadCreate, WorkloadReassignmentPreview, WorkloadReassignmentApply, token_hash
from .workload_service import ExecutionService, summarize

LIMITATIONS = [
    'Only administrator-approved digest-pinned images and fixed commands can run; no arbitrary Job/Pod templates, credentials, mounts or environment variables.',
    'Project bearer tokens are statically provisioned identities, not OIDC/SSO. Use TLS and a reviewed private ingress; unscoped inventory/advisory endpoints require the separate administrator token while execution is enabled.',
    'Native Kubernetes Jobs, ResourceQuota and Kueue perform execution and admission. Submitted is not allocated or running.',
    'Preflight reads cannot prove CNI enforcement, physical GPU isolation, admission cache activation or immunity to administrator changes.',
    'No direct Kubernetes workload RBAC is granted to token holders. Existing permissions outside this broker remain outside its authority.',
    'Execution audit records are durable local events, not immutable external retention. Live GPU/Kubernetes execution still needs target-specific validation.',
]


def install_workload_api(app, source, store, client=None, config=None):
    enabled = os.getenv('DAVANO_EXECUTION_ENABLED', 'false').lower() == 'true'
    service = None
    if enabled:
        if source != 'kubernetes':
            raise ValueError('Execution API requires DAVANO_SOURCE=kubernetes')
        if config is None:
            path = os.getenv('DAVANO_EXECUTION_CONFIG_PATH', '')
            if not path:
                raise ValueError('Execution API requires DAVANO_EXECUTION_CONFIG_PATH')
            try:
                raw = Path(path).read_bytes()
                if len(raw) > 1024 * 1024:
                    raise ValueError('Oversized execution config')
                config = ExecutionConfig.model_validate(json.loads(raw))
            except (OSError, ValueError, ValidationError):
                raise ValueError('Execution configuration could not be read or validated; credentials are never echoed') from None
        else:
            config = ExecutionConfig.model_validate(config)
        admin_token = os.getenv('DAVANO_API_TOKEN', '')
        if admin_token and any(hmac.compare_digest(token_hash(admin_token), identity.token_sha256) for identity in config.identities):
            raise ValueError('Administrator and project execution credentials must be distinct')
        service = ExecutionService(client or KubeClient(), store, config)
    from .workload_reassignment import WorkloadReassignments
    reassignments = WorkloadReassignments(service) if service else None
    router = APIRouter(prefix='/api/v1/execution', tags=['Constrained project execution'])

    def authorize(authorization: str | None = Header(default=None)):
        if not enabled:
            raise HTTPException(409, 'Constrained execution is disabled; no cluster mutation attempted')
        supplied = authorization[7:] if authorization and authorization.startswith('Bearer ') else ''
        digest = token_hash(supplied)
        found = None
        # Compare all configured hashes, including invalid-length credentials.
        for identity in config.identities:
            if hmac.compare_digest(digest, identity.token_sha256):
                found = identity
        if not 32 <= len(supplied) <= 4096 or found is None:
            raise HTTPException(401, 'A valid project Bearer token is required', headers={'WWW-Authenticate': 'Bearer'})
        return found

    def project_for(project_id, identity, mutate=False):
        if project_id not in identity.projects:
            raise HTTPException(404, 'Project not found')
        if mutate and identity.role != 'submitter':
            raise HTTPException(403, 'Project identity has read-only access')
        return next(p for p in config.projects if p.id == project_id)

    def call(method, *args):
        try:
            return method(*args)
        except ApiError as error:
            raise HTTPException(error.status if error.status in (400, 403, 404, 409, 422) else 503, str(error)) from None
        except sqlite3.Error:
            raise HTTPException(503, 'Execution persistence unavailable. A previous mutation may have succeeded; reload the same workload name before retrying.') from None
        except (KeyError, TypeError, ValueError, AttributeError):
            raise HTTPException(503, 'Execution prerequisite or Kubernetes response could not be validated; no automatic retry was made') from None

    @router.get('/status')
    def status():
        return {'enabled': enabled, 'mode': 'kubernetes' if enabled else 'disabled', 'authentication': 'project-bearer',
                'profiles': 'administrator-allowlist', 'automatic_tenant_rbac': False, 'live_validation': 'not-performed', 'limitations': LIMITATIONS}

    @router.get('/projects')
    def projects(identity=Depends(authorize)):
        return {'items': [{'id': p.id, 'namespace': p.namespace, 'can_submit': identity.role == 'submitter',
                           'profiles': [x.model_dump(exclude={'command'}) for x in config.profiles if x.id in p.profiles]}
                          for p in config.projects if p.id in identity.projects]}

    @router.get('/projects/{project_id}/workloads')
    def workloads(project_id: str, identity=Depends(authorize)):
        return {'items': call(service.list, project_for(project_id, identity)), 'source': 'kubernetes'}

    @router.get('/projects/{project_id}/workloads/{name}')
    def workload(project_id: str, name: str, identity=Depends(authorize)):
        project = project_for(project_id, identity)
        return call(lambda: summarize(service.owned(project, name), project))

    @router.post('/projects/{project_id}/preview')
    def preview(project_id: str, body: WorkloadCreate, identity=Depends(authorize)):
        return call(service.preview, project_for(project_id, identity), body)

    @router.post('/projects/{project_id}/workloads', status_code=201)
    def submit(project_id: str, body: WorkloadCreate, identity=Depends(authorize)):
        return call(service.submit, project_for(project_id, identity, True), identity, body)

    @router.post('/projects/{project_id}/workloads/{name}/cancel')
    def cancel(project_id: str, name: str, body: DeletePreconditions, identity=Depends(authorize)):
        return call(service.cancel, project_for(project_id, identity, True), identity, name, body)

    @router.post('/projects/{project_id}/workloads/{name}/reassignment/preview')
    def preview_reassignment(project_id: str, name: str, body: WorkloadReassignmentPreview, identity=Depends(authorize)):
        source_project = project_for(project_id, identity)
        target_project = project_for(body.target_project, identity)
        result = call(reassignments.preview, source_project, name, target_project, body)
        return {k: v for k, v in result.items() if k != '_plan'}

    @router.post('/projects/{project_id}/workloads/{name}/reassignments', status_code=202)
    def begin_reassignment(project_id: str, name: str, body: WorkloadReassignmentApply, identity=Depends(authorize)):
        return call(reassignments.create, project_for(project_id, identity, True), name, project_for(body.target_project, identity, True), identity, body)

    def operation_scope(operation_id, identity, mutate=False):
        row = call(reassignments.row, operation_id)
        source_project = project_for(row['source_project'], identity, mutate)
        target_project = project_for(row['target_project'], identity, mutate)
        return row, source_project, target_project

    @router.get('/reassignments')
    def list_reassignments(identity=Depends(authorize)):
        def read():
            with store.lock:
                placeholders = ','.join('?' for _ in identity.projects)
                sql = 'SELECT id FROM execution_reassignments WHERE source_project IN (' + placeholders + ') AND target_project IN (' + placeholders + ') ORDER BY updated_at DESC LIMIT 100'
                ids = [r[0] for r in store.db.execute(sql, identity.projects + identity.projects)]
            visible = []
            for operation_id in ids:
                row = reassignments.row(operation_id)
                if row['source_project'] in identity.projects and row['target_project'] in identity.projects:
                    visible.append(reassignments.public(row))
                    if len(visible) >= 100:
                        break
            return {'items': visible}
        return call(read)

    @router.get('/reassignments/{operation_id}')
    def reassignment(operation_id: str, identity=Depends(authorize)):
        row, _, _ = operation_scope(operation_id, identity)
        return reassignments.public(row)

    @router.post('/reassignments/{operation_id}/advance')
    def advance_reassignment(operation_id: str, identity=Depends(authorize)):
        _, source_project, target_project = operation_scope(operation_id, identity, True)
        return call(reassignments.advance, operation_id, source_project, target_project, identity)

    @router.get('/projects/{project_id}/audit')
    def audit(project_id: str, identity=Depends(authorize)):
        project_for(project_id, identity)
        def read():
            with store.lock:
                rows = store.db.execute('SELECT id,at,actor,operation,workload,outcome,operation_id FROM execution_audit WHERE project=? ORDER BY id DESC LIMIT 100', (project_id,))
                return {'items': [dict(zip(('id', 'at', 'actor', 'operation', 'workload', 'outcome', 'operation_id'), row)) for row in rows], 'retention': 'local-not-immutable'}
        return call(read)

    app.include_router(router)
    app.state.execution = service
    app.state.reassignments = reassignments
    return service
