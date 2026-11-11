"""Administrator configuration and deliberately narrow tenant execution inputs."""
import hashlib
from typing import Literal
from pydantic import Field, StrictInt, field_validator, model_validator
from .models import StrictModel, ID_PATTERN

UID = r'^[a-zA-Z0-9-]+$'

class ExecutionProfile(StrictModel):
    id: str = Field(pattern=ID_PATTERN, max_length=63)
    label: str = Field(min_length=1, max_length=120)
    image: str = Field(max_length=512, pattern=r'^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$')
    command: list[str] = Field(min_length=1, max_length=32)
    cpu_millicores: StrictInt = Field(ge=100, le=128000)
    memory_mib: StrictInt = Field(ge=128, le=524288)
    gpu_max: StrictInt = Field(ge=1, le=64)
    max_runtime_seconds: StrictInt = Field(ge=30, le=86400)
    @field_validator('command')
    @classmethod
    def bounded_command(cls, value):
        if any(not item or len(item) > 2048 or '\x00' in item for item in value):
            raise ValueError('Profile commands must be nonempty bounded strings')
        return value

class ExecutionProject(StrictModel):
    id: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace_uid: str = Field(pattern=UID, min_length=1, max_length=100)
    policy: str = Field(pattern=ID_PATTERN, max_length=63)
    policy_uid: str = Field(pattern=UID, min_length=1, max_length=100)
    pool_uid: str = Field(pattern=UID, min_length=1, max_length=100)
    service_account: str = Field(pattern=ID_PATTERN, max_length=63)
    service_account_uid: str = Field(pattern=UID, min_length=1, max_length=100)
    network_policy: str = Field(pattern=ID_PATTERN, max_length=63)
    profiles: list[str] = Field(min_length=1, max_length=32)
    execution_quota: str = Field(default='davano-execution-bounds', pattern=ID_PATTERN, max_length=63)
    max_jobs: StrictInt = Field(default=20, ge=1, le=100)
    history_limit: StrictInt = Field(default=1000, ge=1, le=1000)
    @model_validator(mode='after')
    def dedicated_namespace(self):
        if self.namespace in ('default', 'davano') or self.namespace.startswith(('kube-', 'openshift')):
            raise ValueError('Execution requires a dedicated namespace')
        if len(set(self.profiles)) != len(self.profiles):
            raise ValueError('Duplicate project profile')
        return self

class ExecutionIdentity(StrictModel):
    id: str = Field(pattern=ID_PATTERN, max_length=63)
    token_sha256: str = Field(pattern=r'^[a-f0-9]{64}$')
    projects: list[str] = Field(min_length=1, max_length=100)
    role: Literal['viewer', 'submitter'] = 'viewer'

class ExecutionConfig(StrictModel):
    profiles: list[ExecutionProfile] = Field(min_length=1, max_length=100)
    projects: list[ExecutionProject] = Field(min_length=1, max_length=100)
    identities: list[ExecutionIdentity] = Field(min_length=1, max_length=1000)
    @model_validator(mode='after')
    def unique_references(self):
        for collection in (self.profiles, self.projects, self.identities):
            if len({item.id for item in collection}) != len(collection):
                raise ValueError('Execution IDs must be unique within each collection')
        if len({item.namespace for item in self.projects}) != len(self.projects):
            raise ValueError('Execution projects must use separate dedicated namespaces')
        if len({item.token_sha256 for item in self.identities}) != len(self.identities):
            raise ValueError('Execution identities must have separate credentials')
        profiles, projects = {p.id for p in self.profiles}, {p.id for p in self.projects}
        if any(set(p.profiles) - profiles for p in self.projects) or any(set(i.projects) - projects for i in self.identities):
            raise ValueError('Unknown execution profile or project reference')
        return self

class WorkloadCreate(StrictModel):
    name: str = Field(pattern=ID_PATTERN, max_length=50)
    profile: str = Field(pattern=ID_PATTERN, max_length=63)
    gpu_count: StrictInt = Field(ge=1, le=64)


def token_hash(value):
    return hashlib.sha256(value.encode('utf-8')).hexdigest()


class WorkloadReassignmentPreview(StrictModel):
    uid: str = Field(min_length=1, max_length=100, pattern=UID)
    resourceVersion: str = Field(min_length=1, max_length=100, pattern=r'^[0-9]+$')
    target_project: str = Field(pattern=ID_PATTERN, max_length=63)
    target: WorkloadCreate

class WorkloadReassignmentApply(WorkloadReassignmentPreview):
    plan_fingerprint: str = Field(pattern=r'^[a-f0-9]{64}$')
