"""Validated public control-plane input. The Kubernetes CRDs repeat these constraints."""
from typing import Literal
from pydantic import Field, StrictInt, field_validator, model_validator
from .models import StrictModel, ID_PATTERN, PoolCreate

RESOURCE_NAMES = ('nvidia.com/gpu', 'amd.com/gpu', 'gpu.intel.com/i915', 'gpu.intel.com/xe')
PROTECTED_PREFIX = '.node-restriction.kubernetes.io/'

class PoolSpec(StrictModel):
    nodeSelector: dict[str, str] = Field(min_length=1, max_length=8)
    resourceName: Literal['nvidia.com/gpu', 'amd.com/gpu', 'gpu.intel.com/i915', 'gpu.intel.com/xe']
    nominalQuota: StrictInt = Field(ge=0, le=100000)
    mode: Literal['whole-gpu'] = 'whole-gpu'
    @field_validator('nodeSelector')
    @classmethod
    def selector(cls, value):
        PoolCreate.validate_selector(value)
        if not any(PROTECTED_PREFIX in k or k.startswith('node-restriction.kubernetes.io/') for k in value):
            raise ValueError('A NodeRestriction-protected pool selector label is required')
        return value

class ManagedPoolCreate(StrictModel):
    name: str = Field(pattern=ID_PATTERN, max_length=63)
    spec: PoolSpec

class Subject(StrictModel):
    kind: Literal['User', 'Group', 'ServiceAccount']
    name: str = Field(min_length=1, max_length=253, pattern=r'^[^\s]+$')
    namespace: str | None = Field(default=None, pattern=ID_PATTERN, max_length=63)
    @model_validator(mode='after')
    def safe_subject(self):
        if self.kind == 'Group' and self.name in ('system:authenticated', 'system:unauthenticated'):
            raise ValueError('Broad authenticated/unauthenticated groups are forbidden')
        if self.kind == 'User' and self.name.startswith('system:'):
            raise ValueError('System users are not valid tenant subjects')
        if self.kind != 'ServiceAccount' and self.namespace is not None:
            raise ValueError('Only ServiceAccounts have namespaces')
        return self

class AccessSpec(StrictModel):
    poolRef: str = Field(pattern=ID_PATTERN, max_length=63)
    gpuQuota: StrictInt = Field(ge=0, le=100000)
    maxGPUsPerPod: StrictInt = Field(ge=1, le=1024)
    subjects: list[Subject] = Field(min_length=1, max_length=100)

class ManagedPolicyCreate(StrictModel):
    name: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    spec: AccessSpec
    @model_validator(mode='after')
    def safe_namespace(self):
        if self.namespace in ('default', 'davano') or self.namespace.startswith(('kube-', 'openshift')):
            raise ValueError('Use a dedicated tenant namespace')
        for subject in self.spec.subjects:
            if subject.kind == 'ServiceAccount' and subject.namespace not in (None, self.namespace):
                raise ValueError('ServiceAccount subjects must belong to the managed namespace')
        return self

class DeletePreconditions(StrictModel):
    uid: str = Field(min_length=1, max_length=100, pattern=r'^[a-zA-Z0-9-]+$')
    resourceVersion: str = Field(min_length=1, max_length=100, pattern=r'^[0-9]+$')
