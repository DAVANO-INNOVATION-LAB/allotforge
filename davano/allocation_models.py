"""Narrow administrator input for namespace GPU allocation desired state."""
from pydantic import Field, StrictInt
from .control_models import DeletePreconditions
from .models import ID_PATTERN


class AllocationUpdate(DeletePreconditions):
    poolRef: str = Field(pattern=ID_PATTERN, max_length=63)
    gpuQuota: StrictInt = Field(ge=0, le=100000)
    maxGPUsPerPod: StrictInt = Field(ge=1, le=1024)
