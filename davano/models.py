from typing import Literal
import re
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

ID_PATTERN = r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$"
Vendor = Literal["NVIDIA", "AMD", "Intel", "Any"]

class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

class PoolCreate(StrictModel):
    id: str = Field(pattern=ID_PATTERN, max_length=63)
    name: str = Field(min_length=1, max_length=80)
    description: str = Field(default="", max_length=500)
    selector: dict[str, str] = Field(min_length=1, max_length=20)
    vendor: Vendor = "Any"
    mode: Literal["whole-gpu"] = "whole-gpu"

    @field_validator("selector")
    @classmethod
    def validate_selector(cls, value):
        for key, val in value.items():
            pieces = key.split("/")
            name = pieces[-1]
            if len(pieces) > 2 or len(name) > 63 or not re.fullmatch(r"[A-Za-z0-9](?:[-_.A-Za-z0-9]*[A-Za-z0-9])?", name):
                raise ValueError("selector keys must be Kubernetes label keys")
            if len(pieces) == 2:
                prefix = pieces[0]
                if len(prefix) > 253 or not prefix or any(len(part) > 63 or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", part) for part in prefix.split(".")):
                    raise ValueError("selector prefixes must be DNS subdomains")
            if len(val) > 63 or (val and not re.fullmatch(r"[A-Za-z0-9](?:[-_.A-Za-z0-9]*[A-Za-z0-9])?", val)):
                raise ValueError("selector values must be Kubernetes label values")
        return value

class PolicyCreate(StrictModel):
    id: str = Field(pattern=ID_PATTERN, max_length=63)
    name: str = Field(min_length=1, max_length=80)
    pool_id: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    max_gpus_per_workload: StrictInt = Field(ge=1, le=1024)
    priority: Literal["low", "normal", "high"] = "normal"

class EvaluationRequest(StrictModel):
    pool_id: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    gpu_count: StrictInt = Field(ge=1, le=1024)
    vendor: Vendor = "Any"
