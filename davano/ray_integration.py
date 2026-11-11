"""Deterministic Kueue/KubeRay manifests. Building a manifest never submits it.

KubeRay compatibility is pinned to v1.7.2. In particular that version does not
recognize ``nvidia.com/gpu.shared`` as a GPU resource, so Ray logical GPU counts
are explicit. Logical GPU units are NOT evidence of memory/fault isolation.
"""
from copy import deepcopy
import hashlib
import re
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import Field, StrictInt, field_validator, model_validator

from .inventory import gpu_vendor
from .models import ID_PATTERN, PoolCreate, StrictModel

KUBERAY_VERSION = "1.7.2"
KUEUE_API_VERSION = "kueue.x-k8s.io/v1beta1"
MODES = ("whole-gpu", "shared", "mig")
WHOLE_RESOURCES = {"nvidia.com/gpu", "amd.com/gpu", "gpu.intel.com/i915", "gpu.intel.com/xe"}
MIG_RESOURCE = re.compile(r"nvidia\.com/mig-[1-9][0-9]*g\.[1-9][0-9]*gb")


class RayPlanError(ValueError):
    """A known constraint prevents a trustworthy manifest from being generated."""


def generated_name(prefix, *parts):
    """Stable collision-resistant DNS label, including when inputs share prefixes."""
    raw = ":".join(parts)
    suffix = hashlib.sha256(raw.encode()).hexdigest()[:10]
    return f"{prefix}-{('-'.join(parts))[: 63 - len(prefix) - len(suffix) - 2].rstrip('-')}-{suffix}"


class RayJobRequest(StrictModel):
    name: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    pool_id: str = Field(pattern=ID_PATTERN, max_length=63)
    image: str = Field(min_length=1, max_length=512)
    ray_version: str = Field(pattern=r"^[1-9][0-9]*\.[0-9]+\.[0-9]+$", max_length=32)
    entrypoint: str = Field(min_length=1, max_length=4096)
    workers: StrictInt = Field(default=1, ge=1, le=1024)
    gpus_per_worker: StrictInt = Field(default=1, ge=1, le=128)
    resource_name: str = Field(default="nvidia.com/gpu", min_length=1, max_length=253)
    mode: Literal["whole-gpu", "shared", "mig"] = "whole-gpu"
    worker_cpu: StrictInt = Field(default=2, ge=1, le=1024)
    worker_memory_gib: StrictInt = Field(default=8, ge=2, le=16384)
    head_cpu: StrictInt = Field(default=1, ge=1, le=128)
    head_memory_gib: StrictInt = Field(default=2, ge=2, le=1024)
    active_deadline_seconds: StrictInt = Field(default=3600, ge=60, le=604800)

    @field_validator("image")
    @classmethod
    def pinned_image(cls, value):
        # Validate the supported OCI-reference subset without accepting a URL,
        # credentials, shell syntax, empty path components or a bogus digest.
        reference, marker, digest = value.partition("@")
        if marker and not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
            raise ValueError("image digest must be sha256 followed by 64 lowercase hex characters")
        pieces = reference.split("/")
        leaf, colon, tag = pieces[-1].partition(":")
        if colon:
            if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", tag):
                raise ValueError("image has an invalid version tag")
            pieces[-1] = leaf
        if not marker and (not colon or tag.lower() == "latest"):
            raise ValueError("image requires an explicit non-latest version tag or sha256 digest")
        if len(pieces) > 1 and ":" in pieces[0]:
            host, port_marker, port = pieces[0].partition(":")
            if not re.fullmatch(r"[a-z0-9]+(?:[.-][a-z0-9]+)*", host) or not port.isascii() or not port.isdigit() or not 1 <= int(port) <= 65535:
                raise ValueError("image has an invalid registry host or port")
            pieces[0] = host
        component = r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*"
        if not all(re.fullmatch(component, piece) for piece in pieces):
            raise ValueError("image must be a container reference with lowercase repository components")
        return value

    @field_validator("entrypoint")
    @classmethod
    def one_line_entrypoint(cls, value):
        if any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("entrypoint must be a single line without control characters")
        return value

    @model_validator(mode="after")
    def resource_mode(self):
        validate_resource_mode(self.resource_name, self.mode)
        if self.mode == "shared" and self.gpus_per_worker != 1:
            raise ValueError("shared GPU requests are limited to one replica per worker; replicas do not multiply guaranteed GPU capacity")
        if self.workers * self.gpus_per_worker > 65536:
            raise ValueError("total requested GPU units exceeds the integration limit")
        return self


def validate_resource_mode(resource_name, mode):
    valid = (
        (mode == "whole-gpu" and resource_name in WHOLE_RESOURCES)
        or (mode == "shared" and resource_name == "nvidia.com/gpu.shared")
        or (mode == "mig" and bool(MIG_RESOURCE.fullmatch(resource_name)))
    )
    if not valid:
        raise ValueError("resource_name and mode must be an explicitly supported pair; unknown, unrenamed shared and ambiguous MIG resources are not supported")


class QueueBundleRequest(StrictModel):
    """An explicit quota, never inferred from a per-workload limit or free GPUs."""
    pool_id: str = Field(pattern=ID_PATTERN, max_length=63)
    namespace: str = Field(pattern=ID_PATTERN, max_length=63)
    node_selector: dict[str, str] = Field(min_length=1, max_length=8)
    resource_name: str = "nvidia.com/gpu"
    mode: Literal["whole-gpu", "shared", "mig"] = "whole-gpu"
    nominal_gpu_quota: StrictInt = Field(ge=1, le=65536)
    nominal_cpu_quota: StrictInt = Field(ge=1, le=1048576)
    nominal_memory_gib: StrictInt = Field(ge=2, le=16777216)

    @field_validator("node_selector")
    @classmethod
    def selector(cls, value):
        return PoolCreate.validate_selector(value)

    @model_validator(mode="after")
    def resource_mode(self):
        validate_resource_mode(self.resource_name, self.mode)
        return self


def build_kueue_bundle(request: QueueBundleRequest):
    """Build a namespace-scoped allocation with no cohort borrowing or writes.

    Separate CPU/memory and GPU flavors keep the CPU head off the GPU selector.
    Kueue quotas apply only to Kueue-managed workloads, not arbitrary pods.
    """
    request = QueueBundleRequest.model_validate(request)
    queue = generated_name("davano", request.pool_id, request.namespace)
    flavor = generated_name("davano-gpu", request.pool_id, request.namespace)
    compute = generated_name("davano-cpu", request.pool_id, request.namespace)
    labels = {"app.kubernetes.io/managed-by": "davano", "davano.io/pool": request.pool_id}
    def metadata(name, namespace=None):
        result = {"name": name, "labels": deepcopy(labels)}
        if namespace:
            result["namespace"] = namespace
        return result
    return [
        {"apiVersion": KUEUE_API_VERSION, "kind": "ResourceFlavor", "metadata": metadata(flavor), "spec": {"nodeLabels": dict(request.node_selector)}},
        {"apiVersion": KUEUE_API_VERSION, "kind": "ResourceFlavor", "metadata": metadata(compute), "spec": {}},
        {"apiVersion": KUEUE_API_VERSION, "kind": "ClusterQueue", "metadata": metadata(queue), "spec": {
            "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": request.namespace}},
            "queueingStrategy": "BestEffortFIFO",
            "resourceGroups": [
                {"coveredResources": [request.resource_name], "flavors": [{"name": flavor, "resources": [{"name": request.resource_name, "nominalQuota": request.nominal_gpu_quota}]}]},
                {"coveredResources": ["cpu", "memory"], "flavors": [{"name": compute, "resources": [
                    {"name": "cpu", "nominalQuota": str(request.nominal_cpu_quota)},
                    {"name": "memory", "nominalQuota": f"{request.nominal_memory_gib}Gi"},
                ]}]},
            ],
        }},
        {"apiVersion": KUEUE_API_VERSION, "kind": "LocalQueue", "metadata": metadata(queue, request.namespace), "spec": {"clusterQueue": queue}},
    ]


def _pool_contract(pool):
    if "spec" in pool:
        return {
            "id": pool.get("metadata", {}).get("name"),
            "selector": pool["spec"].get("nodeSelector", {}),
            "resource_name": pool["spec"].get("resourceName"),
            "mode": pool["spec"].get("mode", "whole-gpu"),
            "vendor": "Any",
            "quota": pool["spec"].get("nominalQuota"),
        }
    return {**pool, "resource_name": pool.get("resource_name"), "quota": None}


def policy_limits(request, pool, policies):
    """Check unit/mode/namespace caps; never acts as identity authorization.

    Kubernetes policies must already have been resolved by an authenticated
    caller. This pure function does not authorize subjects or assert Ready.
    """
    pool = _pool_contract(pool)
    if pool.get("id") != request.pool_id:
        raise RayPlanError("Pool does not match the requested pool_id")
    if pool.get("mode") != request.mode:
        raise RayPlanError("Requested allocation mode does not match the pool")
    if pool.get("resource_name") and pool["resource_name"] != request.resource_name:
        raise RayPlanError("Requested resource_name does not match the pool")
    if pool.get("vendor", "Any") not in ("Any", gpu_vendor(request.resource_name)):
        raise RayPlanError("Requested GPU resource vendor does not match the pool")
    try:
        PoolCreate.validate_selector(pool.get("selector", {}))
    except ValueError as error:
        raise RayPlanError(str(error)) from error
    if not pool.get("selector"):
        raise RayPlanError("Pool must have a non-empty node selector")
    matching, caps, pod_caps, quotas = [], [], [], []
    for policy in policies:
        if "spec" in policy:
            spec, meta = policy["spec"], policy.get("metadata", {})
            if spec.get("poolRef") != request.pool_id or meta.get("namespace") != request.namespace:
                continue
            cap, quota, identity = spec.get("maxGPUsPerWorkload"), spec.get("gpuQuota"), meta.get("name")
            pod_cap = spec.get("maxGPUsPerPod")
            if type(pod_cap) is not int or pod_cap < 1:
                raise RayPlanError("Matching policy contains an invalid per-pod GPU limit")
            pod_caps.append(pod_cap)
        else:
            if policy.get("pool_id") != request.pool_id or policy.get("namespace") != request.namespace:
                continue
            cap, quota, identity = policy.get("max_gpus_per_workload"), None, policy.get("id")
        if cap is not None and (type(cap) is not int or cap < 1):
            raise RayPlanError("Matching policy contains an invalid per-workload GPU limit")
        if cap is None and "spec" not in policy:
            raise RayPlanError("Matching advisory policy is missing its per-workload GPU limit")
        if quota is not None and (type(quota) is not int or quota < 0):
            raise RayPlanError("Matching policy contains an invalid GPU quota")
        matching.append(identity)
        if cap is not None:
            caps.append(cap)
        if quota is not None:
            quotas.append(quota)
    if not matching:
        raise RayPlanError("No matching policy for this pool and namespace")
    total = request.workers * request.gpus_per_worker
    cap = min(caps) if caps else None
    if pod_caps and request.gpus_per_worker > min(pod_caps):
        raise RayPlanError(f"GPU units per worker exceeds the strictest per-pod policy limit ({min(pod_caps)})")
    if cap is not None and total > cap:
        raise RayPlanError(f"Total requested GPU units ({total}) exceeds the strictest per-workload policy limit ({cap})")
    if pool.get("quota") is not None:
        if type(pool["quota"]) is not int or pool["quota"] < 0:
            raise RayPlanError("Pool contains an invalid nominal GPU quota")
        quotas.append(pool["quota"])
    if quotas and total > min(quotas):
        raise RayPlanError(f"Total requested GPU units ({total}) exceeds the configured quota ({min(quotas)})")
    return {"policy_ids": matching, "per_workload_cap": cap, "per_pod_cap": min(pod_caps) if pod_caps else None, "total_gpu_units": total}


def build_suspended_ray_job(request: RayJobRequest, pool, policies, queue_name):
    """Construct the allowed RayJob template after deterministic quota checks.

    The caller is responsible for authn/authz, policy readiness, observed
    capability checks, queue ownership and an opt-in write before submission.
    Never unsuspend a Kueue-managed job here: admission belongs to Kueue.
    """
    request = RayJobRequest.model_validate(request)
    if not isinstance(queue_name, str) or not re.fullmatch(ID_PATTERN, queue_name):
        raise RayPlanError("queue_name must be a valid non-empty DNS label")
    policy_limits(request, pool, policies)
    pool = _pool_contract(pool)
    labels = {"app.kubernetes.io/managed-by": "davano", "davano.io/pool": request.pool_id, "davano.io/ray-job": request.name}
    security = {"runAsNonRoot": True, "allowPrivilegeEscalation": False, "capabilities": {"drop": ["ALL"]}, "seccompProfile": {"type": "RuntimeDefault"}}
    def pod_template(name, cpu, memory, gpu=False):
        resources = {"cpu": str(cpu), "memory": f"{memory}Gi"}
        if gpu:
            resources[request.resource_name] = request.gpus_per_worker
        container = {
            "name": name, "image": request.image, "imagePullPolicy": "IfNotPresent",
            "resources": {"requests": deepcopy(resources), "limits": deepcopy(resources)},
            "securityContext": deepcopy(security),
        }
        if not gpu:
            container["env"] = [{"name": "NVIDIA_VISIBLE_DEVICES", "value": "void"}]
        pod = {"metadata": {"labels": deepcopy(labels)}, "spec": {
            "automountServiceAccountToken": False,
            "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [container],
        }}
        if gpu:
            pod["spec"]["nodeSelector"] = deepcopy(pool["selector"])
        return pod
    head = pod_template("ray-head", request.head_cpu, request.head_memory_gib)
    head["spec"]["containers"][0]["ports"] = [
        {"name": "gcs-server", "containerPort": 6379},
        {"name": "dashboard", "containerPort": 8265},
        {"name": "client", "containerPort": 10001},
    ]
    # A submitter has no GPU and cannot inherit a service-account API token.
    submitter = pod_template("ray-job-submitter", 1, 2)
    submitter["spec"]["restartPolicy"] = "Never"
    return {
        "apiVersion": "ray.io/v1", "kind": "RayJob",
        "metadata": {"name": request.name, "namespace": request.namespace,
            "labels": {**labels, "kueue.x-k8s.io/queue-name": queue_name},
            "annotations": {"davano.io/allocation-mode": request.mode, "davano.io/gpu-resource": request.resource_name, "davano.io/kuberay-target-version": KUBERAY_VERSION}},
        "spec": {
            "suspend": True, "shutdownAfterJobFinishes": True, "ttlSecondsAfterFinished": 300,
            "entrypoint": request.entrypoint, "submissionMode": "K8sJobMode",
            "activeDeadlineSeconds": request.active_deadline_seconds,
            "backoffLimit": 0, "submitterConfig": {"backoffLimit": 0},
            "submitterPodTemplate": submitter,
            "rayClusterSpec": {
                "rayVersion": request.ray_version, "enableInTreeAutoscaling": False,
                "headGroupSpec": {"serviceType": "ClusterIP", "rayStartParams": {"dashboard-host": "0.0.0.0", "num-gpus": "0"}, "template": head},
                "workerGroupSpecs": [{
                    "groupName": "gpu-workers", "replicas": request.workers,
                    "minReplicas": request.workers, "maxReplicas": request.workers,
                    "rayStartParams": {"num-gpus": str(request.gpus_per_worker)},
                    "template": pod_template("ray-worker", request.worker_cpu, request.worker_memory_gib, gpu=True),
                }],
            },
        },
    }


def ray_capabilities():
    return {
        "status": "manifest-planning", "kuberay_version": KUBERAY_VERSION,
        "kueue_api_version": KUEUE_API_VERSION,
        "submission_enabled": False, "modes": list(MODES), "api_modes": ["whole-gpu"],
        "limitations": [
            "Planning/export does not create Kubernetes resources or reserve GPUs.",
            "The HTTP planner uses local advisory pools and policies, not an authenticated ownership grant.",
            "KubeRay 1.7.2 needs explicit Ray num-gpus for nvidia.com/gpu.shared. Shared units are not physical GPUs or memory/fault isolation.",
            "MIG profiles are distinct resources. MIG and shared modes are available to the pure builder only, not the whole-GPU local pool API.",
            "Fixed replicas only. Ray logical GPU resources do not guarantee device isolation, placement, or gang scheduling.",
        ],
    }


def create_ray_router(fleet):
    """Read-only planner backed by persisted local policy and observed inventory."""
    router = APIRouter(prefix="/api/v1/ray", tags=["Ray and Kueue planning"])

    @router.get("/capabilities")
    def capabilities():
        return ray_capabilities()

    @router.post("/plan")
    def plan(request: RayJobRequest):
        state = fleet.state()
        pool = next((p for p in state["pools"] if p["id"] == request.pool_id), None)
        if pool is None:
            raise HTTPException(404, "Pool not found")
        try:
            limits = policy_limits(request, pool, state["policies"])
            # An annotation or resource spelling alone cannot certify whole-GPU
            # mode. Check normalized inventory evidence without fabricating fit.
            selected_resources = [r for node in state["nodes"]
                if all(node["labels"].get(k) == v for k, v in pool["selector"].items())
                for r in node["resources"] if r["name"] == request.resource_name]
            if any(r["mode"] != request.mode for r in selected_resources):
                raise RayPlanError("Pool selector includes mixed or unknown allocation modes for this resource; narrow the selector before planning")
            resources = [r for r in selected_resources
                if r["allocatable"] is not None and r["allocatable"] > 0]
            if not resources:
                raise RayPlanError("No observed pool resource has the requested resource_name and verified allocation mode")
            if not any(r["allocatable"] >= request.gpus_per_worker for r in resources):
                raise RayPlanError("GPU units per worker exceeds every observed matching node's allocatable resource")
            queue_name = generated_name("davano", request.pool_id, request.namespace)
            manifest = build_suspended_ray_job(request, pool, state["policies"], queue_name)
        except RayPlanError as error:
            raise HTTPException(422, str(error)) from None
        warnings = [
            "Not submitted or allocated. Policy eligibility is advisory; it is not authorization to execute this workload.",
            "Inventory is a snapshot, not a reservation. CPU, memory, readiness, taints, topology and concurrent placement are not proven.",
            "The generated queue name is provisional. Bind it to an administrator-approved LocalQueue before applying.",
            "Ray dashboards and cluster endpoints must stay on trusted private networks. Apply reviewed namespace/network isolation before submission.",
            "Kueue quota admission is not proof all pods can run simultaneously. Ray GPU requests are logical scheduler units.",
        ]
        if state["meta"]["source"] == "demo":
            warnings.insert(0, "This plan is derived from synthetic demo inventory, not a connected cluster.")
        return {
            "status": "planned", "submitted": False, "allocated": False, "advisory_only": True,
            "source": state["meta"]["source"], "observed_at": state["meta"]["observed_at"],
            "manifest": manifest, **limits, "resource_name": request.resource_name, "mode": request.mode,
            "warnings": warnings,
            "prerequisites": [
                "Install and validate KubeRay v1.7.2, compatible Kueue CRDs serving v1beta1, and the vendor device plugin.",
                "Enable Kueue ray.io/rayjob integration and verify webhook readiness before applying the suspended RayJob.",
                "Create a matching ResourceFlavor, ClusterQueue and LocalQueue with explicitly approved quotas; local policy does not create them.",
                "Verify image/Ray version compatibility, application contents, image trust, network isolation and resource availability in the target cluster.",
                "Use cluster RBAC/admission controls to prevent queue or policy bypass; do not manually unsuspend a Kueue-managed RayJob.",
            ],
        }

    return router
