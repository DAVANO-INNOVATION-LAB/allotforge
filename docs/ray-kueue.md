# Kueue and Ray integration

## Implemented boundary

`davano.ray_integration` builds actual Kubernetes API objects rather than simulated jobs:

- `RayJob` (`ray.io/v1`), targeting **KubeRay v1.7.2**.
- `ResourceFlavor`, `ClusterQueue`, and `LocalQueue` (`kueue.x-k8s.io/v1beta1`). The installed Kueue release must still serve this API version.
- A read-only HTTP planner that checks persisted local policy and observed GPU resource modes before producing a suspended RayJob.

**The Ray HTTP API does not submit jobs.** It makes no execution, admission, reservation, identity-authorization, or isolation claim. The separate managed operator is responsible for live GPUPool/GPUAccessPolicy reconciliation; its UID-owned queue names are not interchangeable with the planner's provisional names. Planning remains available in demo mode, with an explicit synthetic-data warning.

No target cluster was used to build this integration. Unit/API contract tests passed locally; API-server dry-run, webhooks, image execution, GPU runtime behavior, Kueue admission, and end-to-end cleanup remain deployment gates.

## API

### Read capabilities

`GET /api/v1/ray/capabilities` reports the target KubeRay version, served manifest API, supported builder modes, HTTP planner modes, limitations, and `submission_enabled: false`.

### Plan a RayJob

`POST /api/v1/ray/plan` accepts:

```json
{
  "name": "training-run",
  "namespace": "research",
  "pool_id": "training",
  "image": "registry.example/team/ray-training:2026-11-11",
  "ray_version": "2.55.1",
  "entrypoint": "python /app/train.py",
  "workers": 2,
  "gpus_per_worker": 2,
  "resource_name": "nvidia.com/gpu",
  "mode": "whole-gpu",
  "worker_cpu": 4,
  "worker_memory_gib": 16,
  "head_cpu": 1,
  "head_memory_gib": 2,
  "active_deadline_seconds": 3600
}
```

The example image is a placeholder, not a published image. Supply an image you trust containing Ray, the application, and the correct vendor runtime. `ray_version` must match the actual image. Explicit non-`latest` tags or a SHA-256 digest are required; tags remain mutable, so use a verified digest for release deployment.

The response includes `manifest`, `policy_ids`, `total_gpu_units`, `per_workload_cap`, `warnings`, and `prerequisites`. It always has `submitted: false`, `allocated: false`, and `advisory_only: true`. HTTP 404 means the pool does not exist. HTTP 422 means invalid request fields, policy constraints, or resource-mode eligibility failed.

Checks include:

- Matching pool, namespace policy, vendor, and allocation mode.
- Total GPU units equal `workers × gpus_per_worker`; local per-workload policy applies to this total, with the strictest matching limit winning.
- The selected resource must actually be present in normalized inventory with a verified allocation mode and known positive allocatable count.
- A worker cannot request more units than any matching node can advertise. Availability is not reserved: jobs may queue when resources are currently busy.
- A selector that matches both whole and shared/unknown nodes under the same resource name is rejected. Finding one whole-GPU node does not make the entire selector safe.
- CPU/memory placement, readiness, taints, concurrency, topology, storage, and multi-node gang scheduling are not proven by this planner.

Unknown request fields are rejected. The request cannot inject privileged containers, volumes, service accounts, arbitrary pod fields, runtime environments, autoscaling, or an unsuspended state. This is a constrained manifest builder, not a general pod-template proxy.

## RayJob design

The manifest has a CPU-only head, a single fixed-size worker group, and an explicit GPU resource request/limit on every worker. Replicas/minReplicas/maxReplicas are equal; in-tree autoscaling is disabled. `spec.suspend` starts true, so Kueue owns release into execution. `shutdownAfterJobFinishes` is true with a five-minute cluster cleanup TTL. Automatic job retries are zero and a bounded active deadline is supplied. These fields follow the [pinned KubeRay RayJob API](https://github.com/ray-project/kuberay/blob/v1.7.2/ray-operator/apis/ray/v1/rayjob_types.go).

Pod templates use non-root execution, disabled privilege escalation, dropped capabilities, RuntimeDefault seccomp, and no auto-mounted Kubernetes API token. CPU-only head and submitter containers have `NVIDIA_VISIBLE_DEVICES=void`; the head advertises zero Ray GPUs. The head service is ClusterIP only. These defaults reduce exposure; they do not turn Ray into an untrusted multi-tenant sandbox. The application must schedule GPU work onto Ray GPU tasks/actors, not assume the CPU head owns a GPU.

Ray and its dashboard must remain on a trusted private network. Add reviewed CNI-enforced NetworkPolicies and namespace boundaries, including head/worker/submitter and KubeRay controller connectivity. This builder does not create a NetworkPolicy or authenticate Ray's dashboard. Do not publish these services directly to the Internet. Validate the image's non-root behavior, `/tmp` needs, object-store memory, driver libraries, and security policy in the target cluster.

A K8sJobMode submitter is also explicitly sized at 1 CPU and 2 GiB with no GPU. Allow capacity for it outside the Ray cluster PodSets; do not infer that Kueue's Ray cluster resource accounting includes the submitter. Cleanup and failure behavior need real-cluster tests.

See [Kueue's RayJob integration requirements](https://kueue.sigs.k8s.io/docs/tasks/run/rayjobs/) for queue labels, suspension ownership, cluster lifecycle, and installed-version constraints. Configure `ray.io/rayjob` integration and verify admission webhooks before applying generated jobs. Do not manually toggle `suspend` to bypass Kueue.

## Allocation mode and KubeRay 1.7.2

The pure builder accepts three distinct modes:

| Mode | Supported resource names | Meaning |
| --- | --- | --- |
| whole-gpu | `nvidia.com/gpu`, `amd.com/gpu`, `gpu.intel.com/i915`, `gpu.intel.com/xe` | Verified device-plugin scheduling units under a whole-GPU pool contract. Name alone is insufficient evidence. |
| shared | `nvidia.com/gpu.shared` | Shared scheduling replicas. Exactly one replica per worker is allowed. No exclusive memory, fault isolation, proportional performance, or physical-device count is asserted. |
| mig | `nvidia.com/mig-<N>g.<M>gb` | Explicit pre-existing MIG profile instances. Different profiles are never converted into whole-GPU totals. |

The HTTP planner uses existing local whole-GPU pools, so its supported mode is currently `whole-gpu`. Shared/MIG builders are for independently validated integrations and exports; this implementation does not configure device-plugin sharing, reconfigure MIG, or enable shared/MIG ownership in the managed operator.

The [KubeRay v1.7.2 resource detector](https://github.com/ray-project/kuberay/blob/v1.7.2/ray-operator/controllers/ray/utils/resources.go) does not automatically recognize `nvidia.com/gpu.shared`. The builder therefore sets each worker group's `rayStartParams.num-gpus` explicitly to the integer number of requested units. This is a Ray logical scheduling declaration, not a hardware isolation mechanism. The same explicit mapping avoids relying on resource-name inference for supported Intel resources.

Unrenamed time-slicing (`nvidia.com/gpu` still advertised) and ambiguous single-strategy MIG names cannot be requested as shared/MIG through this builder. Rename and independently verify the resource before using those modes. NVIDIA's [device-plugin documentation](https://github.com/NVIDIA/k8s-device-plugin#shared-access-to-gpus) describes sharing's `renameByDefault` and `failRequestsGreaterThanOne` semantics. Shared replicas must never be counted as exclusive whole GPUs.

## Pure Python integration

```python
from davano.ray_integration import RayJobRequest, build_suspended_ray_job

request = RayJobRequest(**validated_request_data)
manifest = build_suspended_ray_job(
    request,
    pool=resolved_pool,
    policies=resolved_policies,
    queue_name=verified_local_queue_name,
)
```

The function accepts either local pool/policy records or managed GPUPool/GPUAccessPolicy objects. For managed policy it treats `maxGPUsPerPod` as a per-worker cap and `gpuQuota` as a total namespace quota. It optionally checks `maxGPUsPerWorkload` if supplied; this is not the current managed CRD's field. These numeric checks are not an authorization API. The caller must resolve applicable policies, authenticate and authorize the subject, verify policy/queue readiness and observed generation, verify mode and selector safety against fresh inventory, then submit through its own explicit write path. The builder does not inspect subjects or pretend that a passed-in policy is an ownership grant.

The managed operator's actual queue name is derived from a policy UID, not from an advisory SQLite ID. Resolve it from managed status/resources; do not replace an existing queue's identity or take ownership based on a name match.

## Standalone Kueue bundle

```python
from davano.ray_integration import QueueBundleRequest, build_kueue_bundle

objects = build_kueue_bundle(QueueBundleRequest(
    pool_id="training",
    namespace="research",
    node_selector={"davano.dev/pool": "training"},
    resource_name="nvidia.com/gpu",
    mode="whole-gpu",
    nominal_gpu_quota=8,
    nominal_cpu_quota=64,
    nominal_memory_gib=256,
))
```

This returns four reviewable API objects; it makes no client calls. Quotas are mandatory inputs rather than inferred from a per-workload cap or transient free capacity. The ClusterQueue is limited to the named namespace using Kubernetes's namespace-name label, with no cohort borrowing. CPU/memory use a separate empty ResourceFlavor, so GPU flavor labels are not injected into the CPU head. GPU flavor labels constrain workers. See [ResourceFlavor selection behavior](https://kueue.sigs.k8s.io/docs/concepts/resource_flavor/).

This standalone export has deterministic names and no ownership or reconciliation. It is **not** the managed operator's resource plan. Never apply it over existing managed objects. Operators must budget the sum of quotas across overlapping flavors/namespaces and prevent bypass with cluster RBAC/admission. Kueue quotas alone do not constrain arbitrary Pods or direct Ray submissions outside Kueue.

## Target-cluster acceptance checklist

1. Pick explicit Kubernetes, Kueue, KubeRay v1.7.2, driver, device-plugin, and Ray image versions. Confirm served API schemas and `ray.io/rayjob` integration. Do not infer the installed version from a generated annotation.
2. Verify pool selector exclusivity and actual vendor resource modes across every matching node. Test taints, capacity, non-root container behavior, and hardware/runtime compatibility.
3. Obtain approved pool/namespace quotas and the real LocalQueue name. Establish network, authentication, RBAC, and admission boundaries. Confirm the subject cannot bypass those controls using raw Pods, queue labels, direct Ray access, or another service account.
4. Review exported JSON/YAML. Only on an explicitly selected and authorized target, run API-server dry-run first: `kubectl --context <approved-context> apply --dry-run=server -f rayjob.json`.
5. In an isolated test namespace, check pending queue behavior, admission, actual GPU task execution, over-quota rejection, unauthorized namespace/subject rejection, unschedulable workers, failures, cleanup, restart/reconciliation, and shared/MIG semantics if separately enabled.
6. Verify actual observed RayJob/Workload conditions and GPU task results. A generated manifest or successful Kubernetes create response is not evidence that a workload ran or that quota/ownership enforcement is complete.

Local checks:

```sh
python -m unittest discover -s tests -p test_ray_integration.py -v
python -m compileall -q davano
```
