# Assign GPU resources to namespaces, projects and workloads

AllotForge supports two different operations. Neither physically moves a GPU, edits device-plugin configuration, changes a running container's GPU assignment, or provides transparent live migration. This extension has local API-double tests, not live GPU/cluster validation.

## 1. Namespace/project GPU ceilings

An administrator can preview and update an existing GPUAccessPolicy's namespace GPU quota and per-Pod maximum. Existing configured projects refer to those namespace policies, so the ceilings govern future project workload admission.

- The policy stays in its current pool. Pool selectors and GPU resource names are immutable, and cross-pool policy changes are refused.
- The policy's UID and resourceVersion must match exactly. Preview writes nothing. Apply performs one compare-and-swap update of the GPUAccessPolicy and reports `pending-reconciliation`.
- Native guardrail ownership, specification, quota observation, admission type checking and queue readiness are checked first.
- A lower quota does not evict existing Pods or resize their requests. New native limits become effective asynchronously after reconciliation. Raising a ceiling is permission to request capacity, not a reservation or a guarantee that GPUs are available.
- Ceilings can overlap across projects within the shared Kueue pool capacity. Reducing one namespace and increasing another are separate actions, not an atomic resource transfer or reserved entitlement.
- These policy edits concern GPU limits. CPU and memory requests are selected through approved workload profiles, not namespace CPU/memory policy editing.

API, with the control administrator bearer token:

- `POST /api/v1/control/policies/{namespace}/{name}/allocation/preview`
- `POST /api/v1/control/policies/{namespace}/{name}/allocation/apply`

Input: `{ "uid": "policy-uid", "resourceVersion": "current-version", "poolRef": "existing-pool", "gpuQuota": 4, "maxGPUsPerPod": 2 }`.

Responses contain `before`, `desired`, `preconditions`, `limitations`, and status `preview` or `pending-reconciliation`. Successful apply also includes the updated policy. A stale write conflicts; the server never silently retries it. When a transport failure leaves the outcome uncertain, reload the policy before applying again.

Control must be explicitly enabled. The optional review-only `deploy/examples/allocation-api-rbac.yaml` adds exact-resource policy update permission and read-only native-guardrail access for the trusted control service. It is not installed by default. Replace all namespace and UID-derived names and review it before any authorized deployment. No user/token holder gains Kubernetes RBAC through this interface.

## 2. Queued workload move or resize

A project submitter can preview replacing an existing broker-owned, currently suspended Job with a new Job in an authorized project. The destination project determines the namespace and existing GPU pool; its approved profile determines fixed image, command, CPU/memory and runtime limits. The requested whole-GPU count can change within all destination limits.

This permits routing an eligible queued workload into another configured project/namespace/pool, or changing its approved resource profile in the same project. It does not mutate an immutable Pod template or reuse the running process. A same-project replacement must have a new workload name.

Both source and destination project permissions are required. A viewer can preview, but cannot begin or advance replacement. External arbitrary Jobs, running/admitted workloads, notebooks and distributed controllers are not supported by this path.

### Preview and apply safeguards

Preview verifies the source's exact UID/resourceVersion, broker ownership and current suspension. Execution evidence, source Pods, or Kueue admission/reservation/eviction/requeue history blocks the operation. Destination preflight is the full [constrained execution](constrained-execution.md) preflight. The returned fingerprint binds the source revision/specification, destination project, requested profile/resources and resolved execution configuration.

Apply repeats those checks and refuses a changed fingerprint. It transactionally reserves the destination name and saves the durable operation before attempting deletion. Normal submission cannot consume that reserved name. The source deletion request uses exact UID/resourceVersion preconditions and an audit record.

Only a **confirmed accepted** cancellation by this operation allows automatic continuation after the source disappears. An externally disappeared/deleting source, definitive deletion denial, or uncertain DELETE response followed by disappearance remains `NeedsAttention`: an absent source alone cannot prove that it did not run. No target is automatically submitted in that case. An accepted deletion followed by a local audit-result failure can be recovered from the separately persisted accepted-cancellation state.

After confirmed cancellation, both the source Job and its UID-owned Pods/Kueue Workloads must be absent. Kubernetes accepting a DELETE is insufficient. Only then is the reviewed destination Job submitted through the constrained broker, initially suspended for Kueue. Destination failures leave the operation resumable with the same reserved name; changed destination configuration blocks further mutation. An uncertain destination-create response is recovered against its exact durable ownership identity, avoiding duplicate submission.

**Deletion and replacement are not atomic.** If the source has been removed and the destination cannot start, there may be no workload running. There is no automatic rollback, alternate destination, checkpoint copy, filesystem transfer or substitute name. The UI reports that state rather than claiming resources moved successfully.

### Workload API

All routes require the scoped execution bearer token:

- `POST /api/v1/execution/projects/{source}/workloads/{name}/reassignment/preview`
- `POST /api/v1/execution/projects/{source}/workloads/{name}/reassignments`
- `GET /api/v1/execution/reassignments` returns the latest 100 operations visible in both project scopes.
- `GET /api/v1/execution/reassignments/{id}` reads a saved operation without cluster mutations.
- `POST /api/v1/execution/reassignments/{id}/advance` explicitly resumes that same saved operation.

Preview input:

```json
{
  "uid": "source-job-uid",
  "resourceVersion": "current-version",
  "target_project": "destination-project",
  "target": { "name": "replacement-001", "profile": "approved-profile", "gpu_count": 2 }
}
```

Begin input adds the exact `plan_fingerprint` returned by preview. It returns HTTP 202 with the durable operation's ID and phase. Common phases:

- `WaitingForSourceDeletion`: destination reserved, source/owned-child deletion not yet proven complete.
- `TargetSubmitted`: Kubernetes accepted the replacement; allocation and successful execution remain unconfirmed.
- `NeedsAttention`: a prerequisite, permission, configuration, transport or uncertainty block needs review. Repeatedly pressing advance does not override it.

Responses include `source_deleted`, `source_deletion_state`, `target_submitted`, `detail`, `workload` when available, and `allocated: false`. Up to 1000 reassignment operations per source project are retained; target reservations also consume the existing bounded workload history. No automatic purge or public ownership-reset endpoint exists.

The broker requires read-only access to namespace Kueue Workloads to verify source-child absence; the review-only execution Role includes this. The supported persistent broker deployment remains one replica and one server worker, not an HA coordinator.

## Verification boundaries

Local tests cover previews with zero Kubernetes writes, exact revisions, target-name reservations, project authorization, admitted/running rejection, waiting on actual child deletion, source deletion attribution, destination failure and resume, post-write uncertainty, restart recovery, and deletion-before-submission ordering. API-native quantity canonicalization is accepted without weakening exact quota keys or scope checks.

Real-cluster verification is still required for Kueue/controller races, admission behavior, foreground garbage collection, persistence/crash recovery, CNI isolation, GPU scheduling and runtime behavior. UI screenshots and API doubles demonstrate implemented flows, not physical GPU movement or live production success.
