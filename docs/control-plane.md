# Kubernetes control plane

This is implemented control-plane software, **not a verified production release**. The default app remains a fixture/read-only inventory and local advisory workspace. The separate operator and explicitly enabled control API manage real Kubernetes objects. No cluster or hardware was available for this build, so native admission acceptance, Kueue behavior, runtime images, and GPU scheduling are not certified.

## What is implemented

- Cluster-scoped `fleet.davano.io/v1alpha1` `GPUPool` and namespaced `GPUAccessPolicy` CRDs, persisted by Kubernetes rather than SQLite.
- Reconciliation into owned Kueue `ResourceFlavor`, `ClusterQueue`, `LocalQueue`, native `ResourceQuota`, `ValidatingAdmissionPolicy`/bindings, and namespace quota/queue guards. Automatic tenant workload grants are disabled.
- GPU Pod placement through exact, administrator-managed NodeRestriction-protected labels. No node labels, taints, drivers, device plug-ins, MIG layouts, MPS settings, or GPU sharing settings are mutated.
- Kueue CPU and memory quotas derived strictly from selected nodes’ observed allocatable values, alongside the configured GPU nominal quota. These are accounting limits, not physical reservations.
- Namespace-total GPU request quota; conservative per-Pod GPU cap across all app and init containers; selected-resource restriction; rejection of direct node binding and RuntimeClass on all Pods in managed namespaces (including CPU-only Pods).
- GPU `batch/v1` Jobs must be created suspended with the assigned Kueue LocalQueue label. These are GPU resource guardrails only. No tenant workload creation/read/delete grant is issued, including for CPU Jobs. `subjects` is a requested declaration with no RBAC effect.
- HTTPS Kubernetes transport with rotating service-account token reads, bounded responses, no redirects, safe error messages, complete-list requirements, resource-version compare-and-swap writes, and UID-based ownership checks.
- Lease leadership with renewal and write fencing on leadership loss; bounded retry/backoff; health/readiness probes and Prometheus counters.
- Explicit finalizers, ownership-preserving reconciliation, restart recovery, and deletion preconditions. Polling is every ten seconds, not a real-time watch stream.

## Important scope boundaries

These controls are **not global cluster isolation**. Other namespaces, privileged administrators, existing RBAC grants, unmodeled controllers, direct binding privileges, existing Pods, networking, storage, and physical GPU isolation require separate administrator controls. NodeRestriction must actually be enabled; protected-label names alone do not prove that. Physical whole-GPU classification still relies on installed device-plugin metadata and administrator assertions. A mode change can race a reconcile interval.

Kueue accounting governs queued Jobs. Pod quota and placement guardrails govern the selected GPU namespaces independently, including Pods produced by controllers. Existing broader permissions may bypass queue-based fairness through other workload kinds, although new GPU Pods still face namespace quota and placement/cap admission. Ray planning is implemented separately, but there is no authenticated live RayJob submission path or RayJob creation grant. Slurm remains unsupported. Optional [read-only exporter and queue observation](observability.md) is implemented separately and is not GPU-health certification. The separately configured [constrained Job broker](constrained-execution.md) can submit approved single-Pod Jobs without granting tenants Kubernetes workload permissions; it does not enable live RayJob submission.

RuntimeClass is unsupported for **all new Pods in managed GPU namespaces**, even when containers request no GPU and even when the RuntimeClass only adds CPU/memory overhead. Kubernetes 1.31 admission CEL does not expose `spec.overhead` in this gate. The conservative policy rejects `spec.runtimeClassName` instead; checking only GPU container requests would leave an overhead-only bypass. The built-in **RuntimeClass admission controller must be enabled**. Its validation rejects manually supplied overhead without corresponding RuntimeClass overhead, including changes by mutating admission. This prerequisite is exercised by the real API-server smoke test; do not claim enforcement on clusters with that controller disabled. See [Kubernetes v1.31.4 RuntimeClass admission implementation](https://github.com/kubernetes/kubernetes/blob/v1.31.4/plugin/pkg/admission/runtimeclass/admission.go) (`prepareObjects` and `validateOverhead`). Existing Pods remain outside the CREATE-only policy.

`maxGPUsPerPod` is not a multi-Pod Job or RayJob-wide cap. App and all init GPU limits are summed conservatively; sequential init workloads may be rejected even when their effective peak fits. Pod admission currently checks CREATE. Kubernetes normally makes GPU resource requests and node selectors immutable on existing Pods; validating every possible future API feature requires upgrade testing.

`GPUPool Ready=True` requires current-generation Kueue Active observation. `GPUAccessPolicy Ready` remains **False** with `WorkloadGrantsDisabled`, because this release has no secure tenant execution boundary. `GuardrailsReady=True` separately records current-generation Kueue Active conditions, API-server CEL type-check completion without warnings, and ResourceQuota hard-limit observation. It does not mean that tenant permissions were granted, admission caches have activated every binding, a GPU workload ran, or production acceptance gates passed. Missing dependency status remains pending. A later control-plane failure retains installed native guardrails rather than deleting them. Observed invalid eligibility requests a Kueue admission Hold without eviction; the hold remains asynchronous and does not govern inherited direct workload permissions.

## Installation prerequisites and boundaries

Target Kubernetes 1.30+ with stable ValidatingAdmissionPolicy, or an OpenShift release based on compatible Kubernetes APIs. Install and validate Kueue separately with ResourceFlavor/ClusterQueue/LocalQueue APIs and batch Job integration. Set `DAVANO_KUEUE_API_VERSION` to the served version (`v1beta1`, the current default, or `v1beta2`); newer Kueue releases may require `v1beta2`. The shared resource shape is implemented, but neither version has been cluster-validated in this build. No Kueue, KubeRay, GPU driver, or device-plugin installer is executed automatically.

Before installation:

1. Choose an authorized nonproduction cluster and verify its context, backup policy, native admission configuration, NodeRestriction, device-plugin modes, and existing tenant permissions.
2. Build, scan, sign and pin a AllotForge container digest. The manifests use an intentionally unpinned development image placeholder; replace it.
3. Choose dedicated tenant namespaces, configure `DAVANO_MANAGED_NAMESPACES`, and apply the narrowly scoped Role/RoleBinding examples for **each** namespace. The operator does not create tenant namespaces or silently grant itself namespace access.
4. Apply the operator CRDs and deployment in `deploy/operator`. `deploy/operator-openshift` reuses the restricted nonroot deployment without fixed UID/fsGroup; OpenShift SCC assigns them. This has not been validated on an OpenShift cluster.
5. Review `deploy/examples/managed-resources.yaml`. Pool placement and resource name are immutable, and namespaces support one access policy each. Do not install the unrelated legacy draft CRDs from `deploy/contracts`.
6. Assign protected node labels administratively. All selected nodes must advertise the chosen resource in verified whole-GPU mode; mixed shared/unknown nodes, overlapping pool selectors, or nominal quota above observed allocatable capacity block reconciliation.

There is no public ingress or Route in these manifests. Default-deny ingress is provided, with API-server egress left to the administrator's network design. The operator's cluster-level admission write authority is consequential and must be reviewed. Namespaced quota/queue writes require the separate tenant Role. RoleBinding permissions are get/delete only, for UID-owned legacy grant revocation. The controller cannot create/update/bind roles or impersonate users. The old `davano-workload-submitter` ClusterRole is deliberately inert (`rules: []`) for upgrades.

## Optional administrator API

`DAVANO_CONTROL_ENABLED=true` is accepted only alongside `DAVANO_SOURCE=kubernetes` and a `DAVANO_API_TOKEN` at least 32 characters long. `deploy/control-api` mounts the token from a pre-existing Secret named `davano-control-api`, key `token`. Provision it using your approved secret-management procedure. No credential is generated, stored in source, or exposed by status responses.

All control collection reads and mutations require `Authorization: Bearer …`. `/api/v1/control/status` exposes only configuration and capability descriptions. This is one administrator identity, **not** end-user SSO, per-user authorization, or an identity-aware multi-tenant API. Keep it private behind your authenticated TLS ingress/proxy and enable Kubernetes audit logging. The app does not trust user identity headers. Demo/local advisory APIs remain a separate workspace and do not change managed CRDs.

Routes:

- `GET /api/v1/control/status`
- `GET|POST /api/v1/control/pools`
- `GET|POST /api/v1/control/policies`
- `DELETE /api/v1/control/pools/{name}`
- `DELETE /api/v1/control/policies/{namespace}/{name}`
- `POST /api/v1/control/policies/{namespace}/{name}/allocation/preview`
- `POST /api/v1/control/policies/{namespace}/{name}/allocation/apply`

Creates persist CRDs and do not promise synchronous enforcement. Delete JSON must carry the latest `uid` and `resourceVersion`. The operator finalizer may hold deletion until prerequisites are met. The allocation preview/apply routes narrowly update the same policy's `gpuQuota` and `maxGPUsPerPod`, requiring current `uid`, `resourceVersion` and unchanged `poolRef`. Apply returns `pending-reconciliation`, not synchronous native enforcement. Cross-pool policy changes and atomic cross-policy transfers are blocked; existing workloads are not resized or evicted. Review the separate exact-target permissions in `deploy/examples/allocation-api-rbac.yaml` and [resource-assignment guide](resource-assignment.md). No general-purpose HTTP policy/subject update route is implemented.

## Deletion and failure recovery

Deleting a pool is blocked while access policies reference it or its Kueue queue reports admitted/pending workloads. Deleting access is blocked while active Pods or admitted, reserving, or pending LocalQueue workloads remain. After draining, owned queues and then admission/quota guards are removed, waiting for each actual 404 before proceeding. Legacy owned workload grants are revoked independently before reconciliation dependency reads. Removing a policy therefore removes those namespace protections; other RBAC is unchanged. External workload controllers can race a drain check, so stop submitters and controllers first. Do not use policy deletion as an emergency access-revocation mechanism.

Foreign replacements are never adopted or deleted. A conflicting UID leaves the finalizer and a failed status for administrator investigation. Do not strip finalizers casually: orphaned native objects may retain grants, quota, or admission rules. Deleting the CRD itself bypasses these lifecycle guarantees and is not a supported uninstall sequence.

On controller or API unavailability, installed native objects remain active. A lost Lease immediately fences further writes. No tenant access is granted at any reconciliation stage. Legacy owned bindings must be confirmed absent (404); pending/failed revocation produces WorkloadAccess=Unknown. Apply updated inert-role manifests on upgrade and manually review namespaces removed from the allowlist. API-server, Kueue or quota-controller delays retain pending status. Audit the native resources before resuming after a partial failure.

## Verification and release gates

Local tests cover desired-state persistence through an API double, idempotence, ownership conflicts, shared-mode rejection, overlapping pools, quotas, readiness conditions, namespace allowlisting, deletion guards, transport, authentication, and Lease fencing. Static schema/RBAC tests do not evaluate CEL.

`python scripts/verify_native_admission.py` runs real API-server CRD/CEL/Pod/Job dry-run tests **only** against the disposable context `kind-davano-ci`, with `DAVANO_TEST_CONTEXT=kind-davano-ci`. The corresponding GitHub workflow is included but has not run here. It does not test Kueue or GPUs.

Before production, complete all of these in an authorized staging cluster:

- Render every Kustomize root and server-dry-run all resources; execute the native-admission suite on every supported Kubernetes/OpenShift version.
- Install the chosen Kueue release, verify current-generation readiness fields, exact GPU queue accounting, quota changes and suspended Job admission.
- Exercise actual NVIDIA/AMD/Intel hardware, scheduler placement, topology, drain, device health, and plugin mode changes.
- Test failover, API outages, throttling, resource-version conflicts, webhook/native-admission failures, deletion races and recovery.
- Verify negative RBAC and bypass cases using real tenant identities and existing inherited roles.
- Add organization SSO/authorization, durable audit retention, TLS/secret rotation, resource sizing, image pinning/signing, vulnerability scanning, alerting, backups and upgrade/rollback procedures.

No stage above is marked passed merely because unit tests pass. There is no production deployment, hardware benchmark, load test, or end-to-end scheduling result in this source bundle.

## Upstream references checked

- [Kubernetes CEL list functions](https://kubernetes.io/docs/reference/using-api/cel/) confirm numeric `.sum()` support.
- [ValidatingAdmissionPolicy API](https://kubernetes.io/docs/reference/kubernetes-api/admissionregistration/validating-admission-policy-v1/) defines status type checking and observed generations.
- [Kueue ClusterQueue controller v0.12.4](https://github.com/kubernetes-sigs/kueue/blob/v0.12.4/pkg/controller/core/clusterqueue_controller.go) sets Active condition observedGeneration. This source inspection is not a runtime compatibility test.

## Disabled automatic workload access

The initial controller grant path was removed during independent review. `jobs.create` would permit arbitrary workload templates to borrow service accounts and namespace secrets, and privileged templates where admission allows them. PSA alone is insufficient. A future grant feature requires an independently tested workload identity, credential/volume, Pod Security, and template admission boundary. Do not restore the grant by simply enabling a flag or reintroducing the old ClusterRole rules.
