# Capabilities and ownership boundaries

This document describes the **0.6.0 implementation**, including code paths that are disabled by default. It is not evidence of deployment, GPU execution, production safety or comparative performance. The default app uses labelled synthetic inventory; no cluster credentials, operator, execution access or exporter connection are enabled automatically.

## Implemented capabilities and authority

| Capability | Current level | Meaning |
| --- | --- | --- |
| Kubernetes/OpenShift inventory | Read-only, opt-in | Reads NodeList and PodList; OpenShift-specific runtime/SCC behavior still needs target validation. |
| Local pools, namespace policy and priority | Advisory metadata | SQLite records; no admission, team authentication or reservation. |
| Fit evaluation | Advisory estimate | Checks a single node's observed whole-GPU requests and local policy; never allocates or submits. |
| Ray HTTP planner | Manifest planning | Produces a constrained suspended RayJob; no live RayJob write or lifecycle management. |
| Kueue manifest builders | Offline object generation | Reviewable queue/flavor objects, not an installed or reconciled deployment. |
| Managed GPUPool/GPUAccessPolicy | Opt-in desired-state control | Separate operator reconciles owned queues, namespace quota and native admission guards; administrator API is disabled by default. |
| Project workload execution | Opt-in constrained Job broker | Static project identities and approved profiles can preview, submit, inspect and cancel owned single-Pod Jobs. No arbitrary templates or tenant Kubernetes RBAC grants. |
| Namespace allocation edits | Opt-in desired GPU ceilings | Administrator preview/apply of same-pool namespace GPU quota and per-Pod cap, with UID/resourceVersion concurrency checks. No reservation, atomic transfer or existing-Pod resize. |
| Queued workload reassignment | Opt-in cancel-and-recreate | Replace eligible broker-owned queued Jobs within/across authorized projects, with target-name reservation and durable operation state. No running/admitted-job migration. |
| Pod pending diagnostics | Read-only observation | Sanitized Pod conditions and waiting reasons; Pending alone does not prove GPU exhaustion. |
| Kueue queue observation | Read-only, opt-in | Discovers a served API and reads conditions. Separately enabled scoped pending-summary windows preserve identity uncertainty and never predict admission times. |
| Workload advisor | Read-only, opt-in attributed measurements | All-Pod request budgets plus conservative comparison experiments from a separately supplied trusted adapter; no automatic moves or claimed speed/cost winner. |
| Deployment diagnostics | Bounded, on-demand evidence | Separates configuration, successful narrow reads and API discovery from unverified writes, enforcement and runtime. No production-ready verdict. |
| GPU telemetry | Read-only, opt-in adapters | Selected NVIDIA DCGM and AMD exporter metrics through Prometheus; no real hardware validation, Intel telemetry unsupported. |
| Benchmarks | Scoped measurement tooling | Local CPU smoke results, offline batch adapters and bounded HTTP inference measurements; no included live GPU or competitor run. |
| MIG/sharing reconfiguration, Slurm, provisioning | Unsupported | No driver/device-plugin changes, MIG layout changes, Slurm control, cluster provisioning or autoscaling. |

The [API contract](api-contract.md) describes the separate route families. `/api/v1/state` reports `meta.source`, `meta.read_only_cluster` and `meta.enforcement` for the **inventory/advisory view**, not every enabled process capability. A `read_only_cluster: true` snapshot does not mean a separately enabled control or execution route cannot write. Configuration status is not evidence that its dependencies are ready or that a workload ran.

## Default behavior and identity

- `DAVANO_SOURCE=demo` is the default; live inventory must be explicitly selected. Live failures never substitute fixture data.
- `DAVANO_CONTROL_ENABLED=false` and `DAVANO_EXECUTION_ENABLED=false` are independent defaults. Inventory deployment roots do not install the operator or authorize workload writes.
- Prometheus telemetry has no default URL. Kueue observation defaults off. Demo mode returns no fabricated device or admission measurements.
- The default inventory/advisory API is unauthenticated and intended for a trusted local/operator workspace. Enabling control adds an administrator bearer gate to control collections, not universal authentication.
- Enabling project execution additionally requires the separate administrator token on unscoped inventory/advisory/Ray/observability routes. Project tokens are statically provisioned, project-scoped `viewer` or `submitter` identities; they cannot unlock unscoped fleet data.
- Public status endpoints disclose configuration/capability descriptions, not credentials or workload listings. No SSO, per-person identity provider, tenant onboarding or production security boundary is asserted.

## Preserve the actual resource unit

Keep node identity, extended-resource name, allocation mode and profile distinct instead of converting every quantity to “GPUs.” The implementation's resource-level accounting excludes ambiguous, MIG and shared quantities from whole-GPU aggregates. Counts of requested resources are scheduling accounting, not physical device assignments.

- **Whole GPU:** a device-plugin resource unit is treated as a whole GPU only with supporting configuration/label evidence. `nvidia.com/gpu` alone is insufficient: MIG single mode and unrenamed sharing can use it. Administrator mode assertions must be independently validated.
- **MIG:** count instances by profile and resource name. Seven `1g` instances are not seven physical GPUs; different profiles are not fungible capacity.
- **Time slicing:** replicas are scheduling slots, not isolated devices, guaranteed memory, guaranteed throughput or physical GPU count. The `.shared` suffix is optional in the device plugin, so its absence is not whole-GPU proof.
- **MPS:** client slots have installation-specific memory/compute semantics and are not MIG isolation. Placement evaluation and managed control do not support them.
- **Unknown:** missing or contradictory mode evidence stays unknown. A Running Pod or Ray `num_gpus` does not establish physical device ownership.

Effective requests include active assigned Pods, including assigned Pending Pods, app-container sums, init peaks, restartable init sidecars and GPU overhead. Explicit requests take precedence; limits are a fallback only when requests are absent. Completed/failed Pods do not consume active demand. Lists are non-atomic and can immediately become stale.

A fit result means an eligible single node passes the implemented GPU-count/local-policy and optional CPU/memory request-budget checks at observation time. The evaluator conservatively excludes NotReady/cordoned nodes and all NoSchedule/NoExecute taints. Optional CPU/memory dimensions count all bound nonterminal Pods, including CPU-only work; unsupported/missing evidence remains unknown. It does not evaluate topology, storage, affinity, other quotas or concurrent scheduling. It never combines several nodes to pretend a single-node request fits. Overlapping local pools describe the same resources, not additive capacity or ownership.

## Managed control is not global isolation

The separate controller manages `fleet.davano.io/v1alpha1` GPUPool and GPUAccessPolicy CRDs and UID-owned Kueue ResourceFlavor/ClusterQueue/LocalQueue, ResourceQuota and ValidatingAdmissionPolicy/binding resources. It uses protected node-label selectors, namespace allowlists, resource-version/precondition checks, finalizers and Lease leadership fencing. It does not mutate node labels, device plugins or existing GPU layouts. These paths have local API-double/static tests and a disposable Kubernetes 1.31 API-server admission/deployment CI gate. This does not verify real Kueue scheduling or GPU execution.

The default control model deliberately **does not grant tenant workload RBAC**. Policy `subjects` do not receive permissions. `GPUAccessPolicy Ready` remains false with `WorkloadGrantsDisabled`; `GuardrailsReady` separately describes current-generation dependency observation. It is not proof of CEL cache activation, CNI enforcement, real scheduler behavior or production tenancy. The constrained execution broker is a separate entry point and does not change that status into a tenant-RBAC grant.

Native guardrails restrict selected-resource Pods, namespace quota, per-Pod cap, node selection/direct binding and suspended queued batch Jobs. `maxGPUsPerPod` is not a multi-Pod Job or RayJob-wide ceiling. Conservative init-limit summation can reject workloads whose effective peak would fit. Existing Pods are not retroactively evicted. Other namespaces, inherited privileges, administrator changes, unmodeled controllers, admission delays, storage, networking and physical isolation remain outside this authority.

Kueue owns queue admission; Kubernetes owns scheduling. Readiness and an accepted Kubernetes object do not prove either occurred. Review [control-plane prerequisites, failure behavior and deletion rules](control-plane.md) before granting its write permissions.

## Constrained execution is not arbitrary workload hosting

The execution API requires live mode, a private administrator configuration and independent broker RBAC. Project configuration pins namespace, policy, pool and runtime ServiceAccount UIDs. New-Job input is only a name, approved profile and GPU count; guarded reassignment adds source identity and reviewed target-plan fields. Profiles fix an immutable image digest, command, CPU/memory and runtime/GPU bounds.

Before a new Job is created, the broker verifies recent/current native guardrails, selected-node resource modes, restricted Pod Security, the pinned tokenless ServiceAccount, exactly the configured deny-all ingress/egress NetworkPolicy and a native Job-count quota. These are checks of observed objects, not proof that the CNI or admission caches enforce them. Submit produces a suspended, one-Pod batch Job; Kueue and the scheduler must do the remaining work. It does not return a confirmed physical GPU allocation.

There are no tenant-supplied images, commands, environment values, credentials, volumes, network allowances, custom Pod fields, notebook sessions, logs/exec shells or distributed jobs. Read/cancel operations require the broker's persisted registry and matching Kubernetes identity, not a user-controlled label alone. Cancellation uses UID/resourceVersion preconditions; an accepted deletion is not proof that execution immediately stopped.

Per-project Job counts, registry history and audit capacity are bounded; reaching capacity blocks new mutations rather than silently dropping evidence. SQLite audit events and registry entries are durable only with correctly persisted/backed-up storage. They are not immutable, externally retained or highly available. Lost registry data can prevent safe access to otherwise live Jobs. Uncertain submission outcomes must be reloaded/reconciled rather than blindly resubmitted. See [constrained execution](constrained-execution.md).

## Assignment changes limits or creates a replacement workload

An administrator can preview and apply `gpuQuota` and `maxGPUsPerPod` updates to an existing managed namespace policy. The request includes its current UID/resourceVersion and unchanged `poolRef`. Current guardrails and pool readiness are checked, and the single policy write is compare-and-swap. The result is `pending-reconciliation`, not immediate native enforcement. Existing workloads are not resized, stopped or evicted. Lowering one namespace's ceiling does not reserve released capacity for another. Cross-pool policy changes, atomic cross-policy transfers and namespace CPU/memory-ceiling edits are unsupported.

Projects remain administrator-configured, each bound to one dedicated namespace/policy/pool. A project-scoped identity may preview queued workload reassignment only with visibility into **both** source and target; apply/advance additionally requires submitter authority for both. The source must be a broker-registered suspended Job with matching UID/resourceVersion and no observed execution/admission history or owned Pods. The target may be another approved project/pool or a new name/profile/GPU count in the same project. CPU/memory changes use approved fixed profiles, not arbitrary resource fields.

The reviewed plan fingerprint binds the source and effective target configuration. Applying it durably reserves the target name, requests guarded source deletion, and waits for the source Job, owned Pods and Kueue Workloads to be absent before creating a target. These are separate operations, not an atomic transfer. No process, filesystem, checkpoint or physical device is moved. Target acceptance still does not establish scheduling or GPU allocation.

Operations expose phase and source/target progress and can be explicitly advanced. Target failure after confirmed source deletion may leave a resumable operation with no live replacement; there is no automatic rollback. If a source DELETE outcome is uncertain and the source disappears without a recorded accepted cancellation, the workflow blocks target creation for administrator review rather than guessing whether it ran. Do not submit a substitute target name to bypass an unresolved operation. See [resource assignment](resource-assignment.md) for permissions, routes and recovery boundaries. Real admission/deletion races and GPU execution remain target-cluster acceptance gates.

## Device, queue and workload evidence stay separate

`/api/v1/observability` and `state.observability` separate exporter measurements from resource requests and queue conditions. No exporter is installed automatically. NVIDIA/AMD adapters preserve exporter UUIDs, entity kind, source metric, sample timestamp, scope and join confidence. UUID presence does not prove an exhaustive inventory, exclusive allocation, node identity or health. Ambiguous, unmatched and cross-cluster identities are not silently attributed to nodes.

Raw Prometheus samples older than 120 seconds are stale. Stale, unsupported, invalid or conflicting metrics expose no usable numeric value. Workload-attributed utilization is not full-device usage. MIG/partition readings are not summed into physical-device totals; potentially repeated parent sensors remain unknown. AMD VRAM byte conversion and Intel metrics are not implemented. Runtime exporter versions and coverage remain unknown until independently verified.

Kueue observation discovers supported served `v1beta2`/`v1beta1` APIs and requires current observed generations for admission/active conclusions. Missing API/RBAC or failed reads mean unavailable/unknown, not an empty healthy queue. Pod pending reasons, Kueue admission, Job lifecycle, Node Ready and GPU metrics are different observations. None alone is an allocation certificate. See [observability](observability.md).

## Workload comparison is an experiment, not placement automation

The Workload advisor shows all observed Pod request budgets, including CPU-only work. Requests remain separate from measured use. Optional CPU, memory and GPU-busy windows require a custom trusted attribution adapter that is not included; stock device metrics cannot establish per-container use. Exact current Pod/container identities, supported resource modes, sample freshness, shared observation windows and conservative coverage gates are required.

A controlled CPU-comparison candidate means only that an experiment may be worth running. CPU activity must come from GPU-bearing containers in the same aligned time window, with one whole GPU per such container and no unclassified nonzero accelerator requests. Sidecar work, multi-GPU averages, missing or conflicting evidence cannot establish the candidate. Performance and cost stay unknown until repeated matched trials establish them. No workload is moved, resized, launched or migrated. See [workload advisory](workload-advisor.md).

## Ray and standalone queue exports

The HTTP Ray planner uses local whole-GPU pools and advisory policy. It builds suspended RayJobs targeting KubeRay v1.7.2, with CPU-only head/submitter and fixed-size GPU workers. The pure builder also represents explicitly named MIG profiles and renamed shared replicas, without enabling those modes in managed control. Ray logical `num_gpus` is not physical isolation.

Standalone queue exports have deterministic names but no reconciliation or ownership. They are not interchangeable with the operator's UID-derived queues and must not overwrite managed objects. No live RayJob submission, Ray dashboard authorization, KubeRay installation, admission webhook validation or GPU runtime test is included. See [Ray/Kueue integration](ray-kueue.md).

## Historical contracts and verification limits

`deploy/contracts/capabilities.schema.json` is a **legacy design artifact**, not the current capability endpoint or an authoritative schema for opt-in control. `/api/v1/capabilities` is not served. Use the current state and separate `/control/status`, `/execution/status`, `/ray/capabilities`, `/observability` and the separately authorized `/diagnostics` routes.

The namespaced draft `AllotForgePool` and `AllotForgeOwnershipPolicy` CRDs in `deploy/contracts/` are excluded from deployment roots and unwatched. They are distinct from the implemented managed CRDs. `deploy/examples/future-contracts.yaml` is illustrative, not current operator configuration.

The included [benchmark evidence](../benchmarks/README.md) covers local CPU timings over synthetic inventory, not GPU throughput or competitor superiority. Mock HTTP responses and offline exports validate tooling only. Hardware/runtime/workload/policy parity and independently repeated live runs are required before a comparative claim.

Production gates remain open: target API-server CRD/CEL and OpenShift SCC admission; supported Kueue/KubeRay versions; actual NVIDIA/AMD/Intel resource/runtime behavior; exporter identities/units; negative RBAC and bypass tests with real tenant identities; CNI and namespace isolation; API outages, Lease failover and deletion races; load/performance testing; SSO/authorization; TLS/secret rotation; image signing/scanning; durable audit/backups; operational upgrades and rollback. Passing unit tests or enabling a flag does not mark any of these passed.
