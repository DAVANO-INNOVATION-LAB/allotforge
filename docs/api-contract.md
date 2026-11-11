# AllotForge API v1 (0.5.0 development prototype)

Responses use JSON. OpenAPI at `/openapi.json` and `/docs` describes current input schemas. Inputs reject unknown fields and constrain identifiers/counts. Mutating requests must be same-origin and bounded to 64 KiB; POST/PUT/PATCH require `application/json`. These checks are not a substitute for authentication.

The API has **separate authority boundaries**. The default demo/advisory workspace cannot submit or allocate. Explicitly enabled control and execution routes can change real Kubernetes resources. No route guarantees a physical GPU allocation or completed workload.

## Authentication and defaults

| Route family | Default | When enabled |
| --- | --- | --- |
| Inventory, local pools/policies, evaluation, Ray planning, observability | No application authentication; trusted local/operator workspace only | If project execution is enabled, require the separate administrator `DAVANO_API_TOKEN` bearer credential; a project token is not sufficient. |
| `/api/v1/control/status` | Public configuration description, `enabled: false` | Still a public capability description, not proof of controller readiness. |
| Other `/api/v1/control/*` routes | Disabled, HTTP 409 | Live source, `DAVANO_CONTROL_ENABLED=true` and administrator bearer token of at least 32 characters required; invalid token returns 401. |
| `/api/v1/execution/status` | Public configuration description, `enabled: false` | Still public; contains no projects, credentials or workload listings. |
| Other `/api/v1/execution/*` routes | Disabled, HTTP 409 | Live source, `DAVANO_EXECUTION_ENABLED=true`, valid administrator config and scoped project bearer identity required. |
| `/api/v1/diagnostics` | Demo-only report is public and makes no external requests | Live mode always requires the administrator bearer token, even when control/execution are disabled. No project-token access. |
| `/healthz`, `/readyz`, static assets, OpenAPI | Public | Do not disclose project configuration; not a tenant identity/login system. |

Control and execution gates are independent. Enabling control does not authenticate the default inventory workspace. Enabling execution gates the unscoped inventory/advisory routes even if control is disabled: absent/short/invalid administrator credentials produce 403. Diagnostics has its own live administrator gate: missing/invalid client credentials return 401, and missing/invalid server credentials return 503. Static project `viewer` identities may read and preview; only `submitter` identities may submit, cancel or advance a reassignment. Reassignment routes require scope over both source and target projects. Unknown/inaccessible projects return 404. Tokens must be provisioned privately and used over reviewed TLS ingress. This is not OIDC/SSO.

## Inventory and local advisory workspace

### `GET /api/v1/state`

Returns `meta`, `summary`, `nodes`, `pools`, `policies`, `workloads`, `integrations`, `observability` and local `events`.

An abbreviated **synthetic demo** response is:

```json
{
  "meta": {
    "source": "demo",
    "source_label": "Demo fixture",
    "observed_at": "<inventory observation timestamp>",
    "read_only_cluster": true,
    "enforcement": "advisory-only",
    "version": "0.4.0"
  },
  "summary": {
    "nodes": 6,
    "ready_nodes": 5,
    "whole_gpu_capacity": 36,
    "whole_gpu_allocatable": 36,
    "whole_gpu_requested": 22,
    "whole_gpu_available": 14,
    "pending_workloads": 2,
    "pool_count": 3
  }
}
```

This excerpt omits warnings and collections. `meta.read_only_cluster` and `meta.enforcement` describe this inventory/advisory view, **not** whether a separate control/execution route is enabled. Demo timestamps are fixture observations, not cluster contact or GPU measurements.

- `nodes[].resources` preserves resource name, vendor, mode and capacity/allocatable/requested/available units. Node-level `gpu_*` and `summary.whole_gpu_*` fields count verified **whole-GPU resource units only**, not MIG instances or shared replicas.
- `nodes[].compute.cpu_millicores` and `nodes[].compute.memory_mib` each report `capacity`, `allocatable`, `requested` and `available`. They include CPU-only Pods and preserve unknown evidence as `null`; these are request budgets, not live utilization.
- `available` is observed allocatable minus effective active assigned-Pod requests, not utilization, reservation or guaranteed idle physical devices. Unknown counts are `null`.
- `pools` and `policies` are the local SQLite records with derived membership/accounting. Overlapping pools are not additive resources. These are not managed GPUPool/GPUAccessPolicy objects.
- `workloads` are observed Pod/resource rows, not submitted Jobs. They preserve namespace/name/resource, phase, assigned node, GPU request count, optional queue label, sanitized `conditions` and `pending_reasons`. A queue label is not admission evidence. No secret/env/Pod-template or free-form controller message is returned.
- `integrations` separates configuration from runtime proof: Kubernetes is `demo` or `connected-read-only`; Kueue control is `available-disabled` or `configured`; Ray is `planning-only`; Slurm is `not-implemented`. GPU telemetry uses its separate source status. `configured` does not mean deployed, admitted or healthy.
- `observability` carries separate optional telemetry and Kueue evidence as described below. `events` is local configuration history, distinct from execution audit.

`GET /api/v1/nodes`, `/pools`, `/policies`, `/workloads`, `/integrations` and `/observability` return their corresponding state collection/object. `GET /api/v1/overview` returns only `meta` and `summary`. State is observed on demand; lists and source observations are not atomic.

### Local configuration

`POST /api/v1/pools`, HTTP 201:

```json
{"id":"new-pool","name":"New pool","description":"Example","selector":{"davano.dev/pool":"training"},"vendor":"NVIDIA","mode":"whole-gpu"}
```

Vendor is NVIDIA/AMD/Intel/Any; mode is whole-gpu only. IDs are bounded lowercase DNS-style identifiers; an equality selector is required. This stores metadata, not Kubernetes labels or an ownership grant. Duplicate IDs return 409.

`POST /api/v1/policies`, HTTP 201:

```json
{"id":"research-new","name":"Research","pool_id":"training","namespace":"research","max_gpus_per_workload":4,"priority":"normal"}
```

Priority low/normal/high is metadata, not PriorityClass. Missing pool returns 404. Local creation responses report `enforcement: advisory-only`.

`DELETE /api/v1/pools/{id}` and `/api/v1/policies/{id}` delete only local configuration. A referenced pool returns 409. Success reports `cluster_changed: false`.

### `POST /api/v1/evaluate`

```json
{"pool_id":"training","namespace":"research","gpu_count":4,"vendor":"NVIDIA","cpu_millicores":8000,"memory_mib":32768}
```

Optional `cpu_millicores` and `memory_mib` are strict nonnegative integers. Omission or `null` skips that dimension; zero is trivially satisfied. Maximum values are 1,000,000,000 millicores and 1,000,000,000,000 MiB. Positive requests with unknown evidence cannot produce a fit.

Response includes `verdict` (`fits`, `blocked` or `unknown`), `source`, `observed_at`, `gpu_count`, `pool_id`, `policy_ids`, `candidate_nodes`, `node_checks`, `reasons` and `limitations`, plus echoed compute requests. `node_checks` reports each observed node with `id`, `status` (`eligible`, `rejected`, `unknown`) and human-readable reasons. Candidates preserve GPU fields and add `cpu_available_millicores` and `memory_available_mib`. It always returns `advisory_only: true`, `allocated: false`, `submitted: false`.

One eligible node must fit. All matching policies apply, strictest cap wins, and no matching policy blocks. NotReady, cordoned and NoSchedule/NoExecute-tainted nodes are excluded conservatively. Effective GPU requests account for init/sidecar behavior and overhead. Optional CPU/memory checks count all bound nonterminal Pods, not only GPU Pods. Affinity, storage, topology, other quotas and concurrent schedulers remain unchecked. See [accounting and unknown cases](resource-fit.md).

## Ray manifest planning

- `GET /api/v1/ray/capabilities`: target version, manifest APIs, builder/planner modes, limitations and `submission_enabled: false`.
- `POST /api/v1/ray/plan`: validates local pool/policy and observed resource modes, then returns a suspended RayJob manifest, caps, warnings and prerequisites. It always reports `submitted: false`, `allocated: false`, `advisory_only: true`. No RayJob is written.

The HTTP planner supports whole-GPU local pools; pure Python builders additionally represent explicit shared/MIG resources. A generated queue name is provisional and is not the managed controller's UID-derived queue. Full inputs, examples and limitations are in [Ray/Kueue planning](ray-kueue.md).

## Read-only observability

`GET /api/v1/observability` is equivalent to `state.observability` and returns:

- `source`, `observed_at`, `read_only: true`.
- `telemetry`: `status`, `detail`, `devices`, `adapters`, `warnings`, `coverage`, `stale_after_seconds` and `observation_window_seconds`.
- `queues`: independent `status`, `detail`, selected `api_version`, `local_queues`, `cluster_queues`, `workloads` and separately optional `pending_visibility`.

External observation is off by default and never synthesized in demo mode. Prometheus requires an explicitly configured HTTPS origin in live mode; queue reads require an explicit flag and separate read RBAC. Optional-source errors do not erase successful inventory. Statuses distinguish `disabled`, `invalid-config`, `unavailable` and, for successful telemetry reads, `connected` or `no-data`.

Each exporter entity preserves UUID evidence, node-match result, vendor, entity kind and provenance. Each metric has `value`, `unit`, `status`, `sample_at`, `source_metric`, `scope` and `reason`. Only accepted fresh evidence has a numeric value. Stale/unknown/conflicting values are `null`, not zero. Freshness uses source scrape timestamps, with a 120-second threshold and a bounded 15-minute query window; collection time is not scrape time. Coverage and runtime exporter versions remain unknown.

Queue admission/active/finished booleans require current observed-generation conditions; missing/stale evidence is `null`. No heuristic Pod-to-Workload identity join or claim of actual GPU execution is made. See [observability](observability.md) for exporter versions, metric conversions, permissions and unsupported fields.

### Optional pending visibility

`queues.pending_visibility` has `status`, `detail`, `api_version`, `observed_at`, `limit=100`, `offset=0` and configured `targets`. Each target reports namespace/name, observation status/time, `returned_count`, `coverage` and `reported_entries`. Returned count is the response window size, not total backlog. A full window is partial; no continuation or completeness claim is made.

Each reported entry exposes only namespace/name, zero-based local/cluster queue positions, source/time, `position_base=0` and `identity_status=unverified`. These are literal API summary observations, never linked to Workload actions/admission. Stock Kueue v0.20.5 omits Workload UID; matching names or owner UIDs cannot identify the current Workload.

Workload rows have an independently validated `uid` and `pending_position`. Bound positions stay unknown unless one exact UID/name/namespace/queue match and current negative admission/reservation conditions corroborate the separate read. Positive matching-UID cases in tests are synthetic, not a stock-upstream guarantee. Optional errors preserve basic queue observations. See [the complete visibility contract](observability.md#bounded-pending-workload-visibility) for call/time/size limits and configuration.

## On-demand deployment diagnostics

`GET /api/v1/diagnostics` runs independently of inventory loading. It is not part of `/state` and is not automatically polled. Demo returns configuration and explicit unverified checks without external calls. Live requires a configured `DAVANO_API_TOKEN` of 32–4096 characters and matching administrator bearer credentials; browser-supplied query parameters or targets are rejected. Invalid startup configuration still fails fast, so a process that cannot start cannot serve diagnostics.

The response has `schema_version=1`, `source`, retrieval `observed_at`, `mode`, `configured_modes`, `checks`, `summary.status_counts`, `bounds` and `limitations`. Each check has a stable `id`, `status`, `evidence`, `detail` and `next_step`, with bounded counts where applicable. Statuses are `configured`, `disabled`, `observed`, `blocked`, `unavailable` and `unverified`. No aggregate readiness score is returned.

Fixed GETs test at most one Node and one Pod per list window and only enabled dependency discovery. A successful list is evidence for that particular read, not full inventory or any write permission. Advertised resource scope/verbs are API compatibility evidence, not the caller's RBAC grants. A separately configured Prometheus source may receive the fixed constant `vector(1)` query; it reads no exporter series and proves no telemetry coverage. Namespace/workload names, tokens, raw URLs, local paths, raw responses and server error bodies are not returned.

The report caps requests, each response's bytes, list window size and between-operation elapsed time. It never follows pagination or redirects, changes permissions, installs dependencies, submits access reviews or mutates cluster resources. The time budget is not a hard operating-system cancellation guarantee. Write authorization, controller/admission enforcement, tenant isolation and runtime/hardware remain unverified regardless of successful reads. See [deployment diagnostics](deployment-diagnostics.md) for exact limits and interpretation.

## Optional managed control API

The separate operator reconciles managed CRDs. HTTP creation persists desired state, **not synchronous enforcement**. All collection reads/writes require the administrator bearer credential. No automatic tenant workload grants are issued.

| Method and path | Effect |
| --- | --- |
| `GET /api/v1/control/status` | Configuration/capability descriptions and limitations; no dependency readiness proof |
| `GET /api/v1/control/pools` | `{ "items": [...] }` of managed GPUPool objects |
| `GET /api/v1/control/policies` | `{ "items": [...] }` of managed GPUAccessPolicy objects |
| `POST /api/v1/control/pools` | Create GPUPool from `name` and validated `spec`; HTTP 201 |
| `POST /api/v1/control/policies` | Create GPUAccessPolicy from `name`, `namespace` and validated `spec`; HTTP 201 |
| `DELETE /api/v1/control/pools/{name}` | Request deletion with current `uid` and `resourceVersion` JSON preconditions |
| `DELETE /api/v1/control/policies/{namespace}/{name}` | Same preconditions; controller finalizer may retain the object |
| `POST /api/v1/control/policies/{namespace}/{name}/allocation/preview` | Validate and show same-pool namespace GPU ceiling changes without writing |
| `POST /api/v1/control/policies/{namespace}/{name}/allocation/apply` | Recheck and compare-and-swap those GPU ceilings; returns `pending-reconciliation` |

There is no general-purpose HTTP update endpoint; the allocation routes below are a narrow exception. Pool selectors require a NodeRestriction-protected label and whole-GPU resource mode. Managed policy subjects remain declarations without automatic RBAC effect. Native `GuardrailsReady` is distinct from tenant execution permission. Ownership conflicts and deletion prerequisites can block reconciliation/deletion. See [managed control](control-plane.md) and its schema/examples before using this authority.

### Namespace GPU allocation preview/apply

Both routes accept the current policy identity/revision plus desired GPU ceilings:

```json
{"uid":"current-policy-uid","resourceVersion":"42","poolRef":"training","gpuQuota":4,"maxGPUsPerPod":2}
```

`poolRef` must equal the policy's existing pool. These endpoints do not switch pools, change subjects or edit namespace CPU/memory ceilings. Preview verifies current-generation pool/guardrail observations and native ownership/spec agreement, then returns `operation: namespace-allocation`, `status: preview`, `eligible`, `before`, `desired`, `preconditions` and `limitations`.

Apply repeats validation and writes the single policy using its UID/resourceVersion concurrency preconditions. It adds the returned `policy` and reports `status: pending-reconciliation`. Controllers must still update Kueue/quota/admission resources; existing workloads are not resized, stopped or evicted. This is not a reservation or an atomic transfer between policies/namespaces. Stale revisions, cross-pool requests or observed drift are rejected rather than retried. After an uncertain write, reload the policy before issuing another request.

The control API service account needs separately approved exact-target reads/update permissions; the inventory role is insufficient. See [resource assignment](resource-assignment.md) and the review-only `deploy/examples/allocation-api-rbac.yaml`.

## Optional constrained project execution API

Detailed configuration and permissions are in [constrained execution](constrained-execution.md). Administrator configuration fixes projects, pinned namespace/policy/pool/ServiceAccount UIDs, identity token hashes and approved execution profiles. Tokens never select arbitrary namespaces, Kubernetes users or Pod templates.

| Method and path | Effect |
| --- | --- |
| `GET /api/v1/execution/status` | Public enabled/mode/authentication/profile policy, `automatic_tenant_rbac: false`, live-validation limitation |
| `GET /api/v1/execution/projects` | Authorized project IDs/namespaces, `can_submit` and allowed profiles; omits profile commands |
| `GET /api/v1/execution/projects/{project_id}/workloads` | Broker-owned Job summaries for that project, `source: kubernetes` |
| `GET /api/v1/execution/projects/{project_id}/workloads/{name}` | One registered, identity-matching Job summary |
| `POST /api/v1/execution/projects/{project_id}/preview` | Fresh prerequisite checks plus planned manifest; `submitted: false` |
| `POST /api/v1/execution/projects/{project_id}/workloads` | Submitter-only constrained suspended Job create; HTTP 201 |
| `POST /api/v1/execution/projects/{project_id}/workloads/{name}/cancel` | Submitter-only foreground deletion request with current `uid` and `resourceVersion` |
| `GET /api/v1/execution/projects/{project_id}/audit` | Latest 100 local project audit events; `retention: local-not-immutable` |

New-Job preview/submission body has only three fields:

```json
{"name":"approved-run-1","profile":"approved-profile","gpu_count":1}
```

Only administrator-approved immutable images, fixed commands and bounded resource/runtime settings are accepted. A preview is not a reservation. Submission repeats checks and creates a suspended, restricted single-Pod Job without tenant-controlled templates, volumes, environment values or token mounting. Native Kueue, quota, admission and scheduler behavior remain external dependencies.

Submission responses contain `workload`, `submitted`, `allocated: false` and `idempotent`. An accepted create reports `submitted: true`; a matching registry-backed existing Job returns `submitted: false`, `idempotent: true` instead of submitting another Job. Conflicting names or changed effective configuration are rejected. After an uncertain transport/persistence result, reload the same workload name before retrying. Do not assume an HTTP error proves no Job was created.

Job summaries include name, UID/resourceVersion, project, namespace, profile, GPU request count, observed `phase`, conditions and timestamps. `allocation_confirmed` is always false; lifecycle status is not a physical device assignment. Only persisted broker-owned identities are returned or cancelled, not arbitrary Jobs with copied labels.

Cancellation returns `deletion_requested: true`, `deleted: false` and the observed workload. It requests foreground deletion; it does not certify that the Pod stopped. Reload status before claiming completion. Native Job-count quota, workload registry history and audit capacity are bounded; limits require administrator reconciliation/archival, not silent deletion. Local audit is not immutable external retention; configure durable SQLite storage and backups.

### Queued workload reassignment

Reassignment is an explicitly reviewed **cancel-and-recreate** operation, not live migration. It may replace an eligible broker-owned suspended Job with a new Job in the same or another authorized project. Target projects can have different preconfigured namespaces/pools; the API does not move physical devices or rewrite their pool membership. Running/admitted Jobs, arbitrary existing workloads and process/checkpoint/data transfer are unsupported.

| Method and path | Effect |
| --- | --- |
| `POST /api/v1/execution/projects/{project_id}/workloads/{name}/reassignment/preview` | Verify source eligibility and target prerequisites; return reviewed target manifest and `plan_fingerprint` without mutation |
| `POST /api/v1/execution/projects/{project_id}/workloads/{name}/reassignments` | Submitter-only create/resume of the reviewed durable operation; HTTP 202 is acceptance, not completion |
| `GET /api/v1/execution/reassignments` | Latest 100 operations whose source and target are both within the identity's scope |
| `GET /api/v1/execution/reassignments/{operation_id}` | Inspect one operation, requiring both project scopes |
| `POST /api/v1/execution/reassignments/{operation_id}/advance` | Submitter-only explicit recheck/resume; may request deletion, wait, submit the target or report a blocker |

Preview body:

```json
{
  "uid":"current-source-job-uid",
  "resourceVersion":"42",
  "target_project":"approved-target-project",
  "target":{"name":"replacement-run-1","profile":"approved-target-profile","gpu_count":2}
}
```

Apply uses the same body plus `plan_fingerprint` from the reviewed preview. GPU count is explicit; CPU/memory/image/command come only from the approved target profile. In the same project the target name must differ. Existing or reserved target names cannot be adopted. Viewers may preview, but mutations require submitter authority in both projects.

The source UID/resourceVersion and observed suspended/no-execution/no-admission evidence are rechecked. The target name and effective configuration are durably reserved before guarded source deletion. The broker does not submit the target until the source Job and its owned Pods/Kueue Workloads are confirmed absent. Source deletion and target creation are not atomic, and no automatic rollback creates a substitute source.

Responses expose `id`, `phase`, source/target project and name, `detail`, `updated_at`, `source_deleted`, `source_deletion_state`, `target_submitted`, optional `workload`, `semantics: cancel-and-recreate` and `allocated: false`. Phases include `Prepared`, `WaitingForSourceDeletion`, `SubmittingTarget`, `TargetSubmitted` and `NeedsAttention`. `TargetSubmitted` means Kubernetes accepted the new Job, not that Kueue admitted it or a GPU ran.

Always inspect the phase, including after HTTP 202/200. An operation can remain waiting or need attention. If the source DELETE outcome was uncertain and the source is absent without a durably recorded accepted cancellation, automatic target submission stays blocked because prior execution cannot be excluded. Confirmed deletion followed by a target failure can be resumed through the same operation after resolving the cause. Do not invent another target name after an uncertain result. Local durable state, API-double tests and preflight observations do not prove live race-free scheduling; see [resource-assignment recovery](resource-assignment.md).

## Health, errors and verification

`GET /healthz` is process liveness. `GET /readyz` checks the inventory source, not GPU health, optional telemetry/queues, operator readiness or execution prerequisites. Kubernetes inventory errors return 503 and never fall back to demo. Optional observation failures appear in their separate status blocks.

Input/schema errors normally return 422. Disabled authority returns 409; wrong credentials 401 or, for the unscoped execution-mode gate, 403. Missing/inaccessible resources return 404; ownership/current-state conflicts 409. Control/execution transports sanitize upstream errors; persistence/indeterminate failures may be 503 and require outcome reconciliation. Check the response and current object identity before retrying writes.

Local API tests and controlled transport fixtures verify these contracts, not live target admission, GPU execution, CNI behavior, workload isolation or production readiness. No global `/api/v1/capabilities` endpoint is implemented; the similarly named JSON schema in `deploy/contracts` is a legacy design artifact.
