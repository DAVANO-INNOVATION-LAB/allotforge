# Read-only GPU and queue observation

`GET /api/v1/observability` and `GET /api/v1/state` → `observability` expose optional evidence from existing telemetry and queue services. Nothing here installs drivers, starts GPU work, scrapes kubelet sockets, changes GPU configuration, submits jobs, or writes to Kubernetes. Demo mode creates no synthetic device telemetry.

## Opt-in configuration

The server uses these settings only with `DAVANO_SOURCE=kubernetes`:

| Setting | Meaning |
| --- | --- |
| `DAVANO_PROMETHEUS_URL` | Existing HTTPS Prometheus origin, for example `https://prometheus.internal.example`. No path, query, or embedded credentials. Unset disables telemetry. |
| `DAVANO_PROMETHEUS_CA` | Existing CA certificate file; unset uses the system trust store. TLS verification cannot be disabled. |
| `DAVANO_PROMETHEUS_TOKEN_PATH` | Existing bearer-token file, re-read for rotation. Unset sends no Authorization header. Do not pass a token as a URL or browser setting. |
| `DAVANO_PROMETHEUS_CLUSTER` | Exact `cluster` label selector. Required to join cluster-labelled metrics to this Kubernetes inventory. Use a dedicated single-cluster source for unlabelled metrics. |
| `DAVANO_QUEUE_OBSERVABILITY_ENABLED` | Defaults false. Set `true` to read installed Kueue APIs with the existing Kubernetes service-account credential. |

The Prometheus origin is operator configuration, not a browser parameter. Queries are fixed to an allowlist of metric names; no arbitrary PromQL endpoint exists. Redirects are rejected to avoid forwarding tokens. Requests use GET only, have transport/query timeouts and reject oversized, malformed, warning-bearing, or incomplete responses. No response body, arbitrary metric label, workload annotation, environment variable, or controller condition message is forwarded to the browser.

Use an independently authenticated trusted-operator ingress. The fleet/observability endpoints do not implement tenant redaction. With project execution disabled, these endpoints have no application authentication. With execution enabled, unscoped routes require the separate administrator credential. A project submission token never grants fleet access or makes the fleet dashboard tenant-private. The optional queue role reads all namespaces and is not suitable for an untrusted tenant dashboard.

### Permissions

Existing inventory RBAC remains unchanged: Nodes and Pods only. `deploy/observability/queue-observer-clusterrole.yaml` is a separate **unbound review template**, not referenced by any kustomization. It grants only Kueue resource `get,list` and explicit API discovery `get`. An authorized operator must separately review and bind the role to the selected observer identity. Do not bind controller write permissions just to observe queues. No role was installed or bound while developing this feature.

Prometheus authentication, vendor exporter installation, firewall rules and scrape configuration are external prerequisites. They are not created by AllotForge. Match the exporter and driver/DCGM/ROCm versions supported by your hardware and vendor.

## Telemetry semantics

Each device entry is an **exporter-observed entity**, not a promise of exhaustive physical inventory or an assignment to a workload. It preserves exporter UUID evidence, selected identity labels, source adapter, model when reported, exact node-label matching result, and MIG GPU/compute instance or AMD partition/VF identity. A GPU ordinal alone never becomes a fabricated UUID. Parent devices and child entities are not summed. Unmatched and ambiguous hostnames remain explicit. Different `cluster` labels never merge; without the configured selector they cannot join the Kubernetes nodes.

Each metric contains:

- `value`: a number only for accepted fresh evidence; otherwise `null`.
- `status`: `fresh`, `stale`, `unknown`, or `conflict`.
- `sample_at`: the actual source sample timestamp, not the HTTP request time.
- `unit`, `source_metric`, `scope`, and `reason`.

The fixed query uses a raw 15-minute range selector through Prometheus `/api/v1/query`. Ordinary instant-vector evaluation timestamps are not used as scrape freshness. The latest source sample is fresh for at most 120 seconds, with a 5-second future-clock tolerance. Older values remain `stale` with `value=null`. Once a series leaves the 15-minute window the entity may disappear; coverage remains `unknown`, never “all devices healthy.” There is no historical database or freshness guarantee beyond this bounded observation window.

Missing metrics, unsupported sentinel values, non-finite/out-of-range values, future timestamps and conflicting duplicate series do not become zero. Equal duplicate/alias values can merge, but conflicting fresh values are not averaged. Actual observed zero remains zero. Workload-attributed utilization/memory is not presented as device-wide usage. Repeated parent thermal/power sensors are not presented as independent MIG-instance measurements. No overall GPU health verdict is inferred from temperature, ECC or Kubernetes Node Ready.

### Implemented exporter contracts

- **NVIDIA DCGM Exporter:** selected legacy 4.8.x and 4.9.0 names for utilization, used/total framebuffer memory, GPU temperature, power and legacy volatile uncorrected ECC. Legacy percent/MiB and new ratio/byte names have explicit per-name conversion, not guessed units. Missing total-memory/ECC metrics remain unknown. Both legacy `Hostname` and current `hostname`, `UUID`/`uuid`, MIG instance IDs and compute-instance IDs are handled. Runtime exporter version is **unknown** unless separately checked by an operator; name compatibility is not hardware certification.
- **AMD Device Metrics Exporter 1.5.3:** selected GFX activity, junction temperature, power usage and uncorrected ECC metrics, with the documented optional `amd_` prefix. Enable the exporter `GPU_UUID` label; GPU index alone is insufficient. AMD partition/VF sensor semantics are conservatively unknown. AMD VRAM byte conversion is deliberately not implemented, rather than guessing the exporter’s MB convention. Edge versus hotspot temperatures are not silently equated.
- **Intel:** Kubernetes resource inventory remains supported, but a telemetry adapter is not implemented.

Adapter status `implemented-unverified-on-hardware` means parser/transport contracts are tested with synthetic inputs. It does not mean the deployment, exporter, driver or GPU has been tested.

## Queue and pending-workload evidence

Pod workload rows include bounded, identifier-only condition reasons and waiting-container reasons. For example `Unschedulable` and `ImagePullBackOff` originate from Pod status. Free-form controller messages are omitted to avoid credential/private-content leakage. A Pending phase alone yields `NotBound` or `BoundPending`, not an invented diagnosis of GPU quota exhaustion. Detailed scheduler Events remain available to the operator through Kubernetes, not this endpoint.

When enabled, the Kueue reader discovers served API versions and uses `v1beta2`, falling back to `v1beta1` only if served. Discovery must advertise listable ClusterQueues, LocalQueues and Workloads. Admission/Active/Finished booleans require a condition whose `observedGeneration` equals object `generation`; missing or stale generation yields `null`. Queue pending/reserving/admitted counts retain unknown values when absent. A queue label on a Pod does not prove queue installation or admission.

Kueue Workloads and Pods have separate identities. Their names are not joined heuristically. Conditions report Kueue’s observations, not scheduler placement, exclusive allocation, or GPU execution. List requests are non-atomic. Missing API/RBAC/transport produces `unavailable`, not an empty connected queue or a synthetic fallback. Optional queue/telemetry failure does not erase working Node/Pod inventory.

## Verification and limits

Run `python -m unittest discover -s tests -p 'test_observability*.py' -v` plus the full repository checks. Tests cover raw timestamps, old/new units, stale/invalid/zero distinctions, duplicate conflicts, MIG/AMD identity, workload versus device scope, cluster separation, token rotation, fixed GET-only queries, error sanitization, queue API discovery, generation checks, unavailable states and demo isolation.

Five additional integration tests run an ephemeral loopback HTTPS server with synthetic data and verify real TLS validation, GET transport, redirect rejection, stale-source handling and error-body sanitization. OpenSSL creates test-only temporary certificates; these tests skip with an explicit reason if the CLI is absent. All automated inputs are labelled synthetic test fixtures. No real GPU, exporter, existing Kubernetes cluster or Prometheus account was contacted. To certify a deployment, an authorized operator must run positive/negative RBAC tests, verify version-pinned exporter output on real hardware, test scrape failure and clock skew, compare UUID/partition mappings against vendor tools, and validate per-process/time-sharing/MIG behavior. This is an implemented observation adapter, not a completed hardware integration test.

## Upstream references checked 2026-11-11

- [NVIDIA DCGM Exporter releases](https://github.com/NVIDIA/dcgm-exporter/releases): 4.9.0 changes default names/units. AllotForge normalizes each supported name explicitly.
- [NVIDIA exporter metrics and labels](https://docs.nvidia.com/datacenter/dcgm/latest/reference/dcgm-exporter-metrics.html): exporter coverage depends on selected fields, hardware and configuration.
- [AMD 1.5.3 metric meanings](https://instinct.docs.amd.com/projects/device-metrics-exporter/en/release-v1.5.3/configuration/metricslist.html) and [configuration](https://instinct.docs.amd.com/projects/device-metrics-exporter/en/release-v1.5.3/configuration/configmap.html).
- [Kueue v1beta2 API](https://kueue.sigs.k8s.io/v0.17/docs/reference/kueue.v1beta2/): conditions describe observed state, not execution guarantees.

## Bounded pending-workload visibility

This is a second, independent opt-in layered on the existing Kueue observer. Set both `DAVANO_QUEUE_OBSERVABILITY_ENABLED=true` and `DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED=true`, then configure `DAVANO_KUEUE_PENDING_LOCAL_QUEUES=research/team-queue,training/batch-queue`. The comma-separated server-side allowlist requires 1–10 unique `namespace/LocalQueue` pairs with Kubernetes DNS names. Invalid, duplicate, empty, oversized or path/query-bearing targets are rejected. No browser parameter changes the scope. This does not narrow the pre-existing basic Kueue observer's cluster-wide resource lists; keep its trusted-operator access boundary.

The separate visibility reader discovers `visibility.kueue.x-k8s.io`, selects served `v1beta2` or `v1beta1`, and requires discovery to advertise namespaced `localqueues/pendingworkloads` with the `get` verb. It issues only `GET /apis/visibility.kueue.x-k8s.io/<version>/namespaces/<namespace>/localqueues/<queue>/pendingworkloads?limit=100&offset=0`. There are no cluster-wide visibility lists, retries, continuation requests, or controller writes. Existing authenticated HTTPS transport rejects redirects, verifies CA trust, reads at most 8 MiB per response, and uses a 10-second transport timeout. Each snapshot makes at most two discovery calls plus one call per target (at most 12). A 12-second elapsed-time budget is checked before and after each call; late results are rejected and no further call starts after expiry. This is a soft between-request budget, not a hard wall-clock cancellation: a call already underway can overrun it by its transport timeout, and the standard-library timeout is an I/O timeout rather than a whole-body deadline. No background work is spawned. Basic queue observation costs are separate.

`queues.pending_visibility` contains `status`, sanitized `detail`, discovered `api_version`, retrieval `observed_at`, fixed `limit` and `offset`, and `targets`. Status is `disabled`, `invalid-config`, `unavailable`, `partial`, or `connected`. Each target has `namespace`, `name`, `status` (`observed`, `partial`, `unavailable`), `detail`, retrieval `observed_at`, `returned_count`, and `coverage` (`bounded-window` or `unknown`). **Returned count is only the number of entries in that response window, never a total backlog.** Upstream returns an ObjectMeta summary, not a paginated Kubernetes ListMeta with a continuation token or total. A window reaching 100 entries is marked partial and cannot provide Workload-bound positions. Missing, malformed, duplicate, conflicting or failed summaries are unavailable, not empty healthy queues. Failure of this optional reader preserves existing basic queue observations.

Each normalized Workload exposes its own UID (when present) and `pending_position`: `status` (`unknown` or `observed`), `reason`, `local_queue_position`, `cluster_queue_position`, `observed_at`, `source=kueue-visibility`, and `position_base=0`. Unknown values are null, never zero. Both position fields are explicitly present int32 values in upstream, starting at zero; an omitted key is unknown. UI labels retain this zero-based meaning. The backend requires a unique exact Workload UID, namespace, name, and LocalQueue match, as well as current-generation negative Admitted and QuotaReserved conditions and no current Finished=true. Duplicate Workload identities or stale/unconfirmed generation cannot establish a position. No Pod join is performed.

**Important upstream limitation:** Kueue v0.20.5 `newPendingWorkload` copies Workload name, namespace, creation timestamp and owner references, but does **not** copy the Workload UID into summary metadata. Consequently normal upstream responses yield useful bounded queue-window counts but **unknown Workload positions**. The owner reference UID identifies a different object and is never substituted; matching names, timestamps or priority do not establish identity. The positive UID-binding tests exercise synthetic evidence including an explicit Workload UID, not a claim that stock Kueue emits that evidence. No private labels, arbitrary controller messages, raw owner references are exposed. Only a narrow sanitized summary representation is exposed, as described below.

Retrieval timestamps mark when evidence was fetched, not when Kueue last updated its queue cache. The API supplies no cache freshness guarantee and reads are not atomic with the Workload list. Positions can change immediately and are neither an ETA, predicted admission order, start-time guarantee, nor GPU placement or ownership evidence. Demo mode never connects or fabricates pending positions, even with environment settings present.

`deploy/observability/pending-visibility-role.yaml` is an unbound, unapplied review template: a namespaced Role grants only `get` on selected LocalQueue pending subresources with explicit `resourceNames`; a separate ClusterRole grants only exact discovery URLs. It contains no RoleBinding or ClusterRoleBinding and is not in any kustomization. Operators must review actual API/RBAC and explicitly authorize bindings for their observer identity. No visibility service, credentials, APF resources or production permissions are installed by this feature.

### Visibility sources and verification

Checked 2026-11-11:

- [Official on-demand pending-workload documentation](https://kueue.sigs.k8s.io/docs/tasks/manage/monitor_pending_workloads/pending_workloads_on_demand/).
- [Pinned v0.20.5 summary types and limit/offset options](https://github.com/kubernetes-sigs/kueue/blob/v0.20.5/apis/visibility/v1beta2/types.go).
- [Pinned LocalQueue filtering, limit and offset implementation](https://github.com/kubernetes-sigs/kueue/blob/v0.20.5/pkg/visibility/storage/pending_workloads_lq.go).
- [Pinned metadata copy and zero-based position construction](https://github.com/kubernetes-sigs/kueue/blob/v0.20.5/pkg/visibility/storage/utils.go).

Run `python -m unittest discover -s tests -p 'test_pending_visibility.py' -v` alongside observability and full repository tests. Synthetic cases cover served-version/read-verb discovery, fixed namespace targets, count/call/deadline bounds, UID omission and mismatch, stale generation, duplicates, malformed/missing rank, 403/404 errors, redaction, optional failure isolation and demo isolation. No live Kueue deployment, production permissions, actual scheduling behavior or hardware is verified by these tests.

### Unbound summary entries

Each valid target also exposes `reported_entries`: namespace, name, explicit API-reported `local_queue_position` and `cluster_queue_position`, `position_base=0`, `identity_status=unverified`, `source=kueue-visibility-summary`, and retrieval `observed_at`. These entries are separate from Workload cards and never linked or joined by name. They show what the summary reported without claiming the currently listed Workload has that position. Identity stays unverified even if a summary includes UID. A validated full 100-entry window can display these unbound entries with partial coverage, while Workload-bound positions remain unknown. Malformed/conflicting/error responses expose no entries. Never interpret this display as an ETA, admission order guarantee, workload execution or GPU ownership.
