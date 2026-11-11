# AllotForge

**Plan resources with evidence, not guesswork.**

AllotForge is the new product name for this GPU resource console. The existing `davano` Python module, `DAVANO_*` configuration variables, `davano.dev/*` labels and `fleet.davano.io` CRDs remain supported unchanged for upgrade compatibility. The new `allotforge` command starts the same application.

AllotForge combines GPU resource inventory, local advisory pools and policy, RayJob manifest planning, optional exporter/queue observation, and separately enabled Kubernetes control, namespace GPU-ceiling edits and constrained Job execution/reassignment. The UI and API run together. Version **0.7.0 is a development prototype**, not a production-certified GPU platform. License selection is pending.

**The default is synthetic demo data with no cluster credentials or cluster writes.** Live inventory is read-only. Cluster mutation requires separate opt-in configuration and permissions; installing the inventory console does not install its operator. No live GPU cluster, device exporter or competitor platform was exercised in this build.

## Run it locally

Python 3.11+ is required. No Node build step, GPU, cloud account or Kubernetes cluster is required.

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
python -m davano.app
```

Open **http://127.0.0.1:8080**. Interactive API documentation is at **http://127.0.0.1:8080/docs**. The default server binds only to loopback. Local edits survive restarts in `.data/demo.db`.

The default inventory/advisory workspace has no application authentication. Keep it on localhost or behind an independently reviewed authenticated proxy. Optional administrator and project bearer tokens are limited application identities, not SSO or a production tenancy boundary. See [SECURITY.md](SECURITY.md).

Try the demo:

1. Inspect the fleet and open a node's resource breakdown.
2. Create a node-label pool. This stores local metadata; it does not modify node labels.
3. Add a local policy for a namespace and pool.
4. Check workload fit. In the seeded fixture, `training` + `research` + NVIDIA + 4 GPUs fits `gpu-h100-02`; 8 GPUs does not. Add 8,000 CPU millicores and 32,768 MiB memory to check the complete single-node request budget. Increase CPU to 12,000 millicores to see exactly why it is blocked. The response explicitly states **not allocated, not submitted**.
5. Inspect integration capabilities and Device telemetry. Ray manifest planning is available, Kueue control is available but disabled, and Slurm is not implemented. Demo mode fabricates no GPU telemetry or queue admission results.
6. Open Deployment diagnostics and run its demo report. Configuration is shown separately from observations; no external requests are made in demo mode.
7. Open Workload launch to inspect its disabled-by-default boundary. Real submission requires a separately configured project identity and approved profile; it is not enabled by the demo.

The fixture includes explicitly synthetic CPU/RAM budgets and a CPU-only preprocessing Pod so the fit check accounts for non-GPU workloads too. The AMD node intentionally omits CPU allocatable evidence to demonstrate an unknown result. These values are not hardware specifications.

The fixture has 36 labelled whole-GPU resource units and a separate shared-replica node. Shared replicas never enter the whole-GPU total. One node is NotReady; its unrequested slots remain inventory, but it is excluded from the fit evaluator.

## Implemented capabilities

| Area | Implemented scope | Default and boundary |
| --- | --- | --- |
| Inventory and local workspace | Kubernetes-shaped demo, optional Node/Pod reads, SQLite pools/policies, GPU/CPU/memory request accounting and explainable single-node fit checks | Demo by default. Local policy remains advisory and does not reserve resources. |
| Ray planning | Validated suspended RayJob export plus pure Kueue manifest builders | Planning only; no live RayJob submission or Ray lifecycle management. |
| Workload advisor | All-Pod request monitoring, optional current-container cAdvisor CPU/memory observations, or separately configured custom CPU/memory/GPU evidence | Read-only and off by default. cAdvisor requires preserved source timestamps and cannot advise CPU versus GPU. GPU attribution still needs a trusted custom adapter. |
| Observability | Pod pending reasons, optional read-only Kueue conditions, scoped pending-summary observations and selected NVIDIA/AMD Prometheus metrics | External sources off by default. Missing, stale or conflicting evidence remains unknown; Intel telemetry unsupported. |
| Deployment diagnostics | On-demand bounded inventory reads, dependency API discovery and a fixed constant Prometheus query | Demo makes no external requests. Live diagnostics always require an administrator token; no aggregate readiness or enforcement claim. |
| Managed control | GPUPool/GPUAccessPolicy CRDs and a separate controller for owned Kueue queues, quota and native admission guards | Control API off by default; separate operator installation and consequential RBAC required. No automatic tenant workload grants. |
| Project execution | Project-scoped preview, submission, observation, cancellation and local audit for constrained single-Pod Kubernetes Jobs | Off by default. Administrator-approved digest-pinned images and fixed commands only; not arbitrary code or Pod-template submission. |
| Resource assignment | Administrator GPU quota/per-Pod ceiling edits and queued-Job replacement within or across authorized projects | Off by default. Same-pool policy edits only; workload changes cancel and recreate an eligible suspended Job, not live migration. |
| Benchmark tooling | Local CPU smoke timings, offline scheduler manifests/imports, explicit HTTP inference measurements and evidence comparisons | No automatic installs or deployment. Included results are not live GPU or competitor benchmarks. |

Other foundations include strict FastAPI inputs, source-separated SQLite databases, verified HTTPS transports with no redirects, conservative whole-GPU classification, ownership/precondition checks, and private non-root Kubernetes/OpenShift deployment source. These are implemented safeguards, not a claim that the system is safe for untrusted production tenants.

## What is still missing

No live workload migration, checkpoint/data transfer, arbitrary existing-workload adoption, physical GPU movement, cross-policy atomic quota transfer, namespace CPU/memory-ceiling editor, SSO/OIDC, general interactive notebooks or shell/exec access, arbitrary workload templates, live RayJob submission, Slurm integration, cluster provisioning, GPU driver/device-plugin installation, MIG/MPS/time-slicing reconfiguration, autoscaling, multi-cluster management, immutable external audit retention or HA database design. Exporter adapters do not establish device-health certification, complete device discovery or workload-to-device ownership.

A local pool is a **view**, not ownership. Policy priority is metadata, not a Kubernetes PriorityClass. “Fits” is an estimate against observed GPU requests, not a scheduler guarantee. CPU and memory request budgets are checked when supplied. Topology, storage, affinity, quotas and concurrent scheduling remain unchecked. MIG instances and shared replicas cannot be used in the whole-GPU fit evaluator or managed control path. Read [capability boundaries](docs/capabilities.md) before enabling writes.

## Assign resources to namespaces, projects and workloads

The **Resource assignment** view edits a managed namespace's GPU quota and per-Pod GPU ceiling after administrator review. Changes are bound to the current policy UID/resourceVersion and remain within its existing pool. They update desired limits asynchronously; they do not reserve devices, resize existing Pods or evict running work.

In **Workload launch**, an eligible suspended broker-owned Job can be reassigned to an authorized target project, including its different configured namespace/pool, or replaced with different approved GPU/profile settings in the same project. CPU and memory come from the selected approved profile. The reviewed operation reserves a new target name, cancels the source, waits for the Job and owned Pod/Kueue children to disappear, then submits the target. This is **non-atomic cancel-and-recreate**, with explicit status/resume, not migration. Running/admitted workloads are blocked; uncertain source deletion can require administrator review without a target submission.

See [resource-assignment semantics and recovery](docs/resource-assignment.md). These paths are tested with local API doubles, not a real scheduling or GPU-movement demonstration.

## Observe bounded pending-queue evidence

The queue view can separately observe up to ten explicitly configured LocalQueue pending windows. It shows the API-reported zero-based positions as **identity-unverified summary entries**, never an ETA or guaranteed admission order. Stock Kueue omits Workload UID from these summaries, so they remain separate from actual Workload admission records. Missing, malformed, partial or unauthorized evidence stays explicit. No new role is automatically bound. See [configuration, identity limits and read permissions](docs/observability.md#bounded-pending-workload-visibility).

## Monitor workloads before choosing CPU or GPU

The **Workload advisor** separates CPU/RAM/GPU requests from measured runtime activity for every observed Pod, including CPU-only jobs. The optional [cAdvisor CPU/memory source](docs/cadvisor-cpu-memory.md) matches current Pod and container identities in full cgroup paths and requires an explicitly asserted timestamp-preserving pipeline. That mode leaves GPU activity and processor suitability unknown. The separate custom-adapter mode can suggest a controlled CPU comparison only from sufficiently fresh, aligned CPU/GPU evidence. Device-wide GPU signals, workload names and resource requests alone never establish which processor is better. Cost and performance remain unknown until matched trials are measured; no workload is automatically moved. See the [adapter contract, coverage gates and experiment checklist](docs/workload-advisor.md).

## Kubernetes/OpenShift

The deployment package is source for review, **not an already deployed or certified system**. No container image is published by this bundle.

```sh
# Local image build, if Docker is installed:
docker build -t davano:dev .
# Render source for review, if kubectl is installed:
kubectl kustomize deploy/base
```

- `deploy/base`: demo without cluster credentials.
- `deploy/overlays/live-readonly`: opt-in `get,list` access to nodes and pods. Persistent and OpenShift inventory overlays also include this live read access.
- `deploy/operator` and `deploy/operator-openshift`: separate managed CRDs, controller and reviewed write authority. See [control-plane setup and release gates](docs/control-plane.md).
- `deploy/control-api`: optional administrator API over managed CRDs; requires a pre-provisioned secret and operator setup.
- Project execution requires its own configuration, namespace isolation, runtime identity and broker permissions. See [constrained execution](docs/constrained-execution.md). Allocation edits additionally need the narrowly scoped permissions in the review-only `deploy/examples/allocation-api-rbac.yaml`; they are not granted by the base console.

The old draft CRDs in `deploy/contracts/` are not the managed `fleet.davano.io` API and are not deployed by these roots. Do not mix them with the operator's CRDs.

See [inventory deployment instructions](docs/deployment.md) for storage, SCC, RBAC and networking. The build environment had no Docker, kubectl, kind or live GPU cluster. Image execution, Kustomize rendering, API-server admission, actual Kueue/KubeRay behavior and target-cluster compatibility remain **unverified**.

### Configuration and opt-ins

In-cluster inventory uses standard `KUBERNETES_SERVICE_HOST`, `KUBERNETES_SERVICE_PORT_HTTPS`, the mounted service-account token and CA. Outside a cluster, configure an HTTPS API origin and existing credential files. Do not put Kubernetes or Prometheus tokens in command-line arguments or browser code.

| Variable | Default / purpose |
| --- | --- |
| `DAVANO_SOURCE` | `demo`; `kubernetes` enables live inventory reads |
| `DAVANO_DB_PATH` | `.data/demo.db` or `.data/kubernetes.db`; container `/data/davano.db` |
| `DAVANO_BIND` / `PORT` | `127.0.0.1` / `8080`; container binds `0.0.0.0` behind private networking |
| `DAVANO_K8S_API` | HTTPS origin; otherwise in-cluster service origin |
| `DAVANO_K8S_TOKEN_PATH` / `DAVANO_K8S_CA` | Standard mounted service-account token and CA paths |
| `DAVANO_CONTROL_ENABLED` | `false`; administrator CRD API, requires live mode and `DAVANO_API_TOKEN` of at least 32 characters |
| `DAVANO_EXECUTION_ENABLED` | `false`; constrained project Job API, requires live mode and an administrator config file |
| `DAVANO_EXECUTION_CONFIG_PATH` | No default; private configuration for project identities, pinned object UIDs and approved profiles |
| `DAVANO_PROMETHEUS_URL` | Unset; opt-in verified HTTPS Prometheus origin in live mode |
| `DAVANO_PROMETHEUS_TOKEN_PATH` / `DAVANO_PROMETHEUS_CA` | Optional credential file and custom CA for Prometheus |
| `DAVANO_PROMETHEUS_CLUSTER` | Optional exact cluster label selector; needed for safe labelled/federated node association |
| `DAVANO_WORKLOAD_ADVISOR_ENABLED` | `false`; optional workload observations under the selected source contract, never device-to-container relabeling |
| `DAVANO_WORKLOAD_METRICS_SOURCE` | `custom-adapter`; choose `cadvisor-cpu-memory` for the separate CPU/RAM-only source |
| `DAVANO_CADVISOR_TIMESTAMPS_PRESERVED` | `false`; cAdvisor requires explicit administrator assertion of preserved original timestamps before any source query |
| `DAVANO_QUEUE_OBSERVABILITY_ENABLED` | `false`; live read-only Kueue API discovery and queue/workload conditions, requires extra read RBAC |
| `DAVANO_KUEUE_PENDING_VISIBILITY_ENABLED` | `false`; separately enables bounded LocalQueue pending-summary reads when basic queue observation is enabled |
| `DAVANO_KUEUE_PENDING_LOCAL_QUEUES` | No default; 1–10 explicit `namespace/queue` targets, comma-separated, with separately reviewed GET-only RBAC |

When project execution is enabled, unscoped inventory, advisory, Ray-planning and observability routes require the separate administrator bearer token. Project tokens do not unlock those routes. An absent/short administrator token leaves them inaccessible rather than public. Public capability-status routes contain no project credentials. Keep all modes private behind reviewed TLS/authentication.

Live inventory never falls back to fixtures. `/healthz` reports process health; `/readyz` reads the inventory source and fails closed on errors. Neither certifies GPU health, telemetry freshness, controller readiness or execution prerequisites. Node and pod lists are non-atomic snapshots.

Resource name alone does not prove a whole GPU. Sharing/MIG evidence takes precedence. `davano.dev/allocation-mode=whole-gpu` is an administrator assertion, not a hardware detector. Validate the installed device plugin before using it; AllotForge never applies node labels itself.

## API and architecture guides

```sh
curl http://127.0.0.1:8080/api/v1/state
curl -X POST http://127.0.0.1:8080/api/v1/evaluate \
  -H 'Content-Type: application/json' \
  -d '{"pool_id":"training","namespace":"research","gpu_count":4,"vendor":"NVIDIA"}'
curl http://127.0.0.1:8080/api/v1/control/status
curl http://127.0.0.1:8080/api/v1/execution/status
```

- [API contract and authentication boundaries](docs/api-contract.md)
- [GPU, CPU and memory advisory fit](docs/resource-fit.md)
- [0.7.0 candidate notes](docs/releases/0.7.0.md)
- [0.6.0 release notes](docs/releases/0.6.0.md)
- [0.5.0 release notes](docs/releases/0.5.0.md)
- [Deployment diagnostics](docs/deployment-diagnostics.md)
- [0.4.0 release notes](docs/releases/0.4.0.md)
- [0.3.0 release notes](docs/releases/0.3.0.md)
- [Managed control plane](docs/control-plane.md)
- [Constrained project execution](docs/constrained-execution.md)
- [Namespace allocation and queued workload reassignment](docs/resource-assignment.md)
- [Ray and Kueue manifest planning](docs/ray-kueue.md)
- [Observability sources and limitations](docs/observability.md)
- [Benchmark methodology and actual results](benchmarks/README.md)
- [Offline GPU smoke verification](benchmarks/README.md#offline-gpu-smoke-evidence) and [future collector acceptance tests](docs/gpu-validation.md)
- [cAdvisor CPU/memory configuration and limits](docs/cadvisor-cpu-memory.md)

Local `/pools` and `/policies` mutate only SQLite. Managed `/control/*` routes are a separate authority. `/evaluate` and `/ray/plan` never submit; the explicitly enabled `/execution/*` API can create real Jobs. Submission is not admission, allocation or successful GPU execution.

## Validate

```sh
python -m unittest discover -s tests -v
python -m compileall -q davano benchmarks
node --check web/app.js   # optional syntax check if Node is present
(cd web && npm ci && npm test)  # optional DOM/API regression checks
python -m benchmarks.harness --help
```

Unit, mocked-API and static-manifest tests exercise implementation boundaries. They do not replace real API-server, identity/RBAC, CNI, controller, image or GPU validation. `scripts/verify_native_admission.py` is restricted to the disposable `kind-davano-ci` context and runs in the exact-commit release gate against a disposable kind cluster. The companion diagnostic parser smoke uses fixed kubectl GETs against that same API server; it does not validate application authentication, production RBAC or GPU behavior. [Historical verification](docs/verification.md) covers the initial 0.1.0 snapshot; use the current test output for subsequent changes.

The included benchmark result files measure real local CPU timings over synthetic inventory only. They do not measure GPU throughput, queue admission, live inference or competitor performance. The harness rejects unsupported comparative conclusions rather than presenting replay or mock data as a live result.

## Project structure

- `davano/`: inventory API, advisory evaluation, managed controller, execution broker, observability and SQLite store.
- `web/`: static UI served by the application.
- `fixtures/`: explicitly synthetic Kubernetes-shaped data.
- `deploy/`: separate inventory, operator, control API and illustrative configuration manifests.
- `benchmarks/`: offline adapters, bounded measurement tools, methodology and scoped results.
- `tests/`: repeatable unit, API-double and static checks without a cluster.
- `docs/`: contracts, authority boundaries, deployment and verification notes.

## Contributing and license

Keep source, freshness and authority visible. Add tests with every accounting, authorization and controller change. Do not call request accounting “utilization,” combine MIG/shared units into physical GPU totals, infer exclusive ownership from labels, or call an accepted object proof that admission or GPU execution succeeded. Run the complete test suite and applicable UI checks before proposing changes.

Do not commit credentials, private identity configuration, database files, live inventory exports or manifests containing secrets. This repository is public for source review. **No open-source license grant is asserted**; license selection remains pending. Public visibility alone does not grant permission to reuse or redistribute the code.
