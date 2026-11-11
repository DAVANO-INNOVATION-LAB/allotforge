# Constrained project execution

This opt-in broker implements **real Kubernetes batch Job create/read/cancel APIs**, but no live Kubernetes or GPU execution has been validated in this environment. Default demo mode cannot submit. Enabling the broker is a separate administrator-reviewed deployment action, not an automatic upgrade.

Automatic tenant Kubernetes workload grants remain disabled. Project tokens authorize only AllotForge's constrained broker; GPUAccessPolicy subjects still grant no RBAC access. This is not a general multi-tenant execution platform, organization SSO, hardware isolation system, notebook service, distributed training scheduler, or arbitrary YAML runner.

## Allowed workflow

1. Authenticate with an individually provisioned project bearer token. Only its assigned projects are returned. Viewer identities can inspect and preview; submitters can also submit and cancel.
2. Select an administrator-approved profile and request a workload name plus a whole-GPU count. Input cannot supply an image, command, environment, mount, Secret, ServiceAccount, Pod template, namespace, scheduling bypass, or networking setting.
3. Preview the fixed Job manifest and preflight results. Preview writes nothing and reserves nothing.
4. Submit the same request. All preflights are repeated, then a durable intent and ownership record are saved before Kubernetes is called.
5. Poll the Job. `Suspended`, `Pending`, `Running`, `Succeeded`, `Failed`, and `Deleting` reflect Job fields only. Submitted does not mean admitted, scheduled, allocated, or GPU-healthy.
6. Cancel with the Job's exact UID and resourceVersion. Cancellation requests foreground deletion; the response does not claim deletion has completed. A changed or replaced object is refused.

A retry with the same name and input returns the owned Job rather than submitting twice. A lost response is resolved through the persisted operation identity. Pending retries reject changed resolved profiles/project identities/pool or policy specifications. Previously used completed/deleted names cannot be silently reused. A different name is a new execution, not an automatic retry.

## Configuration and deployment review

Required settings:

- `DAVANO_SOURCE=kubernetes`
- `DAVANO_EXECUTION_ENABLED=true`
- `DAVANO_EXECUTION_CONFIG_PATH` pointing to a mounted administrator-controlled JSON file
- `DAVANO_DB_PATH` pointing to **persistent**, backed-up SQLite storage. The base demo `emptyDir` is not suitable. Run one broker replica with Recreate strategy. Do not share an SQLite file over arbitrary network filesystems.

`deploy/execution/config.example.json` is deliberately invalid until real approved image digests, identity hashes and Kubernetes UIDs are inserted. It is not a ready-made credential. Provision at least 32 high-entropy random characters per token, store only its SHA-256 hash in the configuration, distribute the token securely, and restart the broker to apply configuration/rotation. Token identity is configured server-side and cannot be asserted by request parameters. These static identities are not OIDC/SSO or federated team membership.

Each project pins the namespace, GPUAccessPolicy, GPUPool and runtime ServiceAccount UIDs. One project occupies one dedicated namespace. Rename or replacement requires an explicit configuration review. Profiles pin a SHA-256 image digest, fixed command, CPU/memory, GPU maximum and execution deadline. Digest pinning is not image provenance or signature verification: administrators must review images and their runtime behavior.

Review-only source is in `deploy/execution/`:

- `project-template.yaml`: dedicated restricted namespace, unprivileged runtime ServiceAccount with no bindings, deny-all NetworkPolicy, native Job-count quota, narrowly scoped broker Role/RoleBinding.
- `broker-read-template.yaml`: broker identity and read-only prerequisites. Node/pod inventory is cluster-wide read access and must be reviewed. UID-derived policy/queue names must be replaced with exact existing names.
- `deployment-patch.yaml`: optional patch requiring a precreated persistent volume claim and configuration Secret; never uses the default transient demo database. It does not enable administrator control writes.

No templates in this directory are installed by a default Kustomize overlay. Do not apply them unchanged. The broker Role's Job creation permission is a trusted service privilege, not a permission to grant to a tenant. The runtime ServiceAccount receives no Kubernetes API authority and its token is never mounted.

When execution is enabled, legacy inventory/advisory/Ray/observability `/api/v1/*` routes require the administrator `DAVANO_API_TOKEN`; with no valid administrator token those unscoped routes fail closed. Execution routes have separate project authentication. Control routes retain their own explicit enablement/admin authentication. Health and configuration-status endpoints remain available. Use reviewed TLS/private ingress and network controls; bearer tokens must never be sent over untrusted plain HTTP.

## Preflight and template boundary

Preflight requires:

- Namespace UID, project label and Pod Security Admission `restricted` with `enforce-version=latest`.
- Pinned runtime ServiceAccount UID, `automountServiceAccountToken=false`, and no referenced Secrets/imagePullSecrets.
- Exactly one reviewed namespace NetworkPolicy with empty Pod selector and no ingress/egress allows. Other policies are additive in Kubernetes, so their presence is refused.
- An observed `count/jobs.batch` ResourceQuota matching project `max_jobs` (default 20, configurable 1–100). This bounds suspended Jobs as well as active ones.
- Current-generation policy guardrails, disabled legacy workload grants, ready pool and controller observation within five minutes.
- Exact ownership and semantically matching native quota/admission/queue objects, allowing only documented harmless API defaults. Admission policies must be type-checked; LocalQueue and ClusterQueue must be active, and the ClusterQueue cannot be on Hold.
- Fresh node inventory with whole-GPU eligibility, profile/policy/pool GPU bounds, and at least one node with sufficient total GPU capacity. This is not a free-capacity reservation.

The generated Job starts suspended in its assigned Kueue LocalQueue, has one Pod/one container, no retries, fixed execution deadline, scheduler placement through protected labels, no host namespaces, no token, no environment or mounts, no privilege escalation, dropped capabilities, runtime-default seccomp, nonroot UID/GID 65532 and read-only root filesystem. CPU/memory/GPU requests equal limits. Runtime assets must be baked into the reviewed image. The baseline intentionally offers no model downloads, dataset storage, DNS/network egress, or arbitrary user code. Private image pulls needing Secrets and OpenShift SCCs that disallow the fixed UID need a separate reviewed design, not removal of guards.

Kubernetes admission defaults and quota/controller observations are checked through API objects; that does not prove CNI enforcement or admission cache activation. Administrative changes and webhook mutations remain trusted-cluster concerns. Node state can change after observation. Real GPU, host and cross-tenant isolation must be validated separately.

## Audit, limits and operations

Execution intent/result metadata is persisted in SQLite without tokens, image commands, environment values or Kubernetes response bodies. Each operation reserves its result slot before mutation. A process crash leaves a `pending` result, which is evidence of uncertainty, not proof the action failed. Accepted POST and DELETE responses are not proof a Pod ran or deletion completed. Persist and back up the registry with the audit data; losing it intentionally loses authority to adopt existing Jobs.

Each project retains at most `history_limit` workload names (default/max 1000) and 10,000 audit records. Reservations are transactionally bounded. New submissions/mutations stop when capacity is reached. There is no automatic history purge or public audit-delete API. Administrators must archive records after reconciling uncertain actions and preserving ownership evidence; do not delete registry rows for still-existing Jobs. The current listing returns up to 1000 owned Jobs, and audit reads return the latest 100 records.

This local log is **not immutable or tamper-evident**. Add organizational retention, identity federation, rate limiting, backups, image/runtime assurance and target-cluster tests before broader use. No automatic HA claim is made.

## API

All paths start with `/api/v1/execution`:

- `GET /status`: minimal enabled/mode/limitation information, no identity configuration.
- `GET /projects`: authorized projects and allowed profile metadata.
- `POST /projects/{project}/preview`: `{ "name": "smoke-001", "profile": "gpu-smoke", "gpu_count": 1 }`.
- `POST /projects/{project}/workloads`: same input; returns workload summary plus `submitted`, `allocated: false`, `idempotent`.
- `GET /projects/{project}/workloads` and `GET /projects/{project}/workloads/{name}`: only registry-owned workloads.
- `POST /projects/{project}/workloads/{name}/cancel`: `{ "uid": "...", "resourceVersion": "..." }`.
- `GET /projects/{project}/audit`: project-scoped redacted intent/result metadata.

Disabled mode returns 409; invalid token 401; viewer mutation 403; inaccessible project 404; stale identity/drift 409; invalid input or excessive GPU request 422; unavailable dependencies 503. Do not automatically change workload names after an uncertain result. Reload the original name and inspect its audit record first.

## Verification and remaining release gates

`python -m unittest discover -s tests -p 'test_workload_execution.py' -v` verifies constrained manifests, project isolation, admin-gated legacy routes, failure behavior, UID/resourceVersion fencing, retry reconciliation, effective configuration fingerprints, native-default compatibility and resource limits against an in-memory Kubernetes API double.

Still required on a named authorized disposable cluster: real API defaulting/admission, native Job count quota under concurrent submissions, Kueue suspend/admit/completion, actual deny-all CNI behavior, image runtime under restricted settings, service-account credential absence, UID replacement races, crash/restart recovery, cancellation completion and real GPU execution. No successful local unit test or UI screenshot substitutes for these gates.

## Resource assignment and queued replacements

See [resource assignment](resource-assignment.md) for administrator namespace ceilings and scoped, previewed cancel-and-recreate of eligible queued Jobs across configured projects or approved profiles. This does not resize running workloads or physically move GPUs. Saved operations and destination reservations share the persistent broker database.
