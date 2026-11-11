# Deployment diagnostics

The **Deployment diagnostics** screen runs an explicit `GET /api/v1/diagnostics`.
It is independent of the fleet snapshot, so a runtime inventory failure does not
prevent an administrator from inspecting a small set of deployment prerequisites.
The screen does not poll this endpoint during ordinary state refreshes.

A report is **not a production-readiness verdict**. It separates configured modes,
observed bounded reads or API discovery, and things this feature cannot verify.
There is no aggregate ready/healthy/pass flag. `/healthz` and `/readyz` are unchanged:
liveness reports process health, while readiness still requires the existing
inventory snapshot. Invalid startup configuration remains fail-fast. Diagnostics
cannot diagnose a process that did not start.

## Access and configuration

- Demo mode is safe by default and makes **zero external requests or credential-file
  reads**, even when optional integration settings are present. Its report is public.
- Every live report requires the separate administrator `DAVANO_API_TOKEN`, including
  deployments with control and execution disabled. The endpoint accepts an exact
  `Authorization: Bearer …` header. Project execution tokens are not substitutes.
- An absent or invalid administrator token configuration yields HTTP 503. The token
  must contain 32–4096 characters. An absent, wrong or malformed request credential
  yields HTTP 401 before any diagnostic reads.
- Configuration is captured when the app starts. Restart after changing feature
  flags or origins. Kubernetes and optional Prometheus credential files are re-read
  for each report so token rotation does not require restart.
- Diagnostics reuse the inventory's server-configured HTTPS origin, CA and service
  account file. Embedded applications supplying a `KubernetesInventory` explicitly
  also use that adapter's connection settings. No kubeconfig execution is added.
- No browser-supplied URL, API group, namespace, queue, query, selector or limit is
  accepted. All query parameters are rejected with HTTP 400. The route only exposes
  GET, never mutation, dry-run, or SelfSubjectAccessReview POST operations.

The administrator credential authorizes access to this report. Upstream requests
use the separately configured service-account or Prometheus credentials, never the
browser's administrator token. HTTPS verification remains enabled and redirects
are never followed. Missing or malformed credentials, CA or origins yield sanitized
blocked checks rather than exposing configuration values.

As with other administrator access, use a reviewed private HTTPS ingress and protect
the bearer token. Responses have `Cache-Control: no-store`. A report is generated
only when explicitly requested and is not persisted by the diagnostics service.

## What is checked

Every live report attempts two independent first-page inventory list requests:

- Nodes with `limit=1`.
- Pods with `limit=1`.

A successful response proves **only that particular bounded list read** at that
time. Returned item count is zero or one. A continuation marker is reduced to a
boolean and is never followed. Counts are not cluster totals, capacity, allocation,
utilization, a successful complete inventory parse, or permission to read other
resources or scopes. Resource names, identifiers and workload contents are not
returned in the report.

Optional checks run only for configured features:

| Configuration | Diagnostic evidence |
| --- | --- |
| Control or execution enabled | Discovery for `fleet.davano.io/v1alpha1`, stable validating admission policy/binding APIs, and the configured supported Kueue version. |
| Execution enabled | Additional Batch Job and NetworkPolicy API discovery. |
| Queue observation enabled | Supported Kueue group/version discovery and queue/workload list-operation advertisement. No queue lists. |
| Pending visibility enabled | Validates bounded server-side targets and requires queue observation, then discovers the supported pending-workload GET API. No target queue reads. Target names are withheld. |
| Prometheus origin configured | One fixed constant `vector(1)` query. No exporter series are read, and no GPU measurement or telemetry coverage is claimed. Uses the separately configured optional Prometheus token and CA. |

Discovery validates the exact group/version, required resource names, scope and
server-advertised read verbs. API discovery is not an authorization check: it does
not establish the caller's resource permissions, authenticated identity, write
permissions, installed object state, or controller correctness. Likewise, a loaded
credential file is configuration evidence, not proof the service accepts it.

Disabled features do not cause their dependency endpoints or credential files to
be read. A disabled feature is not presented as broken. Configured flags may still
have blocked or unavailable prerequisite checks; an enabled flag is never proof
that its dependency works. Optional probe failure does not hide independent results.

## Bounds and failure handling

Each report uses at most **12 GET requests**, a **2-second transport timeout** per
request, an **8-second shared budget**, and **256 KiB per response**. Inventory lists
request at most one item, and larger responses are rejected. Discovery paths are
fixed or chosen only from hard-coded supported versions. Repeated discovery reads
within a report are reused. There are no automatic retries, full scans or pagination.

The budget is checked before requests and between response chunks, and responses
that arrive after it expires are discarded. This is **not a hard wall-clock timeout**:
operating-system DNS and an in-progress blocking transport operation can overrun the
budget. No later requests are started after exhaustion. A single diagnostics report
is bounded; deployments should also apply normal ingress rate and concurrency limits.

Malformed JSON, duplicate JSON members, non-finite numbers, unexpected response
shapes, unsupported API versions, wrong resource scopes, missing read verbs,
source warnings and oversized responses fail closed. HTTP error bodies, exception
messages, response payloads, tokens, endpoints, file paths, arbitrary metadata,
controller messages and queue target identities are never copied into reports.

The endpoint returns HTTP 200 for a successfully generated report containing
blocked/unavailable checks. HTTP status therefore does not mean deployment readiness.
Authentication or invalid-request failures remain HTTP errors.

## Response contract

Schema version 1 includes:

- `source`, `observed_at`, and `mode` (`demo` or `read-only`). The timestamp is report
  generation time, not an atomic snapshot or a hardware measurement timestamp.
- `configured_modes`: control, execution, queue observation, pending visibility and
  Prometheus settings, with only fixed mode labels.
- `checks`: stable `id`, `status`, `evidence`, `detail`, and `next_step`. Checks may
  include safe numeric/boolean `counts`, never source identifiers.
- `summary.status_counts`: a tally, **not an overall readiness score**.
- `bounds`: request count, maximum requests, byte count and per-response byte limit,
  timing settings, inventory list limit, no-pagination marker and budget exhaustion.
- `limitations`: explicit non-goals.

Status meanings:

- `configured`: a flag or local configuration prerequisite, not observed behavior.
- `observed`: only the named bounded read, discovery or constant query succeeded.
- `disabled`: the feature is not configured for use; no dependency check was needed.
- `blocked`: configuration, denied GET or missing required API prevents that check.
- `unavailable`: transport, bounds or malformed evidence prevented a reliable result.
- `unverified`: deliberately outside this report's scope.

Evidence labels are `configuration`, `list-read`, `api-discovery`, `read-query`, or
`not-checked`. Failures use fixed sanitized messages, not upstream response text.

Write authorization, controller/admission enforcement, tenant/network/credential
isolation, actual runtime execution, telemetry correctness, GPU health and physical
isolation **always remain unverified**. Successful API discovery does not prove
namespace UID/PSA, pinned service accounts, quotas, native current-generation
readiness, CNI enforcement, devices or a runnable execution profile. Validate these
separately under appropriate authorization on the actual target deployment.

## Verification scope

`tests/test_diagnostics.py` uses synthetic payloads and in-process API requests to
exercise authentication, disabled/demo isolation, malformed evidence, sanitization,
rotating credentials, request/byte/time limits and independent inventory failure.
An ephemeral local HTTPS server also exercises trusted/untrusted CA handling,
redirect rejection and error-body redaction through the real transport. These tests do not establish live cluster compatibility, enforcement, GPU execution
or production performance. Any disposable-cluster parser smoke adds evidence only
for the specific API responses and identity used by that test.
