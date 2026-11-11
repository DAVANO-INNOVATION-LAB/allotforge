# Security boundary

AllotForge is a development prototype, **not production-certified or a secure multi-tenant execution platform**. In default inventory-only mode, inventory/advisory endpoints have no application login. The optional Kubernetes control API uses a separate administrator bearer token. The optional constrained execution broker authenticates statically provisioned, project-scoped bearer identities; it is not OIDC/SSO or an organizational identity service. When execution is enabled, unscoped inventory/advisory routes also require the administrator token and fail closed when it is absent. Keep the app on loopback or a private network behind independently reviewed TLS and authentication.

## Workload attribution is a separate trust boundary

The optional workload advisor uses the fleet snapshot's existing access boundary. Enabling it, setting an administrator token or enabling control alone does not add authentication to inventory-only mode. Project execution mode requires the separate administrator token for the unscoped snapshot. Keep inventory-only deployments local or behind an independently reviewed authenticated proxy.

The custom attribution adapter is not bundled. Exact Pod/container identity labels and coverage assertions are checked, but they do not cryptographically prove process/device attribution. Do not relabel device-wide exporter metrics as container measurements. Comparisons are advisory experiments, not automatic placement, performance guarantees or cost savings. See [the measurement contract](docs/workload-advisor.md).

## On-demand diagnostic boundary

Live deployment diagnostics always require the separate 32–4096-character administrator bearer token, even if control and project execution are disabled. Demo diagnostics make no external calls. Diagnostics use fixed bounded GETs only, return no node/workload names or raw connection/credential/response contents, and never test writes or grant access. A successful read or advertised API verb is not proof of mutation permissions, admission enforcement, runtime health or tenant safety. Keep this endpoint behind the same independently reviewed trusted-operator ingress as the fleet console.

## Explicitly enabled cluster writes

The default app uses synthetic demo data. Live inventory reads require `DAVANO_SOURCE=kubernetes`. Control writes require that setting plus `DAVANO_CONTROL_ENABLED=true` and an administrator token at least 32 characters long. Control collection reads and mutations require the token. The status endpoint reveals configuration, not credentials or proof of successful enforcement.

The separately installed controller writes real AllotForge CRDs, owned Kueue resources, namespace ResourceQuotas, and native ValidatingAdmissionPolicies/bindings. Its admission-policy write privilege is consequential and needs administrator review. It does not change node labels, device plugins, drivers, GPU sharing modes or hardware. Kubernetes credentials stay on the server; transport validates its CA, rereads rotating service-account tokens and refuses redirects.

## Direct tenant workload grants remain disabled

**No automatic workload-creation permissions are granted.** Policy subject declarations confer no RBAC access. A policy remains `Ready=False` with `WorkloadGrantsDisabled` even when its native guardrails are reconciled. `GuardrailsReady=True` is only observation of those resource controllers, not a security claim about execution.

Arbitrary Job creation would permit borrowing service-account identities, mounted namespace secrets and other credentials; unrestricted templates can also request host access. Pod Security Admission alone does not solve credential borrowing. The release therefore removes the previous grant path instead of claiming that GPU quota/placement makes it safe. Separately enabled broker execution uses fixed administrator-reviewed profiles, never arbitrary tenant templates.

The controller has no `bind`, `escalate`, impersonation or RoleBinding create/update permission. Its namespace RoleBinding permission is restricted to get/delete for revoking UID-owned legacy grants. The inert `davano-workload-submitter` ClusterRole intentionally has no rules, including on upgrade. Existing legacy grants are not considered revoked until their bindings return 404. A blocked revocation reports `WorkloadAccess=Unknown` and retains failed readiness. Apply the updated RBAC manifests during upgrade; an image-only update cannot neutralize a shared old ClusterRole in namespaces excluded from the controller allowlist. Review those namespaces manually.

Existing permissions supplied by other administrators are neither revoked nor made safe by AllotForge. Existing Pods are not retroactively evicted. Quota, selected GPU resource, protected-label placement and per-Pod caps are not global tenancy, host, network, storage or physical GPU isolation. Native admission type-check status is not proof that all admission caches have activated a binding.

## Constrained execution is a separate opt-in boundary

`DAVANO_EXECUTION_ENABLED=true` requires live Kubernetes mode and a reviewed administrator-controlled configuration file. Identities contain token hashes, allowed projects and a viewer/submitter role; tokens must be distinct from the administrator credential. Each project pins namespace, policy, pool and runtime-ServiceAccount UIDs. Each allowed profile pins an image digest, fixed command and CPU/memory/GPU/runtime limits. The only submission fields are a name, profile ID and GPU count.

The broker creates a single-container suspended Kueue Job with no token, environment, Secret references, volumes, image-pull credentials, host namespaces or user-controlled Pod specification. The fixed template uses nonroot UID/GID, seccomp, dropped capabilities, no privilege escalation and a read-only root filesystem. Its runtime ServiceAccount receives no RoleBinding. The broker service itself has narrowly namespaced Job creation/deletion authority and is therefore a trusted component; that authority must never be given to token holders.

Preflight checks pinned identities, current native guardrails, an active owned queue, recent whole-GPU inventory, per-profile/policy/pool bounds, a feasible single-node capacity, restricted/latest Pod Security, credential-free runtime ServiceAccount, a sole default-deny NetworkPolicy and an unscoped observed Job-count ResourceQuota. These are API-object checks, not proof that CNI or admission caches enforce them. Trusted cluster administrators/webhooks, GPU drivers, host isolation and runtime image safety remain separate concerns. Current profiles cannot mount datasets or download models over the network.

A persistent SQLite ownership registry fences Job UIDs and exact cancellation preconditions. Intent and a result slot are saved before writes, ambiguous responses are reported as uncertain, and retries use the same name and resolved-configuration fingerprint. Workload history and audit records are transactionally bounded; audit capacity exhaustion blocks further mutations until administrator archival. The local audit is not immutable or tamper-evident. Use a persistent volume, one replica, backups and a documented recovery process; losing the registry does not authorize adopting existing Jobs.

See [constrained execution](docs/constrained-execution.md) for reviewed configuration, minimal broker permissions, limits and remaining release gates. Templates are not installed by default. No live Kubernetes, CNI, Kueue, image-runtime or GPU execution test has passed for this workflow.

## Resource reassignment boundaries

Namespace allocation edits are administrator-only exact-revision updates of GPU quota/per-Pod ceilings within the existing pool. They do not transfer reserved capacity, change existing Pods, or switch immutable pool assignments. Native enforcement is asynchronous. Optional policy-update and guardrail-read permissions are separate review-only RBAC source, not a default grant.

Queued workload reassignment is a durable cancel-and-recreate operation across two authorized project scopes. It reserves the exact destination name, pins the reviewed resolved profile, rejects running/admitted/executed evidence, and waits for source Job and owned children to be absent after a confirmed accepted cancellation. Unknown or externally caused source deletion cannot authorize automatic replacement. Target creation uses the existing constrained broker and ownership registry; normal submission cannot bypass an outstanding reservation. There is no state/checkpoint transfer, atomic handover, live migration, arbitrary-job adoption, node-label/device change or automatic rollback. See [resource assignment](docs/resource-assignment.md).

## Reconciliation and deletion safeguards

Writes require exact owned-resource UID labels and Kubernetes resourceVersion checks. A parent resource's UID, generation and deletion state are rechecked before finalizer/status/child writes. A same-name replacement cannot receive a stale object's status or finalizer. Deletion keeps the parent finalizer and remaining guards until every owned child is actually absent, not merely accepted for deletion.

Observed invalid GPU eligibility requests `stopPolicy=Hold` on the owned Kueue ClusterQueue without evicting admitted workloads. The hold is a request until Kueue observes it, does not revoke inherited permissions, and cannot prevent a device-mode change between observations. API outages may prevent a hold from being applied. Controller unavailability leaves existing native objects in place.

## Operational requirements

- Do not publicly expose inventory-only mode or unreviewed broker deployments. Browser cross-origin and body-size checks are not authentication; project bearer tokens also require trusted TLS and secure provisioning.
- Namespace names, subjects, queue labels and protected-label strings are not proof of a secure identity or host boundary. Verify actual NodeRestriction and admission behavior.
- Review cluster read access: it exposes node/pod names, labels and workload information across namespaces.
- Add organization SSO and federated authorization, immutable audit retention, TLS/credential rotation, tested network isolation, backups, image signature verification, vulnerability scans and tested rollback before broader deployment. Broker image-digest pinning alone does not establish provenance.
- Do not commit bearer tokens, Kubernetes credentials, private keys, live databases or private inventory exports.
- Exact-commit CI exercises native admission and diagnostic API response parsing on disposable Kubernetes 1.31.4. It does not verify target OpenShift, application transport/authentication, production RBAC, Kueue scheduling, image runtime, GPU hardware or production security. Unit tests and API doubles are not substitutes.

The [repository](https://github.com/DAVANO-INNOVATION-LAB/allotforge) is public for source review. A license and private vulnerability-reporting channel still need to be selected. Do not disclose credentials or sensitive cluster data in public issues. See [control-plane scope and release gates](docs/control-plane.md).

### RuntimeClass prerequisite

Managed native Pod guards conservatively reject `runtimeClassName` on all new Pods in their namespace, including CPU-only Pods. Kubernetes RuntimeClass admission must remain enabled so manually supplied Pod overhead without a matching RuntimeClass is rejected after mutation. Do not enable this control path on an API server with that admission plugin disabled. This release does not support custom-runtime or sandboxed RuntimeClass workloads in managed namespaces. Existing Pods are not evicted or rewritten.
