# Inventory deployment: private prototype, not a deployed service

No cluster has been contacted or modified as part of creating these files. The manifests are reviewed source templates. They have not been server-side validated, admitted by an OpenShift SCC, or exercised with physical GPUs.

This page covers the **inventory-only deployment roots** below. They run the console and local advisory configuration API without enabling managed control or workload execution. The repository also contains a separate [managed operator and administrator API](control-plane.md), [constrained project execution broker](constrained-execution.md), [namespace allocation and queued-workload reassignment](resource-assignment.md), [Ray manifest planner](ray-kueue.md) and [optional read-only observability](observability.md). Those capabilities require their own configuration/permissions and are not production-verified.

The legacy draft CRDs under `deploy/contracts/` are excluded from every kustomization. They are distinct from the managed `fleet.davano.io` CRDs installed only by the separate operator roots. Installing the drafts does not make the application watch or enforce them. No deployment here installs drivers, changes MIG/sharing settings, or provisions GPUs.

## Choose the intended authority and storage

| Kustomize root | Inventory source | Cluster credentials | Local configuration storage |
| --- | --- | --- | --- |
| `deploy/base` | Demo fixture | None; token disabled, no RBAC | `emptyDir`, lost on pod replacement |
| `deploy/overlays/live-readonly` | Kubernetes API | Only `get,list` nodes and pods | `emptyDir`, lost on pod replacement |
| `deploy/overlays/persistent` | Kubernetes API | Same read-only access | 1 GiB PVC |
| `deploy/overlays/openshift` | Kubernetes API | Same read-only access | 1 GiB PVC, SCC-selected group |

The persistent and OpenShift overlays **include live read-only access**. Choose deliberately. They are not storage-only patches. None of the inventory roots in this table installs CRDs or grants node mutation, pod creation, secrets access, exec, impersonation, privileged SCC, or wildcard permissions. Review the separately documented operator/control/execution authority before combining packages; the inventory RBAC description is not a description of those write-capable roles.

Allocation edits remain behind the opt-in control API's administrator token and need separately reviewed native-guard reads plus policy `update` permission. `deploy/examples/allocation-api-rbac.yaml` is a review-only example with exact-name placeholders; it is not in the inventory deployment roots. Workload reassignment uses the opt-in execution broker and requires source/target project scope plus namespaced Kueue Workload reads to verify source children. It creates no tenant Kubernetes grants. Both workflows need persistent broker state where applicable and target-cluster tests; neither performs physical GPU movement or live workload migration. See [resource assignment](resource-assignment.md).

Reading all pods and nodes reveals cluster-wide workload names, labels, resource requests, and pod specifications. It is read-only but still meaningful access. Kubernetes RBAC cannot restrict the fields returned by a Pod read. Do not bind the reader role unless that visibility is appropriate.

## Prepare and review, without deploying

1. Build the provided image as `davano:dev`, or use your trusted registry and replace the image with a reviewed digest. The supplied image name is a local development placeholder, not a published image.
2. Render the selected root: `kubectl kustomize deploy/overlays/live-readonly > rendered.yaml`. Review the complete result, image provenance, namespace, PVC storage class and service account authority.
3. If authorized separately to contact a target cluster, use its supported server-side dry-run to check schemas, admission policy, storage and SCC constraints. A successful render does not verify these.
4. Only deploy after the cluster owner approves the target, resource creation and access grant. No apply command is run by this project automatically.

The Service is ClusterIP only. No Ingress, OpenShift Route, NodePort or public load balancer is created. A default-deny ingress NetworkPolicy selects the console pods. It requires a network plugin that enforces NetworkPolicy; it does not restrict egress. Kubelet probes and API-server port forwarding must be checked with the target cluster/CNI. A future proxy needs a narrowly scoped ingress allowance, not removal of the isolation policy. The default inventory/advisory workspace has no application authentication or team identity binding. The optional control API has a separate administrator bearer gate, and enabling project execution gates unscoped inventory routes with that administrator credential. Neither establishes SSO or a verified production tenancy boundary. Keep it local or behind a separately reviewed authenticated proxy; a ClusterIP alone is not authentication. If NetworkPolicy is unsupported, misconfigured or relaxed, other workloads in the cluster may reach it, including its local metadata write API. Do not make it publicly accessible.

For a previously authorized installation, a loopback-only `kubectl port-forward -n davano service/davano 8080:8080` can provide a private development connection. Do not use `--address 0.0.0.0`.

## Storage and process security

- One replica and `Recreate` rollout prevent two versions writing the same SQLite file concurrently. This is not an HA design. Recreate entails temporary unavailability.
- Base and live-readonly use `emptyDir`. Pool/policy edits and event history disappear when the pod is replaced; container restarts within the same pod preserve the volume. Use a PVC for persistence and arrange application-consistent backups before real use.
- Persistent overlays rely on the cluster's default StorageClass. Supply a reviewed class if there is no default. PVC allocation, expansion, backups and deletion policies are the cluster owner's responsibility.
- Kubernetes manifests use `fsGroup: 10001` so a non-root process can write the mounted data volume. The image must run as a numeric non-root UID. The OpenShift overlay removes the fixed group; the namespace's restricted SCC must assign an allowed UID and fsGroup. Confirm the chosen storage driver supports the resulting volume permissions. No privileged init container or `anyuid` SCC grant is used.
- `allowPrivilegeEscalation: false`, all Linux capabilities dropped, RuntimeDefault seccomp, read-only root filesystem, no host mounts, no host network and no GPU resource requests are used. Writable `/data` and `/tmp` are explicit volumes. The UI requires no GPU itself.
- In live mode, the pod-mounted service account token is used only by the server. The client uses the Kubernetes service host, verifies the cluster CA and rejects redirects to avoid forwarding its token to another endpoint. Never put cluster credentials in browser JavaScript or returned API data.

The base disables token mounting twice, on the ServiceAccount and pod. The explicit pod override in the live overlay enables only that pod's projected Kubernetes token. The inventory role permits exactly `get,list` on core `nodes,pods`, not `watch`, because inventory is polled. Optional Kueue observation requires a separately reviewed/bound read role; neither its flag nor a Prometheus URL is enabled by these manifests. The separate managed operator also uses polling and has its own write authority.

## Health and failure semantics

`/healthz` is process liveness. `/readyz` is inventory-source readiness. Neither reports GPU device health, thermal status, ECC errors, utilization or correctness of a training job. Kubernetes Node Ready is not a GPU health check.

Demo mode displays labelled fixtures. Live mode must fail closed when credentials, CA verification, API access or inventory reading fails; it must not silently substitute demo inventory. A disconnected inventory source is not proof that all GPUs failed. Show source errors separately from resource counts, and avoid routing to an unready instance.

## Verification scope

Run `python -m unittest discover -s tests -v`. `tests/test_manifests.py` uses optional development dependencies from `deploy/requirements-test.txt` to parse manifests and validate draft examples. It asserts the read-only RBAC boundary, private service, non-root defaults, explicit persistence, no controller/CRD deployment in the inventory roots, and disjoint resource units. Separate operator/control/execution tests cover their own static/API-double contracts.

These are static checks, not an API-server, admission, image-runtime, scheduler, GPU-driver, storage, Kueue or Ray integration test. The current package must be validated in the actual target cluster before any production claim.
