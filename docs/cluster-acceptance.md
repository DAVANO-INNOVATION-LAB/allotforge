# Reproducible Kubernetes component acceptance

This suite exercises real Kubernetes and Kueue APIs in a newly created **disposable CPU-only kind cluster**. It tests AllotForge-generated queue resources and Kueue quota reservation. It does not certify a production installation, run GPU work, or bypass the managed operator's hardware eligibility checks.

The [compatibility matrix](compatibility-matrix.md) separates these checks from actual hardware, OpenShift, Ray, tenant authorization and successful managed-controller lifecycle validation. A CI job is evidence only for its exact source commit and pinned inputs. The sanitized report is an execution record, not independent cryptographic attestation.

## Pinned target and execution scope

`acceptance/profile.json` records the reviewed inputs:

- kind v0.33.0 with Kubernetes v1.34.11, pinned by the official node-image digest.
- Kueue v0.20.5, using `kueue.x-k8s.io/v1beta2`.
- The official Kueue installation manifest's exact byte length and SHA-256, and the verified controller OCI index digest.
- A standard `ubuntu-latest` GitHub Actions runner and a 20-minute job cap. No paid runner, cloud cluster, GPU session or external account is provisioned.

The workflow installs Kueue's official controller, CRDs, webhook and RBAC resources into that disposable cluster. This installation is privileged **inside the disposable test cluster**. It is not a least-privilege production deployment or a tenant-RBAC test. The Kueue controller itself runs Pods; the acceptance workload namespace must have no workload Pods or Jobs at checked points.

The new target is separate from the older Kubernetes v1.31.4 native-admission regression suite. Kueue 0.20 removed v1beta1 serving; its 0.20 installation documentation identifies Kubernetes 1.34+ as supported/tested. Do not install this pin into the old CI cluster and call that supported compatibility. The managed AllotForge code supports explicit v1beta2; its default v1beta1 and the Ray queue-bundle builder must not be assumed compatible with Kueue 0.20.5.

## What the runner does

1. Refuses inferred/current contexts. Requires a private, single-context kind kubeconfig JSON, exact loopback HTTPS server, expected kube-system Namespace UID, exact Kubernetes version, disposable-instance label and one expected CPU-only kind Node. It checks the pinned controller Deployment and beta2 discovery.
2. Validates and snapshots embedded CA/client-certificate credentials into a private immutable, unlinked descriptor. Every subprocess uses that snapshot, not ambient kubeconfig or proxy settings. Auth plugins, tokens, file-based credential references, insecure TLS and alternate targets are rejected. Credential content is never printed or uploaded.
3. Requires every fixed fixture name to be absent before any create. The namespace and generated ResourceFlavor, ClusterQueue and LocalQueue have unique run labels. Synthetic pool/policy metadata produces **zero GPU quota**, 1 CPU and 64 MiB of queue accounting. The synthetic protected selector is never applied to a Node. No GPUPool or GPUAccessPolicy object is created.
4. Waits for current-generation ClusterQueue and LocalQueue Active conditions. Creates two standalone CPU/memory Workloads, not Jobs or Pods: 100m CPU is admitted, while a 1-CPU request waits behind it. Checks exact queue/flavor assignment, resource usage and reservation counters.
5. Deletes the first reservation with UID/resourceVersion preconditions, then checks that the waiting workload is admitted with the released quota. Admission is accounting evidence, not proof of schedulability or execution.
6. Checks the fixture namespace for Pods and Jobs at explicit checkpoints, including before namespace cleanup. These are snapshots, not continuous audit. Unexpected workloads block namespace deletion.
7. Deletes only recorded fixture identities, waits for actual absence, and retains an incomplete-cleanup result on conflict or failure. No force-finalizer removal, foreign-object adoption or generic cleanup command exists. The workflow separately destroys the named kind cluster and verifies its absence.

The source-mode and ownership boundaries matter: this is **generated-resource compatibility with a real Kueue controller**, not successful positive-path AllotForge operator reconciliation. That operator still requires matching real whole-GPU inventory. The suite does not invent GPU Nodes, patch allocatable capacity, deploy the operator, grant tenant roles or execute a GPU workload.

## Offline inspection

From the source checkout with development dependencies available:

```sh
python -m acceptance.cluster --plan
python scripts/prepare_kueue_acceptance.py
```

Both commands are offline. The first prints fixed fixtures and authority bounds; the second prints the pinned artifact preparation plan. Neither reads credentials, contacts Kubernetes, downloads an image or installs anything. A conflicting `DAVANO_KUEUE_API_VERSION` is refused rather than silently changed.

`prepare_kueue_acceptance.py --download --output <new-file>` explicitly downloads only the pinned official manifest. It verifies bytes and SHA-256 **before** YAML parsing, then changes only the named manager container's image to the pinned digest. It never applies a manifest. Existing output files and symlinks are refused; output is private. Its network timeout is per socket operation, not a hard wall-clock limit.

## Authorized execution

The provided workflow `.github/workflows/kueue-acceptance.yml` is the reproducible execution path. It creates its own named cluster with a dedicated kubeconfig, pins all third-party actions by commit, uses a contents-read GitHub token, prepares the verified manifest, waits for the Kueue Deployment, invokes the runner and uploads only the sanitized evidence file. The main release build depends on this reusable gate as well as existing Python/UI/native-admission checks.

The runner is deliberately not a general staging installer. It is Linux-only and requires an explicitly authorized disposable kind instance matching every fixed guard. A context name, a label or an expected UID does not itself establish user permission. Do not point the workflow or runner at a shared/production cluster, rename another cluster to pass a guard, or weaken the checks to obtain a green result. A separate real GPU or OpenShift acceptance target requires its own reviewed scope, credentials and spending limits.

For an already authorized matching disposable instance, the runner requires all target inputs:

```sh
python -m acceptance.cluster --execute \
  --context kind-allotforge-acceptance \
  --kubeconfig-json <private-single-context-kind-json> \
  --expected-server https://127.0.0.1:<explicit-port> \
  --expected-kube-system-uid <observed-instance-UUID> \
  --expected-kubernetes-version v1.34.11 \
  --run-id <12-lowercase-hex-characters> \
  --evidence <new-private-report.json>
```

The existing workflow shows how to export the newly created kind configuration privately. Never paste a kubeconfig, certificate private key or token into a report, issue, log or chat. No ambient-context fallback is supported.

## Bounds and result interpretation

The fixture phase has a 180-second budget; cleanup gets a separate 60-second budget. Each phase is limited to 240 subprocess calls, each command to at most 10 seconds, each request body to 64 KiB and each stdout response to 2 MiB; stderr is discarded. Oversized, ambiguous, non-integer, deeply nested and malformed JSON is rejected. Child processes are terminated on timeout or overflow. There are no arbitrary queries, commands, generic list pagination or retries outside the bounded observation loops.

The report includes source commit and runner/builder-directory dirty-tree status, profile hash, exact software pins, fixed case observations and cleanup status. The offline plan contains the authority bounds. Server URLs, credentials, raw kubeconfig, raw API payloads and upstream error text are omitted. The new report file is created privately and never overwritten.

- `passed_bounded_cpu_integration` and exit 0 require the bounded cases and fixture cleanup to succeed.
- Failure or incomplete cleanup cannot produce that status. Refused/local-error invocations return exit 2; an executed unsuccessful suite returns exit 1.
- `production_pass` is always false. Hardware, GPU placement, actual workload execution, positive managed reconciliation, tenant RBAC, Lease behavior, native admission and production readiness remain separately unverified by this runner.

Inspect the exact GitHub Actions run and its `allotforge-kueue-acceptance` artifact. The source report is generated before workflow-level cluster destruction; the successful teardown step/job supplies that separate evidence. Unit-test transport doubles validate runner behavior, not the live compatibility target.
