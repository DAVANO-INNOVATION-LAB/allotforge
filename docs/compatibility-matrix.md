# Compatibility and evidence matrix

Compatibility is scoped to a component, configuration and exact software version. A source inspection, generated manifest, accepted API object, queue admission and completed GPU workload are different evidence. No row below establishes production readiness.

## Version boundaries

| Component / configuration | Evidence and boundary |
| --- | --- |
| Python 3.11 and 3.12; UI DOM/API contracts | Exact-commit automated checks in the release workflow. DOM checks and static previews are not native browser tests. |
| Kubernetes v1.31.4 native admission | Existing disposable kind gate exercises real API-server CRD/CEL, Pod/Job server dry-runs and deployment rendering. [0.8 release run](https://github.com/DAVANO-INNOVATION-LAB/allotforge/actions/runs/43135167383) passed. This does not run Kueue or GPU work. |
| Kubernetes v1.34.11 + Kueue v0.20.5 + kind v0.33.0 | New pinned [component acceptance gate](cluster-acceptance.md). Its exact-commit report records real generated-resource compatibility, queue activation, CPU quota reservation/pending handoff and cleanup when executed successfully. Synthetic runner tests alone do not mark this target passed. |
| Managed Kueue v1beta2 | Explicit `DAVANO_KUEUE_API_VERSION=v1beta2` selects production builders used by the new gate. This does not establish successful positive-path managed operator reconciliation. |
| Managed default v1beta1 with Kueue v0.20.5 | **Incompatible API selection.** Kueue 0.20 removed v1beta1 serving. Select beta2 for this target; do not infer compatibility from discovery of another API. |
| Ray queue bundle v1beta1 with Kueue v0.20.5 | **Incompatible bundle API.** The current Ray bundle is not silently converted by the managed-controller flag. A tested Ray/Kueue integration remains future work. |
| KubeRay v1.7.2 / RayJob v1 | Builder and API-contract tests only. No live Ray controller, image execution, dashboard authentication or end-to-end cleanup evidence. |
| OpenShift | Unverified. Manifest overlays are not SCC, CNI, native-admission or tenant-isolation acceptance. Validate a specific OpenShift/Kubernetes pair separately. |
| cAdvisor CPU/RAM | Pinned-source research, parser/transport doubles and strict current-runtime/timestamp contracts. No live collector pipeline validated; timestamp preservation remains an administrator assertion. |
| NVIDIA, AMD and Intel whole GPUs | Inventory/resource contracts and selected telemetry adapters only. No target driver/plugin/runtime/hardware execution validation. A resource name does not prove exclusivity. |
| T4 sharing; A100 MIG; MPS/vGPU | No reconfiguration or hardware validation. Inventory units, shared slots and MIG profiles remain distinct. Unsupported managed modes are not enabled by the acceptance gate. |

The implementation uses stable native ValidatingAdmissionPolicy APIs available in Kubernetes 1.30+, but that API floor is not a blanket compatibility claim for every later distribution. Kueue's own supported/tested floor is separate. Pin and validate the complete target combination before deployment.

## What the new CPU-only gate can establish

| Check | Evidence on a successful exact-commit run | Still unverified |
| --- | --- | --- |
| Official software inputs | Manifest SHA-256/length, pinned action commits, node-image digest and Kueue OCI index digest | Image signing/scanning, production supply-chain policy and hardware runtime identity |
| API/resource compatibility | Beta2 discovery and acceptance of actual production-builder queue specifications | Every possible schema field, all older/newer versions, managed CRD/API authorization |
| Kueue behavior | Current-generation CQ/LQ Active and Workload QuotaReserved/Admitted; exact resource/flavor assignment; pending handoff after quota release | Pod placement, actual execution, throughput, fairness under load, GPU ownership or performance |
| Cleanup | Own fixture UIDs removed and actual absence observed; separate kind teardown step | General operator finalizers, failover/Lease fencing, production recovery and concurrent tenant races |
| Access boundary | Explicit disposable target and private immutable client configuration; no workload Pods/Jobs at checkpoints | Least-privilege production deployment, negative tenant RBAC, authentication/SSO and continuous audit |

A passing component gate never fills the remaining cells automatically. Evidence is not promoted into a live GPU benchmark or CPU/GPU savings claim. Use the existing [GPU acceptance matrix](gpu-validation.md), [workload comparison checklist](workload-advisor.md#a-measurable-comparison) and an approved target/budget for those stages.

## Pinned upstream sources

- [kind v0.33.0 node-image digests](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0).
- [Kueue v0.20.5 installation requirements](https://github.com/kubernetes-sigs/kueue/blob/v0.20.5/site/content/en/docs/getting-started/installation.md).
- [Kueue v0.20.0 v1beta1 removal](https://github.com/kubernetes-sigs/kueue/releases/tag/v0.20.0).
- [Official v0.20.5 manifest](https://github.com/kubernetes-sigs/kueue/releases/download/v0.20.5/manifests.yaml).
- [Immutable upstream image-promotion record](https://github.com/kubernetes/k8s.io/blob/06bb36dab7568c6ec933081d7b24dbc080d3e1d5/registry.k8s.io/images/k8s-staging-kueue/images.yaml).

The manifest bytes and Kueue OCI index were independently hashed during preparation on 2026-11-12; the versioned profile keeps those exact pins. Source research is not runtime acceptance. The CI artifact and run link record what actually executed.
