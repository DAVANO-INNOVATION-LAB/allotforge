# GPU validation and attribution acceptance

**Status: preparation, not a completed hardware test.** The smoke verifier checks supplied files without contacting a cluster or executing a workload. Its tests use synthetic exports and logs. No real GPU, attribution collector, target driver, scheduler or matched CPU/GPU comparison has been validated by this change.

## Software checks for this development change

Local validation on 2026-11-11 passed 396 Python tests, including 17 smoke-verifier test methods with adversarial cases, plus 241 unchanged UI checks, six JavaScript syntax checks and Python compilation. Independent review also exercised 3,190 field mutations and 34 quantity edge cases. Oversized integers, contradictory attempts, duplicate Pod identities, malformed timezone offsets and output-file overwrites were checked. These are software and synthetic-fixture checks only. Remote CI, when run, is recorded on the draft PR; no new release is asserted by this document.

## First staging smoke

Use an explicitly approved disposable GPU target, namespace, digest-pinned image, installed queue/controller configuration and resource/time budget. The existing [benchmark planner](../benchmarks/README.md#offline-batch-plans) creates an inert single-GPU Job manifest. Applying that manifest is a separate operation that can start work and incur cost. This kit has no apply, install, exec or delete command.

Capture the original Job, all relevant Pod attempts, the benchmark container's complete stdout and a capture receipt. Keep these files private. The receipt records the operator's assertion about which Pod/container produced the stdout; it is not a signature or independent attestation. Do not construct a receipt from an assumed Pod name or reuse a log after a restart. Use the [offline smoke verification instructions](../benchmarks/README.md#offline-gpu-smoke-evidence) to check the bundle.

The verifier joins controller UIDs and the exported container incarnation, then checks the expected fixed program, requested image and resources, sample identity, successful termination, raw timing samples and capture hashes. Scheduler success alone does not establish that the GPU payload ran. Conversely, internally consistent JSON does not independently prove GPU execution: an operator can supply or alter every input file.

Requested image references and runtime-reported image IDs are retained separately. An image index, platform manifest and image configuration can have different digests. The verifier cannot resolve that mapping without registry/runtime evidence and does not claim it is verified. A request for one `nvidia.com/gpu` also does not prove an exclusive physical GPU, because sharing configurations can retain that resource name.

The 100 synchronized CUDA iterations belong to one smoke execution. They are not 100 independent experimental blocks, a CPU comparison, a whole-job profile or a scheduler performance result. Never promote the verification output into a live campaign envelope automatically.

## Why there is no relabeling adapter

The workload advisor accepts an explicitly defined per-container, runtime-bound busy percentage. Stock metric names or plausible labels alone cannot establish that meaning. Research checked on 2026-11-11 found the following boundaries:

| Source | Useful evidence | Missing or incompatible evidence |
| --- | --- | --- |
| Kubernetes PodResources v1 | Allocated device IDs and Pod/container names | No Pod UID or current container runtime ID; allocation is not use. |
| CRI ContainerStats | Current runtime container ID, timestamped cumulative CPU core-nanoseconds, memory working-set bytes | Requires a verified Kubernetes-to-CRI incarnation join and a safely authorized collector. It does not provide this GPU metric. |
| cAdvisor | CPU-time counter and memory working set | Counter deltas and current cgroup/runtime identity are necessary. Pod names alone do not bind historical samples. |
| NVIDIA NVML process utilization | PID, source timestamp and supported per-process SM observations | Unsupported on MIG-enabled GPUs; missing/nonzero-only process samples are not proof of zero. Process overlap and PID reuse require explicit treatment. |
| NVIDIA DCGM Exporter 4.9.0 process metrics | Process-derived Pod activity is available | Pod sums can exceed 100%. The tagged transformation aggregates by Pod UID, and the reviewed path does not preserve the per-container/source-time evidence required here. Relabeling or clamping cannot repair it. |
| AMD SMI / AMD device metrics exporter | Device and process observations, including engine-time/occupancy fields | Their units and aggregation are not automatically a bounded per-container wall-time busy percentage. Validate separately. |

This is a source-semantics assessment, not a claim that these tools are defective or that all later versions have the same limitations. Pin the exact implementation and hardware used in any future validation.

A CPU/RAM-only CRI adapter is a viable separate increment: calculate cores as the change in CPU core-nanoseconds divided by the source timestamp interval for the same incarnation. Reject counter resets, missing fields, nonpositive intervals and stale sources. Omit GPU series rather than inventing zeros. Access to a container-runtime socket is powerful; mounting it read-only does not restrict which RPCs can be called. No runtime socket, host filesystem, elevated role or collector DaemonSet is added by this change.

The development [cAdvisor CPU/RAM consumer](cadvisor-cpu-memory.md) now offers a separate route through an existing Prometheus source, without a runtime socket or new collector. It requires strict current-container cgroup identities and an explicitly asserted source-timestamp-preserving pipeline. It does not supply GPU attribution or satisfy the hardware acceptance matrix below.

## Acceptance matrix for a future GPU collector

Run only after approving the target, load, privileges and budget. Start with one dedicated whole GPU and one regular container. Preserve raw source data, status codes, source timestamps and immutable capture hashes for every case.

| Case | Required behavior before integration |
| --- | --- |
| Idle, sustained compute and controlled duty cycle | Establish the metric's units, measurement interval and zero semantics against an independent reference. Do not assume missing process records mean idle. |
| Activity in an unrelated Pod | Never attribute another Pod's device activity to the tested container. |
| Two containers in one Pod | Distinguish the GPU-bearing container from unrelated CPU work. Pod-wide sums cannot be copied into both containers. |
| Multiple processes and overlapping kernels | Establish a bounded busy-time definition; summing percentages or clamping at 100 is not sufficient. |
| Process exit and PID reuse | Old samples must not attach to a new process or cgroup. Preserve process identity/lifetime evidence. |
| Container restart and Pod recreation | Old samples must not attach to a new runtime ID or Pod UID, even when names are reused. |
| Missing device, permissions, unsupported APIs or partial enumeration | Emit explicit unavailable/unknown evidence. No error-to-zero or device-to-container fallback. |
| Multiple requested devices, including idle devices | Prove complete coverage and matching device identities. A busy subset cannot stand in for the allocation. The current advisor still blocks multi-GPU CPU-comparison suggestions. |
| MIG, MPS, time-slicing, vGPU, partitions or unknown mode | Reject the current whole-GPU attribution path. Neither resource names nor a single visible CUDA device prove whole-GPU exclusivity. |
| Gaps, cache re-scrapes and source clock skew | Preserve distinct raw source timestamps. Re-scraping an old value must not manufacture new evidence or freshness. |

Exercise the real advisor end to end only after source semantics and attribution pass those checks. Its 15-minute policy requires at least 41 distinct source samples, at least 600 seconds of span, no gaps above 90 seconds, a newest sample within 120 seconds and 12 shared one-minute buckets. Roughly 15-second source observations can satisfy the count; a 30-second cadence normally cannot. A successful synthetic replay validates software checks, not collector correctness.

Keep independently measured CPU/GPU performance and cost trials separate from collector validation. The [workload comparison checklist](workload-advisor.md#a-measurable-comparison) still requires matched code/data, correctness, repeated trials, resource limits and real prices. No automatic placement decision follows from the acceptance matrix.

## Primary sources

- [Kubernetes PodResources v1 schema](https://raw.githubusercontent.com/kubernetes/kubelet/master/pkg/apis/podresources/v1/api.proto)
- [CRI v1 ContainerStats schema](https://raw.githubusercontent.com/kubernetes/cri-api/master/pkg/apis/runtime/v1/api.proto)
- [cAdvisor Prometheus metric definitions](https://raw.githubusercontent.com/google/cadvisor/master/docs/storage/prometheus.md)
- [NVIDIA NVML device/process query APIs](https://docs.nvidia.com/deploy/nvml-api/latest/api/group__nvmlDeviceQueries.html)
- [DCGM Exporter 4.9.0 release notes](https://github.com/NVIDIA/dcgm-exporter/releases/tag/4.9.0), [tagged Kubernetes transformation](https://raw.githubusercontent.com/NVIDIA/dcgm-exporter/4.9.0/internal/pkg/transformation/kubernetes.go) and [tagged NVML provider](https://raw.githubusercontent.com/NVIDIA/dcgm-exporter/4.9.0/internal/pkg/nvmlprovider/provider.go)
- [AMD SMI functions](https://rocm.docs.amd.com/projects/amdsmi/en/latest/reference/amdsmi-cpp-api/functions.html) and [field definitions](https://rocm.docs.amd.com/projects/amdsmi/en/latest/reference/amdsmi-cpp-api/types.html)
- [AMD device metrics exporter 1.5.1 metrics](https://instinct.docs.amd.com/projects/device-metrics-exporter/en/release-v1.5.1/configuration/metricslist.html)
- [NVIDIA GPU sharing behavior](https://docs.nvidia.com/datacenter/cloud-native/gpu-operator/latest/gpu-sharing.html)
