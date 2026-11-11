# Workload monitoring and CPU/GPU advice

The **Workload advisor** is read-only. It shows every observed Pod, including CPU-only Pods, with requested CPU, memory and GPU budgets. Requests are scheduler accounting, never measured usage. The existing GPU Workloads view remains separate.

Optional measurements feed a conservative experiment recommendation, **not an automatic CPU/GPU selector**. No workload is moved, resized, submitted or migrated. CPU/GPU speed, correctness and cost comparisons remain unknown until matched trials are run. No real GPU hardware or attribution collector was exercised in this implementation.

## What the verdicts mean

- **Insufficient evidence:** identity, scope, freshness, coverage or required metrics are missing. Missing is not zero.
- **CPU comparison candidate:** aligned runtime-attributed CPU and GPU evidence shows concurrent CPU work in the GPU-bearing containers (mean at least 0.1 cores), GPU mean at most 15%, and no observed container GPU sample above 25%. Each GPU-bearing container must request exactly one whole GPU; multi-GPU averages can hide a busy device and cannot produce this verdict. Nonzero or invalid unclassified extended-resource requests/limits also block this candidate, because they may include an accelerator outside the measurements. Observations and GPU-activity verdicts are retained. This is a reason to test a CPU alternative, not proof of a CPU bottleneck or savings. I/O, transfer, synchronization and workload phases can cause the same pattern.
- **GPU activity observed:** mean attributed GPU busy time is at least 50%. This does not prove acceleration is necessary, faster or cheaper.
- **Inconclusive:** sufficient measurements are available but do not select a resource.
- **No supported GPU request** (`cpu-requested` in the API): the Pod has no recognized GPU request. Custom accelerator resource keys are not classified. This does not establish optimal placement or rule out useful acceleration.

Confidence in an observed pattern is at most **limited**. A single 15-minute window is not a whole-job profile. The advisor stores no durable time series and performs no background polling; refresh retrieves another bounded window. Terminal or pending Pods retain request information but receive no live utilization verdict. Init/sidecar and ephemeral-container workloads, Pod-level resource budgets, shared GPU resources, MIG and unknown GPU modes are not supported for a CPU comparison.

## Opt-in integration contract

Set `DAVANO_WORKLOAD_ADVISOR_ENABLED=true` in live (`DAVANO_SOURCE=kubernetes`) mode, alongside `DAVANO_PROMETHEUS_URL`. The existing optional `DAVANO_PROMETHEUS_TOKEN_PATH`, `DAVANO_PROMETHEUS_CA` and `DAVANO_PROMETHEUS_CLUSTER` apply. No extra Kubernetes writes or RBAC are added. The default inventory-only server has **no application authentication**, including advisor data; enabling the advisor, control API or setting an administrator token alone does not add it. Keep the server on loopback or behind a separately reviewed authenticated proxy. When project execution is enabled, the existing `/api/v1/state` middleware requires the separate administrator bearer token and denies project tokens. Demo mode ignores metric readers and creates no synthetic measurement values.

**A custom, trusted instrumentation adapter is required and is not included.** These are AllotForge's input names, not stock cAdvisor/DCGM/AMD exporter names. Deploying an exporter by itself does not enable runtime attribution. Do not rename device metrics to satisfy this contract. Before enabling it, verify the adapter can prove every included measurement belongs to the stated current container and covers every requested whole GPU.

The adapter emits gauges:

| Metric | Semantics | Bounds |
| --- | --- | --- |
| `allotforge_container_cpu_usage_cores` | Container CPU time divided by its measurement interval, summed over cores. A value of 1 means one CPU core's time, not 1%. | 0–1,000,000 |
| `allotforge_container_memory_working_set_bytes` | Container working set at the source sample time. | 0–2^60 |
| `allotforge_container_gpu_busy_percent` | Average busy percentage across **all** whole GPUs requested by this container, attributed to its own work. Must include idle requested GPUs and exclude other containers' work. | 0–100 |

Recognized GPU request keys are `nvidia.com/gpu`, `nvidia.com/gpu.shared`, `nvidia.com/mig-*`, `amd.com/gpu`, `amd.com/gpu.shared`, `gpu.intel.com/i915` and `gpu.intel.com/xe`. Shared/MIG/unknown modes are counted as requests but cannot support a resource-comparison candidate. No recognized GPU request does not prove the Pod has no custom accelerator resource.

Every series requires exact `namespace`, `pod`, `pod_uid`, `node`, `container`, `container_id`, `scope="container"` and `attribution="container-runtime"` labels. `container_id` is the full current Kubernetes containerID (for example `containerd://…`), not merely a container name. GPU series additionally require a decimal `gpu_count` label exactly matching that container's effective requested whole-GPU count. Changing the Pod or container incarnation invalidates old samples. Every current regular container is required for CPU and memory aggregates; every GPU-requesting container is required for the GPU aggregate.

These label assertions are a **trust boundary**, not cryptographic verification or a collector implementation. AllotForge checks them against observed Kubernetes identities, but cannot independently prove the collector measured the right process or every device. A Pod label on a device-wide DCGM metric does not prove per-workload use. Stock device metric names are never accepted by this advisor.

With `DAVANO_PROMETHEUS_CLUSTER` configured, every series must carry that exact cluster label. Without it, cluster-labelled series are not joined, preventing accidental federation joins on short Pod/node names. Administrators must ensure an unlabelled source belongs to the inventory's cluster.

The reader uses the same verified HTTPS, bounded-response and no-redirect transport as device observability, with limits of 8 MiB per response, 10,000 series and 150,000 samples. Exceeding a limit rejects the entire observation rather than yielding a partially covered fleet. Its fixed server-side query selects only the three names above using a raw `[15m]` vector. Callers cannot supply a query, URL or credential. Prometheus warnings, malformed envelopes and safety-limit overruns reject the read. Duplicate series for one current container/metric are conflicts, even if their values agree; deduplicate HA scrapes in the trusted source first.

## Coverage and aggregation

- Current Pod is Running, all current container runtime IDs are known, and every container has been running for at least 15 minutes.
- At least 41 distinct valid source timestamps per required container and metric, spanning at least 600 seconds, within the rolling 900-second window.
- Latest sample is at most 120 seconds old. Samples more than 5 seconds in the future reject the series; smaller future skew is ignored. Negative, non-finite or out-of-range samples and contradictory duplicate timestamps reject the series.
- No adjacent sample gap above 90 seconds. At least 12 of 15 aligned one-minute buckets (80%) must exist across all required containers. CPU and GPU need at least 12 shared buckets before any comparison suggestion. Decision means are recomputed over those shared buckets. CPU work in non-GPU containers does not drive the comparison recommendation. The assessment basis reports these aligned means and bucket count.
- Per-container samples are averaged inside each minute. CPU and memory minute means are summed across containers. GPU minute means are weighted by requested GPU count. These aligned minute values have equal weight in the displayed mean and p95; p95 is **not** a job-latency percentile.
- GPU peak is the largest observed per-container GPU sample, deliberately stricter than an aggregate mean. CPU/memory peak is the sum of per-container maxima, an upper bound that may not be simultaneous.

Results retain metric name, scope, sample start/end, minimum per-container sample count, coverage, explanation and source. Observation-window time and inventory-snapshot time are separately recorded. Kubernetes Node and Pod snapshots are not atomic, and cluster state can change immediately afterward.

## A measurable comparison

The inspection panel provides this checklist. It does not execute it:

1. Confirm a CPU implementation exists and meets the same output-quality target.
2. Hold code/image version, input dataset, batch size, concurrency, seed and output checks constant.
3. Record CPU/RAM limits, CPU and accelerator models/counts, software versions and competing work. Account for the GPU host's CPU and memory cost too.
4. Separate warm-up from at least five completed measured trials per resource. Include transfers and complete end-to-end execution, not only kernel time.
5. Compare correctness, median and tail latency or jobs per second, peak memory and cost per successful job against the actual deadline.
6. Use real prices and elapsed billed time. Missing prices or failed trials cannot substantiate savings. Review before changing placement.

This release provides an experiment plan, not a benchmark-runner, price feed or validated CPU/GPU comparison result. An idle GPU is not sufficient evidence to declare a job CPU-bound.
