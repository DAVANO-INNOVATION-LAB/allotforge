# CPU and memory observations from cAdvisor

The development `cadvisor-cpu-memory` source reads existing Prometheus samples for current Kubernetes containers. It measures **CPU interval rates and memory working set only**. It does not supply GPU activity, recommend CPU instead of GPU, install a collector, change scraping or grant host access.

This mode has been implemented against documented source contracts and synthetic tests. No real kubelet, cAdvisor installation, ingestion pipeline or GPU hardware has been validated by the implementation. Deployment compatibility must be checked on the intended cluster.

## Explicit opt-in and source prerequisites

The existing advisor remains disabled by default. Its default enabled source is the separate custom-adapter contract. To select cAdvisor, first verify the actual effective scrape and storage pipeline, then configure:

```sh
DAVANO_SOURCE=kubernetes
DAVANO_WORKLOAD_ADVISOR_ENABLED=true
DAVANO_WORKLOAD_METRICS_SOURCE=cadvisor-cpu-memory
DAVANO_CADVISOR_TIMESTAMPS_PRESERVED=true
DAVANO_PROMETHEUS_URL=https://your-approved-prometheus.example
```

The existing optional Prometheus credential-file, CA and cluster settings apply. The last `true` above is an **administrator assertion**, not a result that AllotForge independently verified. Without that assertion, this mode fails closed before querying the source. Do not set it merely to clear a warning.

Verify all of the following in the effective configuration and observed series:

1. The producer is the reviewed cAdvisor statistics collector. A kubelet endpoint named `/metrics/cadvisor` alone is insufficient: Kubernetes can switch to a different CRI metrics producer through `PodAndContainerStatsFromCRI`.
2. Prometheus preserves exporter timestamps with `honor_timestamps: true`. For a Prometheus Operator ServiceMonitor, inspect the effective `honorTimestamps` setting and generated configuration. Preserve those timestamps through any downstream storage/query pipeline too. Recording-rule replacements and downsampling are outside this contract.
3. Collection and scraping supply enough **distinct source observations**, normally with a 10–15 second scrape interval. CPU needs at least 42 distinct counter points to produce the advisor's 41 required interval rates. Scraping cached values faster cannot manufacture observations. A 30-second interval normally cannot meet the count.
4. Preserve the `id`, `cpu`, `namespace`, `pod` and `container` metric labels. Supply a trusted `node` label from Kubernetes discovery that exactly matches the inventory node name. Do not replace workload namespace/Pod labels with the scrape Service's identity.
5. If `DAVANO_PROMETHEUS_CLUSTER` is configured, every selected series carries that exact cluster label. Prometheus global `external_labels` alone do not add it to local queries. Without configured cluster selection, cluster-labelled series do not join.
6. Avoid duplicate matching series from scrape targets or HA replicas. The adapter rejects ambiguity rather than picking a convenient source.

Distribution defaults differ. At the research date, kube-prometheus's main manifest used `honorTimestamps: false` and a 30-second interval, while kube-prometheus-stack's Helm defaults used preserved timestamps and a 10-second cAdvisor interval. Neither repository's current defaults prove how a deployed cluster is configured. This feature makes no configuration changes and does not inspect privileged Prometheus configuration endpoints.

## What is read

The verified-HTTPS reader uses a fixed raw 15-minute query for only:

- `container_cpu_usage_seconds_total`, a cumulative CPU-seconds counter with `cpu="total"`.
- `container_memory_working_set_bytes`, a working-set gauge in bytes.

It does not query `container_last_seen`: cAdvisor generates that metric from its current clock, so it cannot establish the freshness of cached CPU/RAM statistics. The selected CPU and memory metrics use the original statistics timestamp when ingestion preserves it.

The existing 8 MiB response, 10,000-series and 150,000-sample bounds apply. Callers cannot provide arbitrary queries, URLs or credentials. No runtime socket, host mount, new Kubernetes permission, background polling or durable history is added. Existing application authentication boundaries are unchanged: inventory-only mode still needs loopback or an independently authenticated proxy.

## Current-incarnation identity

A sample must match namespace, Pod name, container name, node and cluster **and** an exact supported full cgroup path containing the current Pod UID and current runtime container ID. Names alone cannot establish identity.

Supported initial forms are standard kubelet `kubepods` cgroupfs/systemd paths for `containerd://` and `cri-o://` identities with full lowercase 64-hex runtime IDs. Guaranteed, Burstable and BestEffort Pod parents are recognized. Systemd Pod UID hyphens are converted to underscores exactly; the runtime-specific leaf must match the current ID. Path matching is whole-string equality.

Custom cgroup roots, shortened IDs, arbitrary descendants, alternate runtimes, sandbox/Pod aggregate paths and missing identity labels stay unknown. The reader does not infer a node from a scrape address or attach today's runtime ID to historical name-only metrics. Current runtime, freshness, coverage and supported-Pod restrictions from the [advisor](workload-advisor.md#coverage-and-aggregation) still apply.

Labels and the scrape pipeline remain a trusted administrator boundary. Matching their contents is not cryptographic proof that a collector measured the correct cgroup.

## CPU and memory calculations

CPU cores are calculated between adjacent source observations as:

`(later CPU seconds - earlier CPU seconds) / (later source time - earlier source time)`

Each derived value retains the later actual source timestamp and describes the average cores consumed over that interval. Both endpoints must belong to the current incarnation and observation window. Counter decreases/resets, malformed values, conflicting duplicate timestamps and excessive gaps do not become zero or a bridged interval. Per-CPU series are not summed: missing `cpu="total"`, per-CPU-only or ambiguous mixed evidence is unsupported.

Memory working set preserves its original gauge values and timestamps. It is total usage minus inactive file memory, floored at zero by the producer. It is **not** RSS, application heap or proof that all included memory is unreclaimable.

Existing per-minute summaries average the available derived CPU interval rates or memory observations in each bucket, then weight shared minute buckets equally. CPU means are not exact time-weighted integration, and CPU peaks are peaks of interval averages, not instantaneous CPU peaks. Pod CPU/memory peak remains a sum of container maxima that may not be simultaneous. An invalid CPU stream does not erase independently valid memory evidence, or vice versa.

GPU measurements are always unavailable in this mode, even if a source returns custom GPU metrics. A complete CPU/RAM window cannot produce a CPU-versus-GPU recommendation, a GPU-activity verdict, a speedup estimate or a cost saving. Use the separately configured custom-adapter mode only when a trusted adapter satisfies its complete attribution contract.

## Primary sources checked 2026-11-11

- [cAdvisor v0.60.6 CPU metric](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go#L171-L199), [working set](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go#L522-L530), [source timestamp emission](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go#L2298-L2313) and [last-seen behavior](https://github.com/google/cadvisor/blob/v0.60.6/lib/metrics/prometheus.go#L118-L125).
- [Working-set calculation](https://github.com/google/cadvisor/blob/v0.60.6/lib/container/libcontainer/handler.go#L770-L845).
- [Prometheus scrape configuration](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config), [Kubernetes discovery](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#kubernetes_sd_config) and [PromQL rate semantics](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate). This adapter's strict adjacent deltas do not use PromQL reset adjustment or extrapolation.
- [kube-prometheus manifest](https://github.com/prometheus-operator/kube-prometheus/blob/main/manifests/kubernetesControlPlane-serviceMonitorKubelet.yaml#L56-L85) and [kube-prometheus-stack defaults](https://github.com/prometheus-community/helm-charts/blob/main/charts/kube-prometheus-stack/values.yaml).
- [Kubelet cAdvisor setup](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cadvisor/cadvisor_linux.go#L57-L111), [metric labels](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/server/server.go#L1359-L1393) and [Kubernetes v1.35 producer switch](https://github.com/kubernetes/kubernetes/blob/v1.35.0/pkg/kubelet/server/server.go#L426-L454).
- [Kubelet Pod cgroup parents](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cm/pod_container_manager_linux.go#L115-L134), [systemd conversion](https://github.com/kubernetes/kubernetes/blob/master/pkg/kubelet/cm/cgroup_manager_linux.go#L53-L105), [containerd path helper](https://github.com/containerd/containerd/blob/bc6efe74c55e588dded980f941a7b1ddd2f6e24d/internal/cri/server/helpers.go#L408-L417), [CRI-O cgroupfs](https://github.com/cri-o/cri-o/blob/main/internal/config/cgmgr/cgroupfs_linux.go#L45-L65) and [CRI-O systemd](https://github.com/cri-o/cri-o/blob/main/internal/config/cgmgr/systemd_linux.go#L65-L94).
