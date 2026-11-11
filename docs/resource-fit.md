# Explainable GPU, CPU and memory fit

The fit planner answers a deliberately narrow question: does one eligible node's observed request budget fit this workload? It never reserves, submits, allocates or guarantees scheduler placement.

## Inputs and evidence

Supply a local pool, namespace, whole-GPU count and vendor. Optionally supply CPU millicores (1,000 = 1 core) and memory MiB (1,024 = 1 GiB). Blank or null skips that check. Zero is valid and needs no capacity evidence. Any positive requested dimension needs known allocatable capacity and known effective requests on the same node as the GPU candidate.

All bound nonterminal Pods count, including CPU-only Pods, Pending Pods with a node assignment and terminating Pods. Unbound Pods and Succeeded/Failed Pods do not consume an observed node request budget. Requests are not utilization, process RSS or physical idle hardware.

Container requests, limit-only defaulting, ordinary init peaks, restartable init sidecars and Pod overhead are supported. Missing request and limit means a zero scheduler request. Allocatable resources, rather than hardware capacity, determine the remaining budget. Requests and overhead round up to displayed millicores/MiB; capacity rounds down. This conservative rounding can understate remaining capacity.

## Fail-closed boundaries

Malformed quantities and unsupported resource evidence stay unknown. Pod-level CPU/memory budgets and in-place-resize evidence are not implemented; affected dimensions become unknown rather than assuming the old request still applies. A missing compute dimension does not invalidate an independent GPU-only check. Whole-GPU classification, readiness, cordon, taint and local policy checks remain in force.

Results include one explanation per observed node. `eligible` means checked constraints fit, `rejected` means a known constraint fails, and `unknown` means required evidence is missing. A known failure remains blocked even if another dimension is unknown. No capacity is summed across nodes or incompatible GPU resource names.

## Synthetic demo examples

- training / research / NVIDIA / 4 GPUs / 8,000 millicores / 32,768 MiB: fits the observed h100-02 budget.
- The same request with 12,000 millicores: blocked because the GPU-compatible node has only 10,000 millicores left after its GPU and CPU-only Pod requests.
- research / research / AMD / 2 GPUs / 1,000 millicores: unknown because the AMD fixture deliberately omits CPU allocatable evidence.

These are invented fixture budgets, not GPU hardware specifications or a live cluster demonstration. Quotas, topology, storage, affinity, gang scheduling, device availability, concurrent scheduling and actual admission remain outside this planner. The Ray manifest planner keeps its existing separate contract and does not gain a scheduler guarantee.
