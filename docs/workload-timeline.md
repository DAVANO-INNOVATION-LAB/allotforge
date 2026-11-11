# Workload observation timeline

The Workload advisor inspection shows the accepted minute values behind a metric summary. This is a view of the current rolling 15-minute observation, not a stored history or a new measurement source. Refresh replaces the window; it does not accumulate a profile of the job.

## What each interval means

Each accepted metric has at most 15 one-minute intervals. Their boundaries are relative to the advisor observation window, not necessarily UTC clock-minute boundaries. The labels are **derived interval bounds**, not source-sample timestamps. Existing `sample_start` and `sample_end` retain the accepted source-observation bounds. Intervals use `[start, end)` assignment, except that a sample exactly at the observation-window end belongs to the final interval.

Within each interval, the existing advisor first averages each required container's samples. CPU and memory values sum those container means. Custom-adapter GPU values average them with requested-GPU-count weights. Only intervals shared by every required container have a value. These are the same values used by that metric's displayed mean and p95. They are not additional independent samples.

- **CPU:** cores, not percent of a host. In cAdvisor mode each input is an adjacent counter interval-average rate; the minute value is not exact time-weighted CPU consumption or an instantaneous peak.
- **Memory:** working-set bytes, displayed in MiB. Working set is not RSS, application heap, or a guarantee that memory is unreclaimable.
- **GPU:** custom-adapter attributed busy percentage only. A weighted multi-GPU mean can hide a busy device. cAdvisor mode supplies no GPU timeline.

The three metrics can have different missing intervals. The CPU/GPU assessment still uses its separately validated shared window and GPU-bearing-container CPU evidence. A chart is not a new suitability rule, cost estimate, scheduling guarantee or instruction to move a workload.

## Missing evidence stays missing

All existing attribution, current-incarnation, source-mode, timestamp, sample-count, freshness, gap and coverage gates apply before any numeric timeline is returned. A stale, conflicting, insufficient, disabled or unsupported metric has `timeline: null`. Partially accepted numeric intervals are not exposed to bypass a failed metric gate.

For an accepted metric, a missing shared minute has `value: null`. A valid measured zero remains `0`. The interface shows gaps rather than joining or interpolating across them, and provides a textual equivalent of the interval values. Unknown must never be interpreted as idle. Source failure, invalid timestamp-preservation assertions, refresh, navigation and credential changes must not preserve a stale inspected chart.

## Additive API contract

Each `state.workload_advisor.workloads[].metrics` entry adds `timeline`. When observed, its shape is:

```json
{
  "bucket_seconds": 60,
  "aggregation": "aligned-minute-means",
  "points": [
    {"window_start": "<derived interval start>", "window_end": "<derived interval end>", "value": 0.5}
  ]
}
```

The abbreviated example shows one point; an accepted timeline contains exactly 15 ordered contiguous points. Values use the enclosing metric's unit. Bounds cover the enclosing advisor's `window_start` through `window_end`. No source labels, container runtime identifiers, raw Pod specifications, credentials or additional raw samples are introduced. The field is additive; prior metric summaries and assessments retain their semantics.

The feature uses the existing bounded raw Prometheus response and accepted aggregate windows. It performs no additional network call, query, background polling, persistent storage, cluster write, allocation or workload execution. The existing source response, series and sample limits remain in force. It does not broaden the inventory/advisor authentication boundary; use a trusted local/operator workspace or a reviewed authenticated proxy as described in [workload advisory](workload-advisor.md).

## Verification scope

Automated tests use synthetic observations and DOM/API contracts. Static previews must be labelled synthetic and not native browser QA. No live cAdvisor pipeline, real GPU, target cluster or production workload is validated by these tests. See the [cAdvisor source requirements](cadvisor-cpu-memory.md) and [capability boundaries](capabilities.md).
