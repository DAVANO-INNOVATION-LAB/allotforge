# AllotForge 0.8.0 candidate verification

This document describes the candidate verification scope, not publication. Any later remote CI or release must identify its exact tested commit in GitHub Actions.

## Local checks

- Full Python suite: **468 tests passed**, including 24 new timeline tests. Existing source, advisor, smoke-verifier and control tests remain included.
- Full UI DOM/API suite: **356 checks passed** (64 main UI/API, 27 control, 57 operations, 24 assignments, 43 diagnostics/session and 141 advisor).
- Python compilation, all six JavaScript syntax checks, whitespace and local document links checked.
- A 0.8.0 wheel was built locally with the updated advisor module and interface assets. Dependencies are unchanged.

## Independent review

Randomized comparison of 240 synthetic backend cases against the 0.7.0 baseline found identical prior summaries and assessments after stripping only the new timeline field. Additional cross-language checks generated 120 backend snapshots with microsecond bounds in both source modes: all 300 observed timelines rendered, all 60 cAdvisor GPU timelines remained withheld, and 23 malformed/source/unit/precision mutations were rejected.

The review also checked refresh and navigation invalidation, pending-load reopening, new-session credential changes and late old-session responses. Old inspected chart content is erased at refresh/navigation start. A pending refresh cannot reopen old evidence; late responses cannot cross the existing inventory session boundary. These checks are synthetic, not native browser or production authentication certification.

## Data and interpretation boundaries

Only existing accepted metric windows become timelines. Every non-observed metric returns a null timeline. An accepted metric has exactly 15 rolling interval bounds, with null for any missing shared minute and zero only for a measured zero. Values retain the existing per-container mean/sum or GPU-weighted mean semantics. Timeline data do not alter the CPU/GPU assessment or imply whole-job profiling, performance, cost, physical device ownership or guaranteed capacity.

The cAdvisor source remains CPU/RAM-only and requires administrator-asserted timestamp preservation. No real cAdvisor collector, ingestion pipeline, GPU, target cluster or matched processor trial was run. No additional query, background polling, history store, runtime-socket access, cluster writes or paid resources were added.

## Publication gate

Stage 16 previews use the actual interface with synthetic observations and are visibly labelled static development-candidate previews, not native browser QA or real telemetry. The source and tests are prepared on a feature branch; main and the 0.7.0 release are unchanged unless a separately authorized update is completed.

The existing main-branch release workflow requires Python 3.11/3.12 and UI checks, native admission/diagnostic parser/deployment checks on disposable Kubernetes 1.31.4, then a build of the exact source. Only the publishing job receives contents-write permission. It verifies artifact checksums and the tested main head before creating a development prerelease and a new tag. This gate does not validate real GPU execution, production authorization, Kueue scheduling, CNI or collector correctness.
