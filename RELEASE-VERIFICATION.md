# AllotForge 0.6.0 verification

Verification was performed on 2026-11-11 before publication. Remote CI status is recorded against the exact release commit in GitHub Actions, not anticipated here.

## Local checks

- Full Python suite: **379 tests passed**, including 40 workload-advisor and 32 diagnostics tests.
- Full UI DOM/API suite: **241 checks passed** (64 main UI/API, 27 control, 57 operations, 24 assignments, 43 diagnostics/session, 26 advisor).
- Python compilation, all six JavaScript syntax checks and whitespace checks passed.

## Feature and integration review

The workload advisor was independently reviewed in isolation and again after integration with deployment diagnostics. Reproductions that could create false CPU-comparison suggestions from unpaired windows, unrelated CPU-only containers or multi-GPU averages were corrected with regressions. Exact fractional GPU parsing, malformed response sanitization and stale inspection handling were also tightened.

The combined UI review exercises administrator-session changes during pending inventory refreshes. Old successes/errors cannot populate evidence after credential replacement, disconnect or change-and-revert. The report is advisory only and preserves performance/cost uncertainty.

## Verification scope

Python tests cover identity attribution, raw timestamps, metric units, aligned buckets, minimum samples/span/coverage, gaps, restarts, duplicate/conflicting series, custom extended resources, malformed inputs, no-demo measurements, privacy and failure isolation. UI checks cover all existing route families, both new screens and interrupted/late responses. Real localhost HTTPS regression tests validate CA, redirect and sanitized-error behavior for the transport paths they exercise.

Stage 13 desktop/mobile/inspection previews use synthetic fixtures and are visibly labelled static, not browser QA. No live browser, trusted attribution adapter, real GPU or matched CPU/GPU performance trial was run. Historical CPU benchmark result files remain earlier evidence, not a new speed or cost comparison.

## Exact-commit release gate

The release workflow tests Python 3.11/3.12 and UI contracts, then runs native admission, diagnostic parser and deployment checks on disposable Kubernetes 1.31.4. These API-server checks do not validate Kueue scheduling, collector correctness, CNI, production authorization or workload/GPU execution. Only the publishing job receives contents-write permission; it verifies artifact checksums and the tested main head before creating a development prerelease and a new tag targeted to that commit.

No production resources were provisioned or altered. RuntimeClass restrictions, target validation requirements, the trusted-operator access boundary and pending license selection remain in effect.
