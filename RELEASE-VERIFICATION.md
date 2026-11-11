# AllotForge 0.4.0 verification

Local verification was performed on 2026-11-11 before publication. Remote CI status is recorded against the exact release commit in GitHub Actions, not anticipated here.

## Local checks

- Full Python unit/API-double/static-manifest suite: **307 tests passed**.
- Full UI DOM/API suite: **169 checks passed** (61 main UI, 27 control, 57 execution/telemetry/visibility, 24 assignment/reassignment).
- Python compilation and all four JavaScript syntax checks passed.
- Independent review verified the pinned Kueue v0.20.5 contract, including omitted Workload UID and zero-based queue indices. No blocking findings remain.
- Stage11 labelled static previews cover upstream-style identity-unverified summary entries, partial windows and unavailable visibility. They are synthetic HTML/CSS previews, not browser QA or live Kueue screenshots.

## Release gates

The release workflow tests Python 3.11/3.12 and UI contracts, runs existing disposable-kind native admission and Kustomize/server checks, then builds exact-commit source and wheel artifacts. Only publishing has contents-write permission. It verifies checksums, checks the main head, refuses existing tags and publishes a development prerelease targeted to the tested SHA.

## Limits

New visibility checks use synthetic transport/API doubles. No live Kueue visibility service, production permission binding, real queue scheduling, exporter or GPU hardware was tested. RuntimeClass restrictions and prerequisites from 0.3.0 remain in force. Existing Kubernetes CI is real API-server validation on a disposable CPU-only kind cluster, not a GPU/Kueue deployment test.

Local browser interaction/layout remains unverified; DOM/API checks and static previews are not browser certification. Historical CPU benchmark files remain 0.2.0 evidence, not new performance results. No production cluster or GPU resources were provisioned. License selection remains pending.
