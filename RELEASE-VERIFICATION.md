# AllotForge 0.3.0 local verification

Verified against the 0.3.0 working tree on 2026-11-11 before publication. This file records local verification, not a claim that remote CI or GPU hardware testing has passed.

## Passed locally

- Full Python unit/API-double/static-manifest suite: **290 tests passed**.
- UI DOM/API suite: **143 checks passed** (61 main UI, 27 control, 31 execution/telemetry, 24 assignment/reassignment).
- Python compilation: `davano` and `benchmarks`.
- JavaScript syntax: app, features, operations and assignments.
- Wheel build: `davano-0.3.0-py3-none-any.whl`; the package/import name is retained for compatibility, with an additional `allotforge` command.
- Independent review of request-accounting changes and release workflow. Malformed GPU-map and CPU-only Pod-condition regressions found during review were fixed and covered by tests.
- Three labelled static HTML/CSS previews exercise synthetic fit, CPU-blocked and unknown-CPU scenarios. These are not browser QA or live cluster evidence. Preview files are generated artifacts and excluded from the repository source manifest.

The first remote native-admission attempt exposed a pre-existing CRD CEL reserved-field reference. The `namespace` field now uses its required CEL escape, with static and real-API subject allow/deny regression coverage. A second server check exposed an unsupported Pod overhead CEL field. The guard now conservatively disallows RuntimeClass on all new Pods in managed namespaces, with explicit built-in RuntimeClass-admission prerequisites and real-server bypass tests. Publication remains gated on a successful fresh run.

## Release gates

The release workflow tests Python 3.11/3.12 and UI contracts, runs the existing disposable-kind native-admission/Kustomize checks, then builds exact-commit source and wheel artifacts. Only the publishing job has contents-write permission. It checks the current main head, verifies artifact checksums, refuses an existing tag and creates a development prerelease targeted to that tested commit. Check the exact commit's GitHub Actions result for remote verification; this local record does not anticipate its outcome.

## Not verified locally

No real GPU, live cluster admission/scheduling, Kueue/KAI runtime, hardware allocation, migration, exporter, production tenancy or competitor platform was exercised. Local Chromium could not start due to environment socket restrictions; the available browser could not open the local app. DOM/API checks and labelled static previews do not certify actual browser layout or interaction behavior. No cloud GPU resources were provisioned, credentials generated or production cluster changed.

The historical files in `benchmarks/results/` measure the prior 0.2.0 source/fixture and are not fresh performance results for 0.3.0. They establish no GPU speedup. License selection remains pending; no open-source license grant is asserted.

Reproduce with the commands in README.md. Static preview tooling uses optional pinned development dependencies from `web/tests/requirements-render.txt`.
