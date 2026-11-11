# Davano 0.2.0 source archive verification

Verified 2026-11-11T18:01:59.503868+00:00 against the source ZIP snapshot from which this repository was prepared.

## Passed

- Full Python unit/API-double/static-manifest suite: **273 tests passed**.
- UI suite: **132 checks passed**: 50 DOM/API, 27 control, 31 execution/telemetry, 24 assignment/reassignment.
- Python compilation: `davano` and `benchmarks` passed.
- JavaScript syntax: `app.js`, `features.js`, `operations.js`, `assignments.js` passed.
- Benchmark evidence refreshed: five repetitions of 200 CPU operations per operation type, with 20 warm-ups, on the six-node/nine-Pod synthetic fixture.
- Benchmark source SHA-256: `fa1a377ae18ece9c1d6657996714bd51f3bd8007934ff98368d7d63947eaf314`.
- Source selection excludes dependency directories, caches, runtime databases, private configuration, credentials, stale build output and superseded screenshots.
- Original ZIP contents were byte-verified against the stable source snapshot. The repository `SOURCE-MANIFEST.json` was regenerated for the published file selection.

## Included evidence and reproduction

- `benchmarks/results/local-cpu-2026-11-11.json`: raw timing samples, timestamps, environment and source/fixture hashes.
- `benchmarks/results/environment-2026-11-11.json`: local capability probe.
- `benchmarks/README.md`: benchmark methods, commands, comparison gates and staging prerequisites.
- `web/tests/GAP-VALIDATION.md`: UI validation scope and reproduction.
- The source ZIP includes stage 1–9 labelled static HTML/CSS fixture previews. Generated previews are excluded from this repository; reproduce them using `web/tests/render-gap-previews.py`. They are not real browser screenshots or live GPU/cluster evidence.

Run the repository README's Python and UI checks after installing its pinned development dependencies in your own development environment. This validation reused an existing development-only jsdom install; the ZIP contains the package lock but no dependency folders.

## Explicitly not verified

No live GPU workloads, competitor application deployments, real Kubernetes/OpenShift admission/scheduling, Kueue/KAI integration, actual GPU allocation/migration, container execution, or real-browser interaction/layout test ran here. No cluster was changed, credentials generated, new software installed, or money spent by this validation pass. Live GPU/competitor benchmarking still requires an explicitly authorized staging target and bounded workload scope.

CPU results measure real in-process wall time over synthetic inputs and establish no GPU performance advantage. Static checks and mocked transports do not certify production safety. License selection remains pending; this bundle asserts no open-source license grant.
