# Workload and observability UI verification

## Repeat the checks

From `web/`:

```sh
npm ci
npm run check
npm test
```

The suite currently has:

- 50 DOM/API checks against an isolated actual FastAPI demo instance.
- 27 cluster-control UI checks with an explicit transport fixture.
- 31 project-execution and observability UI checks with explicit transport fixtures.
- 24 namespace-allocation and workload-reassignment UI checks with explicit transport fixtures.

Execution coverage includes disabled authority, separate memory-only credentials, project isolation in UI requests, approved profiles, preview target verification, input changes during a pending preview, required review, duplicate clicks, observed status, original UID/resourceVersion cancellation, uncertain outcomes, failed reads, disconnect during pending reads, viewer permissions, restricted administrator inventory, and idempotent existing Jobs.

Observability coverage includes absent measurements, fresh zero, stale/unknown/conflicting values, workload scope suppression, exporter provenance, scheduler and queue reasons, escaping untrusted text, source-aware snapshot changes and explicit non-benchmark labels.

These are **DOM and API contract checks**, not a real browser, real Kubernetes cluster, admission controller, CNI, or GPU hardware test.

Namespace changes keep the pool fixed and preview before applying a UID/resourceVersion-guarded GPU quota or per-Pod-cap change. Workload moves are eligible suspended-only cancel-and-recreate operations, bound to a server fingerprint and authorized source and target projects. A durable operation waits for source and owned-child absence before replacement submission. UI checks cover fixed scope, stale plans, blocked changes, duplicate actions, uncertain outcomes, viewer restrictions, status observation and explicit advancement. They do not prove live migration.

## Stage images

Capture exercised DOM and render labelled static previews:

```sh
cd web
DAVANO_CAPTURE_STATIC=1 node tests/operations.test.cjs
DAVANO_CAPTURE_STATIC=1 node tests/assignments.test.cjs
cd ..
python -m pip install -r web/tests/requirements-render.txt
python web/tests/render-gap-previews.py
```

The renderer also needs the `pdftoppm` command from Poppler. It writes PNG, PDF and HTML files under `screenshots/gap-build/`. Every image prominently states **static HTML/CSS preview**, **synthetic UI contract fixture**, **no live cluster**, and **not browser QA**. Stage 1 and 2 include mobile-width variants. Fixtures with exporter values are test inputs only and are never part of the running demo adapter.

A Chromium/Playwright browser launch was attempted in the development sandbox on 2026-11-11. Chromium could not start because its socket operation returned `EPERM`, including an elevated attempt. Consequently browser interaction and browser layout checks remain unverified; the images use the static renderer and are not represented as browser screenshots.
