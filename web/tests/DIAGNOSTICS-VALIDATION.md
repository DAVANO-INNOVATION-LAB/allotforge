# Deployment diagnostics UI validation

The deployment diagnostics page makes one explicit, same-origin `GET /api/v1/diagnostics` request when Run checks is selected. Route entry, inventory refresh and ordinary navigation do not trigger probes. The page remains available when inventory fails or returns an authorization error.

## Checked locally

- `npm run check`: syntax checks passed for all five UI modules.
- `npm test`: 200 checks passed: 64 DOM/API, 27 control, 57 operations, 24 assignments and 28 diagnostics.
- The DOM/API suite consumes the actual demo diagnostics endpoint through the isolated in-process API bridge. It verifies the ten known checks and absence of live observations.
- Diagnostics fixtures exercise repeated clicks, navigation away, clear/cancel, late and out-of-order responses, credential replacement/disconnect, project-versus-administrator authority, unauthorized/unavailable/timeout responses, stale observations, unknown/malformed/duplicate checks, malicious strings, mismatched evidence, non-string fields and demo/live boundaries.
- Raw transport errors are not displayed. Known response strings are bounded and escaped. Unknown result shapes remain unverified.
- `git diff --check`: passed.

## Reproduce labeled static previews

From the repository root:

```sh
DAVANO_CAPTURE_STATIC=1 node web/tests/diagnostics.test.cjs
PYTHONPATH=/tmp/allotforge-render-libs python web/tests/render-diagnostics-previews.py
```

The optional rendering dependencies are listed in `requirements-render.txt`. The `PYTHONPATH` above describes this validation environment; install or select those dependencies appropriately elsewhere.

Stage 12 captures are written under `screenshots/gap-build/`:

- `stage-12-diagnostics-empty.png`
- `stage-12-diagnostics-loading.png`
- `stage-12-diagnostics-observed.png`
- `stage-12-diagnostics-observed-mobile.png`
- `stage-12-diagnostics-stale.png`
- `stage-12-diagnostics-unauthorized.png`
- `stage-12-diagnostics-unauthorized-mobile.png`
- `stage-12-diagnostics-demo.png`

Each has its captured DOM and a rendered HTML/PDF counterpart. The previews are prominently labeled **STATIC HTML/CSS PREVIEW · SYNTHETIC CONTRACT FIXTURE · NOT BROWSER QA**. All eight PDFs were checked to fit one page. Desktop/mobile states were visually inspected; credential-button contrast, empty-section wrapping and mobile mode wrapping were corrected.

## Limits

jsdom verifies DOM behavior and transport contracts, not browser rendering, keyboard behavior in an actual browser, GPU hardware, Kubernetes deployment, admission enforcement, isolation or runtime correctness. No native browser or real cluster validation was performed for this UI change. Static rendering is not a substitute for that verification. Configuration, individual API observations and unverified guarantees remain separate; no overall production-readiness score is inferred.
