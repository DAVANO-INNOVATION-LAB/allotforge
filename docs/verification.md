# Verification record — AllotForge 0.1.0

This is the historical **initial 0.1.0 snapshot** checked on 2026-11-11, not a current aggregate test count for the expanded 0.2.0 implementation. It is not a production-readiness certification. Later control, execution, observability, UI and benchmark features have separate tests and scope notes in their linked documentation; rerun the current suite rather than applying these old counts to the new code.

## Passed

- **66 Python tests** via `python -m unittest discover -s tests -v`. Covers the HTTP API, persistence, input validation, request-size limits, live-read failure without fixture fallback, single-node advisory fit, no allocation/submission side effects, policy caps, overlaps, GPU modes, request accounting and static manifest/CRD contracts.
- **31 DOM/API checks** run with jsdom and the actual FastAPI application through an in-process TestClient bridge. The UI is not backed by a fake API in these checks. An isolated SQLite database is used; no cluster is contacted. See [UI verification](ui-verification.md) for flow coverage and limitations.
- Python source compilation (`python -m compileall -q davano`) and JavaScript syntax (`node --check web/app.js`).
- Python wheel build. An extracted wheel in a separate directory served the HTML and JavaScript assets and returned the expected 36-unit labelled demo inventory through the real API. This verifies package asset inclusion, not a production server deployment.
- Static manifest checks: least-privilege inventory-only RBAC, no credentials in demo mode, non-root/restricted container configuration, private Service, default-deny ingress NetworkPolicy, explicit storage choices, and exclusion of draft CRDs from deployment roots.

The Python environment prints an upstream Starlette/httpx deprecation warning during tests. It did not fail any tests.

## Preview images

The generated PNGs under `screenshots/` (excluded from the repository) are **static renders of the implemented UI's DOM and CSS**, including state obtained from the real API in DOM tests. They are clearly labelled as static previews. They are not evidence of browser layout accuracy, physical GPU inventory, a connected cluster, or a deployed service. Intermediate-stage previews may show earlier wording; the source is authoritative.

## Not run or not available

- No end-to-end real browser rendering or browser-to-listening-server run. Execution sandboxes isolated localhost listeners from the available browser. DOM tests do not establish visual layout, native dialog behavior or browser history behavior in real Chromium/Safari/Firefox.
- No Docker image build or container-runtime test; Docker was unavailable.
- No Kustomize rendering, Kubernetes API-server schema validation, server-side dry-run, actual RBAC probes, PVC binding, OpenShift SCC admission or deployment. kubectl and a target cluster were unavailable.
- No physical GPU tests, NVIDIA/AMD/Intel device-plugin integration test, Kueue/Ray/Slurm integration, telemetry validation, performance/load test or production authentication audit.
- The supplied CI workflow has not run in a remote repository. No repository or image was published.

The live Kubernetes adapter's transport is exercised with controlled mocks, including pagination, service-account token rotation, errors, HTTPS configuration and redirect rejection. This is not equivalent to a live-cluster test.

## Repeat locally

```sh
python -m pip install -r requirements-dev.txt
python -m unittest discover -s tests -v
python -m compileall -q davano
node --check web/app.js
cd web
npm ci
npm test
```

Then start the server from the project root with `python -m davano.app` and review it in an actual browser at http://127.0.0.1:8080. Validate a separately approved target environment before changing the inventory source to Kubernetes. Keep demo and live databases separate.
