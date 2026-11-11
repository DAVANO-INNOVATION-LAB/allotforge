# UI implementation and verification

## Implemented

Davano's runtime UI has no external JavaScript, CSS, font, CDN, bundler or Node-server dependency. It is served by the same FastAPI application.

- Eight navigable views: overview, GPU inventory, local resource pools, observed workloads, local advisory policies, integrations, managed cluster control, and local configuration activity.
- Explicit source provenance, manual refresh, failure/retry, snapshot export, inventory/workload search and filters, node/resource details, and responsive navigation.
- Persistent local pool/policy creation and advisory single-node workload evaluation. These remain separate from managed Kubernetes CRDs and never imply quota enforcement.
- Ray workload planning through the actual `/api/v1/ray/plan` endpoint. The form covers the image/version/entrypoint, fixed workers, GPU resource, CPU/memory requests, and deadline. The review exposes prerequisites, limitations and the suspended RayJob JSON. Export is enabled only after a successful response that explicitly says not submitted/not allocated and has `spec.suspend: true`.
- Managed cluster pool/policy list, create, detail and delete workflows behind explicit server-side enablement and administrator authentication. Policies distinguish namespace-total GPU quota from the per-Pod cap. Subject types include Group, User and ServiceAccount, but they are **declarations only, not permission grants**. Automatic workload grants are disabled. Legacy grants may remain until their revocation is observed, and unrelated RBAC is unchanged.
- Pool status requires current-generation `Ready=True` before showing **Reconciled**. Policy status separates current-generation `GuardrailsReady`, overall `Ready` and `WorkloadAccess`. Native guardrails may reconcile while `Ready=False / WorkloadGrantsDisabled` and no policy access is granted. Legacy revocation pending means permissions may still exist. Missing/stale conditions cannot claim access revoked or guards current. An unexpected policy Ready/access True is flagged, never treated as supported tenant execution. Guardrail/access proof requires condition-specific observed generations. No status implies physical GPU isolation or production certification.
- Managed resource deletion requires the exact resource name. UID/resourceVersion are captured when the confirmation opens, so a subsequent list refresh cannot silently change the target. The UI says deletion requested, not deletion complete, and explains draining and removal of policy guardrails.

## Browser-side security and interrupted flows

- The administrator credential is kept only in memory. It is never written to localStorage, sessionStorage, cookies or exported snapshots. Cancel/close clears the input; disconnect invalidates outstanding reads and removes managed data.
- Credential entry is disabled on non-loopback HTTP origins. Requests use same-origin credentials, reject redirects and disable caching. The backend independently authenticates and validates actions; UI checks are not security enforcement.
- External resource names, labels, conditions, errors and manifest text are escaped. JSON/selector input is validated before local serialization and again by the backend.
- Pending form submissions are disabled to prevent duplicates. Changed inputs clear advisory/Ray results and disable obsolete exports. Late evaluation/planning responses cannot replace a newer dialog or a changed request.
- Native dialogs have labelled titles, buttons have explicit labels, tables have readable headings, status/error regions use live announcements, the skip link focuses the main content, and reduced motion is respected. Native keyboard focus and real-browser responsive behavior still need browser QA.

## Passed checks

- `node --check web/app.js` and `node --check web/features.js`.
- **45 DOM/API checks** in `web/tests/ui.test.cjs`, using jsdom and the actual FastAPI application through the in-process TestClient bridge. Each run uses an isolated temporary SQLite database with an explicitly synthetic inventory. No real cluster is contacted.
- **27 isolated UI control contract checks** in `web/tests/control.test.cjs`. These deliberately use an in-memory mock transport for enabled cluster-control responses and writes. They verify request shapes and UI security/interruption behavior; they do not prove Kubernetes integration or actual authentication enforcement.

API-backed coverage includes the original inventory/local-policy flows, all eight navigation views, error/retry/no-fallback, external text escaping, disabled control status, credential-form boundaries, successful suspended Ray planning, rejected images/caps, repeat submission prevention, changed-input races and close/new-dialog races.

Control-contract coverage additionally includes declaration-only subject copy, the eight-label selector limit, independent guardrail/access conditions, stale/missing generation proof, unexpected access grants and pending legacy revocation. It also includes enabled-but-unauthenticated status, denied reads, memory-only credentials, protected selector defaults, explicit write review, malformed JSON, correct pool/policy payloads, current/stale/deleting status, condition escaping, exact-name deletion and immutable preconditions, failed reads, disconnect during a request, cancel and insecure-origin credential rejection.

## Reproduce

From the repository root, install the Python development requirements. Then:

```sh
cd web
npm ci
npm run check
npm test
```

`jsdom` is development-only. Python tests cover the actual control API and controller separately.

## Visual evidence and limits

The supplied PNGs are **static renders of the implemented UI's DOM and CSS**, using real in-process API responses with fixture data. Their banners explicitly say **STATIC HTML/CSS PREVIEW · DEMO DATA · NOT BROWSER QA**. WeasyPrint adapts native controls, viewport sizes and modal positioning for rendering. These images are not evidence of Chromium/Safari/Firefox layout fidelity, real GPU hardware, an enabled cluster, or a deployment.

Additional stages:

- Stage 5: actual disabled cluster-control status and reported capabilities.
- Stage 6: actual suspended RayJob planning response, prerequisites and export flow.
- Stage 7: reported integration capabilities and a narrow-screen control preview.
- Stage 8: security-reviewed automatic-grants-disabled control view. This supersedes earlier stage copy that did not yet explain the fail-closed grant boundary.

For captures, run `DAVANO_CAPTURE_STATIC=1 npm test`, then `web/tests/render-previews.py` with WeasyPrint and BeautifulSoup installed. Convert the emitted PDFs with `pdftoppm`. Old intermediate screenshots can contain earlier wording; current source is authoritative.

**Not run:** native browser rendering, focus/keyboard behavior, download behavior in real browsers, mobile overflow QA, real Kubernetes/OpenShift admission, device plugins, GPUs or scheduler integration. Earlier browser startup could not initialize its required sockets and the available remote browser could not reach the local listener. No deployment or cluster change occurred during UI development.
