# AllotForge 0.5.0 verification

Local verification was performed on 2026-11-11 before publication. Remote CI status is recorded against the exact release commit in GitHub Actions, not anticipated here.

## Local checks

- Full Python suite: **339 tests passed**, including **32 deployment-diagnostics tests**.
- Full UI DOM/API suite: **200 checks passed** (64 main UI/API, 27 control, 57 operations, 24 assignments, 28 diagnostics).
- Python compilation, all five JavaScript syntax checks and whitespace checks passed.
- Diagnostics transport tests use a real ephemeral localhost HTTPS server for trusted/untrusted CA behavior, redirect rejection and error redaction. API tests cover the independent live administrator gate, disabled/demo isolation and runtime inventory failure.
- Independent review found no remaining blocking issue. A separate 636-case malformed-payload sweep found no source-data leakage or request-limit violations. Missing discovery verbs and numeric overflow handling were corrected before publication.
- Stage12 static previews cover initial, loading, demo, observed/blocked, stale and unauthorized states on desktop/mobile. They are labelled synthetic HTML/CSS previews, not browser QA or live cluster screenshots.

## Exact-commit release gates

The release workflow tests Python 3.11/3.12 and all UI contracts, then runs native admission and deployment checks on disposable Kubernetes 1.31.4. A new diagnostic parser smoke performs five fixed kubectl GETs under the existing CI identity: bounded Node/Pod lists, managed/native API discovery, and absent Kueue detection. The smoke checks its exact disposable context before reading; it never forwards its dummy fixture credential.

The parser smoke validates real API response shapes, not the application's HTTPS transport or administrator authentication, target least-privilege RBAC, Kueue operation, controller correctness or hardware. Those evidence scopes stay distinct. Existing native admission smoke remains a separate positive/negative API-server test.

Only publishing receives contents-write permission. After all gates pass, it builds exact-commit source and wheel artifacts, verifies checksums, verifies main's head, refuses existing tags and publishes a development prerelease targeted to the tested SHA.

## Limits

No production cluster, real Kueue service, CNI/tenant isolation, exporter deployment, workload runtime or GPU hardware was validated or provisioned. Local HTTPS and synthetic contract tests do not establish target compatibility. No live browser interaction/layout validation was completed; DOM/API checks and static previews are not browser certification. Existing RuntimeClass restrictions and prerequisites remain in force. Historical CPU benchmark files remain 0.2.0 evidence, not new performance results. License selection remains pending.
