# AllotForge 0.7.0 candidate verification

Local verification was performed on 2026-11-11. This document does not assert publication. Remote CI and any later release must identify the exact tested commit in GitHub Actions.

## Local checks

- Full Python suite: **444 tests passed**, including 48 cAdvisor tests and 17 smoke-verifier test methods with adversarial cases.
- Full UI DOM/API suite: **280 checks passed** (64 main UI/API, 27 control, 57 operations, 24 assignments, 43 diagnostics/session, 65 advisor).
- Python compilation, all six JavaScript syntax checks, whitespace and local documentation links checked.
- A 0.7.0 wheel was built locally. The cAdvisor module and updated UI assets are included; source-checkout benchmark tools remain in the source distribution rather than the application wheel.

## Feature review

The cAdvisor source was reviewed independently against source timestamp, current-incarnation identity, quantity and mode-separation boundaries. Disabled or mismatched injected readers cannot issue the wrong source query. Duplicate runtime identities cannot double-count CPU or memory. The interface withholds values unless source status and both source-level and per-metric timestamp assertions are consistent. Existing custom-adapter and session/navigation behavior remains covered.

The offline smoke verifier was independently exercised with 3,190 field mutations and 34 quantity edge cases. Oversized integers, contradictory attempts, duplicate Pod identities, malformed timezone offsets and output overwrites were checked. Internally consistent supplied evidence still does not authenticate a capture or certify GPU execution.

## Verification limits

The source-timestamp prerequisite remains an administrator assertion. No live kubelet, cAdvisor collector, ingestion pipeline, real GPU, target runtime or matched CPU/GPU performance trial was run. Request counts do not establish physical GPU exclusivity. The source modes are not mixed, and cAdvisor supplies no GPU activity or processor-suitability result.

Stage 15 desktop/inspection previews use synthetic fixtures and are visibly labelled static development-candidate previews, not browser QA or real telemetry. DOM/API tests do not establish native browser layout or interaction compatibility. Historical benchmark result files remain earlier evidence, not new performance results.

## Publication gate

The existing release workflow tests Python 3.11/3.12 and UI contracts, then runs native admission, diagnostic parser and deployment checks on disposable Kubernetes 1.31.4. These checks do not validate Kueue scheduling, CNI, production authorization, collectors or GPU execution. Only the publishing job receives contents-write permission; it verifies artifact checksums and the tested main head before creating a development prerelease and a new tag targeted to that commit.

This candidate is prepared on a feature branch. No production resources, paid workloads, runtime-socket access or collector permissions were provisioned. The trusted-operator access boundary, compatibility names and pending license selection remain in effect.
