# AllotForge 0.9.0 candidate verification

This document records local candidate checks, not publication or a successful live integration. Remote CI must identify its exact tested commit in GitHub Actions.

## Local checks

- Full Python suite: **522 tests passed**, including 41 runner tests and 13 profile, manifest-preparation and workflow-contract tests.
- Full UI DOM/API suite: **356 checks passed** (64 main UI/API, 27 control, 57 operations, 24 assignments, 43 diagnostics/session and 141 advisor).
- Python compilation, all six JavaScript syntax checks, whitespace and local documentation links checked.
- A 0.9.0 wheel was built locally. Runtime dependencies and UI dependency lock entries are unchanged. Acceptance tools are source-checkout tooling, not wheel entry points.
- The official Kueue 0.20.5 manifest was downloaded and its exact 1,798,254 bytes and SHA-256 verified before YAML parsing. The preparation helper produced 92 resources with only the named controller image pinned to its verified digest. It did not contact or install into a cluster.

## Independent review

The reviewer approved scoped branch CI after checking all 54 focused tests, immutable private credential snapshots, fixed context and instance binding, subprocess bounds, precise object ownership and deletion preconditions, redacted evidence, failed-cleanup behavior, pinned upstream inputs, workflow shell syntax and explicit bounded cluster teardown. Unit-test transport doubles are not live Kubernetes evidence.

## New compatibility gate

A separate disposable CPU-only kind cluster pins Kubernetes 1.34.11 and Kueue 0.20.5 with v1beta2. The suite checks production resource-builder output against real APIs, queue activation, standalone CPU/memory Workload admission, a pending reservation, release and quota handoff, exact resource usage and fixture cleanup. The workflow destroys the cluster afterward. No workload Pods or Jobs are created by the runner. The Kueue installation itself runs controller Pods and installs privileged resources only inside this disposable test cluster.

This is generated-resource compatibility, not successful AllotForge managed-operator reconciliation. No GPU Node is invented, no hardware capacity is patched and no GPU workload runs. The compatibility matrix explicitly separates the older Kubernetes 1.31.4 native-admission gate, beta1 Ray bundle, OpenShift, physical GPUs, tenant authorization and production readiness.

## Publication gate and limitations

The candidate is prepared on a feature branch; main and published 0.8.0 remain unchanged unless a separately authorized update is completed. The new live integration has not been executed at the time this source document is authored. Inspect the exact CI run and its sanitized artifact for observed results. The report always states production_pass=false and cannot pass after fixture-cleanup failure.

The main release build now depends on Python 3.11/3.12 and UI checks, the existing disposable Kubernetes 1.31.4 admission checks and this new CPU-only Kueue gate. Only the existing publishing job receives contents-write permission. It verifies checksums and the tested main head before publishing a new development prerelease and tag. These gates do not establish real GPU execution, collector correctness, workload schedulability, production authorization, OpenShift support, CNI behavior or a sustained pilot.
