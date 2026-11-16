# AllotForge evidence-driven roadmap

This is a development plan, not a statement that unimplemented features work.
Each feature should receive its own reviewed release after its acceptance gates.
The 0.9 candidate in PR #5 remains separate from later work; nothing here
approves merging, publication, cluster access, or expenditure.

## Next delivery slices

| Priority | Slice | Acceptance criteria and boundary |
| --- | --- | --- |
| 1 | Close 0.9 component acceptance | Preserve exact-commit Python/UI, native admission and CPU-only Kueue evidence; review and obtain the pending publication decision. No GPU or production claim. |
| 2 | Explicit Ray/Kueue API targeting | Preserve beta1 defaults, accept only beta1/beta2 per request, emit one consistent API in all bundle objects, echo the target in HTTP planning, and reject unsupported values. Validate against pinned schemas. Live Ray controller and GPU execution remain separate gates. |
| 3 | Managed operator lifecycle acceptance | On a separately authorized target, exercise create/update/restart/delete, UID ownership, observed-generation convergence and cleanup. Never fabricate GPU capacity to make eligibility pass. |
| 4 | Namespace CPU/memory ceilings | Extend policy schema, preview/CAS update and native guard rendering together; cover zero/reduction, stale resourceVersion, units/overflow and existing workloads. Prove enforcement with real API-server tests before advertising it. |
| 5 | Attributed GPU evidence | Validate timestamp and Pod/container/device identity against an approved live collector; stale, missing, shared or ambiguous evidence must remain unknown. CPU/GPU recommendations require matched trials, not device utilization alone. |
| 6 | Tenant authentication and audit | Define identity mapping and authorization independently of local advisory policies; test negative cross-project access, revocation, replay and redaction before a multi-user pilot. |
| 7 | Multi-cluster inventory | Read-only first, explicit stable cluster identities, separate credentials and timestamps, no aggregation of incompatible MIG/shared/whole units, and partial-failure isolation. No cross-cluster allocation or migration implication. |
| 8 | GPU partitioning and workload integrations | Add individually scoped hardware-backed MIG/time-slice or Ray/Slurm integrations only after target-specific review, reconciliation/rollback tests and measured hardware evidence. No provider purchase or driver change implied. |

## Current planning-only increment

The beta2 request-selection candidate implements slice 2's local code contracts.
It does not enable writes, auto-detect a cluster, change the managed-controller
flag, migrate exports, or make a KubeRay compatibility claim. The existing UI
continues using the backward-compatible beta1 default; API clients and pure
bundle builders may explicitly select beta2. UI target selection and a live
Ray/Kueue integration are later work, not implied by local tests.

## Decisions and external inputs

- Resolve the existing 0.9 integration/publication approval separately.
- Select a license before claiming a licensed open-source release.
- Confirm the GPU target, credentials and total budget/provider before paid
  or live-hardware validation. The previously discussed amount alone is not
  enough to provision a resource.
- Keep defaults local/demo and all consequential controls separately opt-in.
