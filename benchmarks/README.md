# Evidence-first GPU benchmark harness

**Live GPU/competitor benchmark status: not run.** The connected Linux environment
has no NVIDIA device nodes, NVIDIA CLI, `kubectl`, Docker, kind or Helm. No target
cluster, GPU host, competitor endpoint or hardware configuration was supplied.
The environment catalog also reported the user's Mac offline. No computer was
started, cluster contacted, credentials read, software installed or cloud spend
incurred. This is a hardware/access blocker, not a denied cluster operation.

The code below is executable preparation plus locally measured CPU results. It
is not evidence that Davano is faster than a competitor or production-ready.

## What has actually been run

- `python -m benchmarks.harness environment`: local executable/device presence
  checks, timestamped in `results/environment-2026-11-11.json`.
- `python -m benchmarks.harness micro`: real monotonic-clock CPU timings of
  normalization, state and advisory evaluation, using the explicitly synthetic
  fixture. Five repetitions of 200 operations each, after 20 warm-up operations.
  Every timing is retained in `results/local-cpu-2026-11-11.json` together with
  source/fixture hashes, runtime and scope. No GPU, network, HTTP or scheduling
  performance is measured. Shared-host noise is visible; do not use these as a
  production sizing forecast.
- Unit tests exercise manifest parity, export timestamps, missing metrics,
  failed attempts, comparison rejection, deterministic paired intervals, TLS/
  redirect boundaries and a **localhost mock**, not a real GPUStack deployment.

The local data should be regenerated after final source changes, retaining its
own source hash. Do not compare different source snapshots without describing
what changed. Python dependencies are the same as the repository's existing
local test environment; this tooling installs nothing.

## Keep the comparison lanes separate

| Lane | What can be compared | What must not be implied |
| --- | --- | --- |
| Batch/scheduling | Same single-GPU Job under Kueue, Davano-managed Kueue policies and KAI | Davano is a separate GPU kernel or inference engine |
| Inference | GPUStack vs an explicitly selected equivalent serving baseline, such as the same vLLM engine served directly | Davano currently offers an equivalent model-serving API |
| Davano control plane | CPU cost and functional correctness of inventory, admission, submission and observation | In-process fixture latency is real cluster or GPU performance |

The `davano-kueue` manifest adapter produces the same Kueue Job and relies on
**already installed** Davano-managed policies/queues. This isolates deployment
configuration overhead. It does **not** measure Davano's HTTP submit route. To
measure that route, provision an administrator-approved profile for the exact
same benchmark payload using Davano's project workflow and separately time the
client submit request. Keep that interval separate from API-server Job creation
time. No profile is provisioned by this harness.

GPUStack is not a Kubernetes queue replacement. KAI is not an inference server.
Compare overlapping capabilities rather than forcing a misleading leaderboard.

## Commands

Run from the source checkout:

```sh
python -m benchmarks.harness environment --output /tmp/environment.json
python -m benchmarks.harness micro --iterations 200 --repeats 5 --output /tmp/local-cpu.json
python -m unittest discover -s tests -p test_benchmark_harness.py -v
```

### Offline batch plans

Copy and fill `examples/batch-plan.template.json`. The placeholder image is
intentionally invalid: use an administrator-approved PyTorch CUDA image pinned
by its real SHA-256 digest. The image must contain Python/PyTorch, work under a
non-root read-only filesystem and match the installed NVIDIA driver. No images
or model weights are downloaded by the planner.

```sh
python -m benchmarks.harness plan --product kueue --config batch-plan.json --output /tmp/kueue-job.json
python -m benchmarks.harness plan --product davano-kueue --config batch-plan.json --output /tmp/davano-job.json
python -m benchmarks.harness plan --product kai --config batch-plan.json --output /tmp/kai-job.json
```

All adapters use the identical fixed Python payload, 1 whole NVIDIA GPU,
2 vCPUs, 4 GiB memory, fixed node selector, disabled service-account token mount,
non-root settings, zero Job retries and a 600-second active deadline. No cleanup
TTL is set: preserve raw evidence before an authorized administrator removes
objects. The active deadline does not bound a suspended Job's queue wait, so
pre-register an external queue timeout and retain timeout cases.

Kueue Jobs begin suspended, as required by Kueue admission. KAI Jobs begin
unsuspended with `kai-scheduler` and `kai.scheduler/queue`; KAI does not unsuspend
Jobs. The generated files are inert local output, **but applying a KAI file will
start a workload**. The tool has no apply/install/delete command. It does not
create namespaces, queues, ResourceFlavors, credentials or cluster permissions.
Only run these files after approving the staging target, namespace, scope and
budget. Never point test commands at an inferred current production context.

`gpu_payload.py` performs 4096×4096 FP16 matrix multiplications, 10 warm-ups and
100 measured iterations with CUDA synchronization. It requires exactly one
visible GPU and refuses CPU fallback. It reports raw step times, observed GPU
name/memory, PyTorch/CUDA versions and a finite-output smoke check. These are
**inner iterations**, not 100 independent experimental replications. Container
startup, scheduler delay and per-step GPU execution are different intervals.
This workload does not test model inference, training convergence, NCCL,
multi-GPU topology, MIG, sharing or all vendor hardware.

### Import actual Kubernetes exports

Capture the original Job and all relevant Pods as JSON, preserving UIDs,
ownerReferences, namespaces and status. Keep the full immutable capture outside
public source control. Then:

```sh
python -m benchmarks.harness import-job --job job.json --pods pods-list.json --output sample.json
```

The importer joins controller owner UIDs, not reused Job names. It rejects
multiple attempts or container restarts instead of selecting the fastest. A
successful benchmark container is not a successful sample until the Pod is
Succeeded. Missing events remain null, pending work remains incomplete and
negative timestamp intervals fail as a clock-skew warning. Each imported sample
includes a source-export hash. It reports:

- `submit_to_scheduled_ms`: Job creation to PodScheduled transition. This
  includes Kueue admission, Job-controller and scheduler delays, not only queue
  time and not client submission latency.
- `submit_to_started_ms`: Job creation to benchmark container start. This also
  includes image pull and runtime startup.
- `submit_to_finished_ms`: Job creation to benchmark container finish.
- `container_runtime_ms`: container start to finish, including Python/CUDA init.

Kubernetes timestamps come from different components and have finite resolution.
Record NTP/chrony health and an error bound. Do not interpret sub-resolution
changes as real effects. Preserve rejected submissions and queued timeouts in
the campaign ledger even when no Job/Pod exists. The single-export importer
cannot independently discover missing attempted submissions.

### Optional inference endpoint runner

Copy and fill `examples/inference.template.json`. Use only an existing endpoint
explicitly approved for this load and any associated cost. Select the exact
GPUStack or vLLM version, model/weights revision and verified hardware/runtime
record. The runner sends a fixed public synthetic matrix-multiplication prompt,
not private user data. No model weights are downloaded. If needed, provide an
existing approved credential file; never put a key in the URL, JSON config or
command line. No credential or response text is stored in the result.

```sh
python -m benchmarks.harness inference --config inference.json \
  --base-url https://approved-staging.example/v1 \
  --key-file /approved/secret/file --output block-001.json
```

This is an explicit network/load operation, not an environment check. The
harness does not run it automatically. HTTPS is required except for explicit
loopback fixture endpoints; redirects are rejected. Limits are 1,000 measured
requests, 64 concurrent requests, 1,024 output tokens per request and a
300-second socket timeout. Three sequential warm-ups are extra requests and
abort measurement if any fail. The fixed prompt can hit prompt caches, so
record cache policy and compare cache-on/off experiments separately.

The runner records each attempted request and failure. Latency ends when the
non-streaming body finishes. It cannot measure TTFT, which stays null. Token
counts are server-reported; absent usage is unknown. Output tokens/second is
sum of completion tokens divided by total measured run wall time and is null
if any attempt fails or lacks usage. No mean of per-request token rates is
used. HTTP requests do not attest that a GPU ran. Client/server clocks do not
enter latency arithmetic; the client uses a monotonic clock.

## Offline GPU smoke evidence

The development `verify-smoke` command joins the fixed CUDA payload's stdout to
exported Job/Pod evidence and an explicit capture receipt. It performs no network
requests, reads no credentials and starts no workloads. Passing means the
operator-supplied files are **internally consistent**, not independently attested
GPU execution or certified whole-GPU isolation. It does not create a campaign
envelope, relax comparison gates or promote synthetic fixtures to live results.

Use only the approved dedicated staging target and an already authorized smoke
Job. Preserve complete exports and every attempt, including unsuccessful runs.
After the Job terminates, capture its Job object and the complete Pod list from
the dedicated namespace, then the single benchmark container's complete stdout.
Choose the Pod by its controller owner UID matching the exported Job UID, not a
name alone. Read the Pod UID and current container ID again after capturing logs;
if they changed, retain the capture as invalid rather than relabeling it. Do not
use `--previous`, `--timestamps`, `--tail`, a line filter or a selected "best" JSON
record. The verifier requires exactly one complete JSON stdout object.

These example commands are manual read-only capture instructions, not actions
the harness executes. Fill the explicit approved context, namespace and names
before running them. They do not authorize applying or creating a Job:

```sh
kubectl --context "$APPROVED_CONTEXT" --namespace "$APPROVED_NAMESPACE" \
  get job "$JOB_NAME" -o json > job.json
kubectl --context "$APPROVED_CONTEXT" --namespace "$APPROVED_NAMESPACE" \
  get pods -o json > pods.json
# First confirm POD_NAME's controller owner UID is the captured Job UID.
kubectl --context "$APPROVED_CONTEXT" --namespace "$APPROVED_NAMESPACE" \
  logs "$POD_NAME" -c benchmark > stdout.json
# Recheck this Pod's UID and current benchmark containerID before making a receipt.
kubectl --context "$APPROVED_CONTEXT" --namespace "$APPROVED_NAMESPACE" \
  get pod "$POD_NAME" -o json > pod-after-capture.json
```

Fill a receipt with the actual observed identities, exact raw stdout file hash
and timezone-aware capture time. `containerID` includes its runtime prefix.
`imageID` is copied exactly from the benchmark container status, not substituted
with the requested image digest. The following template is intentionally invalid
until replaced with real capture facts:

```json
{
  "schema_version": 1,
  "namespace": "REPLACE_WITH_CAPTURED_NAMESPACE",
  "jobUID": "REPLACE_WITH_JOB_UID",
  "podUID": "REPLACE_WITH_POD_UID",
  "containerName": "benchmark",
  "containerID": "REPLACE_WITH_FULL_CONTAINER_RUNTIME_ID",
  "sampleID": "REPLACE_WITH_PREREGISTERED_SAMPLE_ID",
  "imageID": "REPLACE_WITH_OBSERVED_IMAGE_ID",
  "stdoutSHA256": "REPLACE_WITH_SHA256_OF_EXACT_STDOUT_BYTES",
  "capturedAt": "REPLACE_WITH_TIMEZONE_AWARE_CAPTURE_TIMESTAMP"
}
```

The receipt is an operator declaration, not an authenticated provenance chain.
The verifier cannot discover deleted attempts, missing log records, forged
exports or a false receipt. Keep the original files private and independently
audit the capture before relying on it.

```sh
python -m benchmarks.harness verify-smoke --job job.json --pods pods.json \
  --stdout stdout.json --receipt receipt.json --output smoke-verification.json
python -m unittest discover -s tests -p test_gpu_evidence.py -v
```

The check requires the exact fixed payload and its SHA-256, matching sample
labels/environment, digest-pinned requested image parity, the expected resource
requests/limits, exactly one controller-owned Pod and one regular benchmark
container, no restart/init/ephemeral attempts, successful termination and complete
raw payload timings. Missing evidence remains incomplete; contradictory evidence
is rejected. Non-finite values, duplicate JSON keys, additional stdout records,
deep nesting and oversized inputs are rejected. The command exits nonzero unless
the supplied evidence is internally consistent. A report output file must not
already exist; this prevents accidentally overwriting capture files or an earlier
report. Omit `--output` to print the report to stdout instead.

Inputs are bounded regular files: Job 1 MiB, Pod list 8 MiB, stdout 256 KiB and
receipt 64 KiB, with a maximum JSON nesting depth of 64. It reports raw-file
hashes and the single smoke's raw step times without copying full Pod manifests,
environment variables or raw error content. Cross-component timing checks allow
a fixed two seconds of clock difference. The sum of step timings must fit its
reported interval with only the serialized endpoint resolutions as allowance,
not that two-second clock allowance. These checks tolerate coarse event
timestamps but do not validate clock health.
Requested images and runtime image IDs are separate observations; their digest
mapping remains unknown. MIG/sharing/exclusivity, correctness beyond the finite
output smoke check, performance superiority and savings remain unverified.

See [GPU validation and attribution acceptance](../docs/gpu-validation.md) for
the primary-source research and hardware test matrix that must precede a trusted
per-container GPU collector. This verifier is not that collector.

## Fair experiment design and comparison gate

1. Pre-register at least 20 paired **independent run blocks**, attempt IDs,
   workload, arrival trace, queue timeout, SLO, success definition, metric and
   stopping rule before seeing results. For every paired block use the same
   input/seed and same reserved hardware. Alternate AB/BA order with a fixed
   randomization seed. Do not run compared products simultaneously on the same
   GPUs. Record cooldown/background load and retain every attempted run.
2. Fix or record GPU model/count/memory/UUID-set hash, MIG/sharing mode, power
   limit/clocks, CPU/memory/interconnect, driver/CUDA/Kubernetes versions,
   immutable workload image and engine settings. Pin the tested product versions
   and configuration hashes. Node labels alone do not attest physical identity.
3. Match GPU quotas, priority, borrowing, preemption, fair-share weights,
   node eligibility and security controls. A security-disabled baseline is a
   separate trade-off experiment, not an apples-to-apples speed win.
4. Separate warm/cold image and model cache runs. Match model and tokenizer
   hashes, quantization, context/output lengths, batching, tensor parallelism,
   arrival pattern, concurrency and cache policy for inference.
5. Retain raw exports/logs, hardware evidence, exact commands/configuration,
   UTC start/end times, source revisions and hashes. Never commit credentials
   or private cluster inventories. A hash is provenance, not proof of truth.
6. Treat a repeated request set as one inference block. Concurrent requests and
   inner CUDA iterations are correlated and must not inflate independent n.

After each inference block has a unique preassigned `block_id`:

```sh
python -m benchmarks.harness aggregate --runs baseline-block-*.json --output baseline-campaign.json
python -m benchmarks.harness aggregate --runs candidate-block-*.json --output candidate-campaign.json
python -m benchmarks.harness compare --baseline baseline-campaign.json \
  --candidate candidate-campaign.json --output comparison.json
```

For batch campaigns, collect each imported independent block in the normalized
run envelope described by `validate_run`, with `sampling_unit` set to
`independent-run-block`, exact `METRICS` definitions, product name/version,
suite/version, UTC run bounds, the five `PARITY_FIELDS` sections and provenance.
All attempted block IDs must be retained. The harness verifies the envelope and
pairing; it does not create a live evidence envelope from synthetic fixtures.

The comparator refuses deltas for non-live evidence, unknown/mismatched parity,
changed metric semantics, absent provenance, unknown product versions, failures,
incomplete attempts, mismatched attempted IDs, request-level sampling or fewer
than 20 blocks. Per-metric missing values suppress that metric's delta. Zero
baselines have null percentage change. Both sides retain their own error rates
and metric coverage. Identical operator declarations cannot independently prove
hardware equivalence; audit the underlying evidence before publishing.

Deltas are **candidate minus baseline**, with units, paired block count,
relative percent and a seeded 2,000-resample paired-mean bootstrap interval.
Negative latency deltas mean lower latency. There is no automatic winner label,
causal claim or multiple-comparison correction. Pre-register the primary metric
and practically meaningful threshold. Tail quantiles are nearest-rank; the
minimum 20 blocks does not make p99 reliable. Use a larger preregistered sample
for tails and keep block-level uncertainty separate from request quantiles.

## Diagnosing an observed gap

- Longer submission-to-scheduled but unchanged GPU step time: inspect admission,
  queue eligibility, quota/borrowing, controller/scheduler logs and API throttling.
  Separate Job-controller time from Kueue admission using Workload conditions.
- Longer scheduled-to-started: inspect image pulls, mounts, container runtime and
  CUDA initialization before blaming the scheduler.
- Longer GPU steps on the same workload: inspect power/thermal throttling,
  clocks, driver/runtime, device placement, sharing and competing processes.
- Inference latency/tokens differences: check actual output token counts, model
  revision, quantization, engine/batching, context lengths, caching and network.
  Equal `max_tokens` does not guarantee equal generated lengths.
- Low observed utilization: cross-check raw DCGM/AMD telemetry sample freshness
  and coverage, CPU/IO starvation and workload shape. Requested GPUs are not
  utilization. Missing telemetry is not zero; stale values cannot support a win.
- Rejections/timeouts: report reliability before successful-only latency. Never
  discard them to improve the benchmark.

## Smallest next input for a live campaign

Provide an **authorized disposable staging GPU cluster or GPU host**, its exact
context/endpoint and dedicated namespace, and explicit permission for the
bounded workload/competitor configuration. Specify GPU model/count and the
existing GPU operator/device plugin, installed product versions, an approved
image digest, queue/policy setup and a maximum duration/resource budget. For
GPUStack provide the approved deployed endpoint/model and secure credential
source if required. Confirm that required controllers already exist or separately
approve their installation. A laptop connection alone does not supply GPUs.

First run read-only preflight checks and one bounded smoke job. Inspect actual
GPU identity/runtime output and capture permissions before scaling to the paired
campaign. Any required production changes, access expansion, installation,
purchases or new spend remain separate approvals. All live phases remain
**not run** until their captured evidence exists.

## Primary protocol references checked 2026-11-11

- [Kueue: Run a Kubernetes Job](https://kueue.sigs.k8s.io/docs/tasks/run/jobs/):
  LocalQueue label and suspended Job integration.
- [KAI Scheduler quickstart](https://github.com/kai-scheduler/KAI-Scheduler/blob/main/docs/quickstart/README.md):
  workload queue label and scheduler name.
- [GPUStack inference APIs](https://docs.gpustack.ai/2.3/integrations/inference-apis/):
  OpenAI-compatible `/v1` protocol. The reference is not evidence that any
  deployment here runs version 2.3.

These are upstream API-shape references, not runtime compatibility certification.
Select and record actual release versions when staging access is available.
