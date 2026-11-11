"""Run with `python -m benchmarks.harness --help` from the source checkout.

Local CPU tests, offline workload/export adapters, optional explicit inference
endpoint tests, and strict evidence comparisons. No installation or deployment.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import platform
import random
import shutil
import ssl
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPSHandler, HTTPRedirectHandler, Request, build_opener

from .adapters import PRODUCTS, digest, import_job, make_job, timestamp

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE_TYPES = {"local-cpu", "live-http", "live-gpu", "replay", "synthetic"}
PARITY_FIELDS = {
    "hardware": {"gpu_model", "gpu_count", "gpu_memory_bytes", "node_set_sha256", "mig_mode", "sharing_mode", "power_limit_w", "cpu_model", "host_memory_bytes", "network_topology"},
    "runtime": {"image_digest", "driver", "cuda", "engine", "kubernetes"},
    "workload": {"workload_sha256", "precision", "batch_size"},
    "load": {"concurrency", "arrival_pattern", "warmup_policy", "cache_policy"},
    "policy": {"quota_gpus", "priority", "preemption", "fair_share", "security_controls"},
}
METRICS = {
    "submit_to_scheduled_ms": {"unit": "ms", "direction": "lower", "definition": "Job creationTimestamp to PodScheduled=True transition; includes admission and controller time"},
    "submit_to_started_ms": {"unit": "ms", "direction": "lower", "definition": "Job creationTimestamp to benchmark container startedAt; includes image/runtime startup"},
    "submit_to_finished_ms": {"unit": "ms", "direction": "lower", "definition": "Job creationTimestamp to benchmark container finishedAt"},
    "container_runtime_ms": {"unit": "ms", "direction": "lower", "definition": "benchmark container startedAt to finishedAt; includes Python/CUDA initialization"},
    "request_latency_ms": {"unit": "ms", "direction": "lower", "definition": "client monotonic clock immediately before HTTP request until complete response body is read; non-streaming"},
    "output_tokens": {"unit": "tokens", "direction": "neutral", "definition": "server-reported usage.completion_tokens; absent means unknown"},
}


AGGREGATED_METRICS = {key: {**value, "definition": "Arithmetic mean within one independently repeated run block of: " + value["definition"]} for key, value in METRICS.items()}


def now():
    return datetime.now(timezone.utc).isoformat()


def load(path):
    return json.loads(Path(path).read_text(), parse_constant=lambda value: (_ for _ in ()).throw(ValueError("Non-finite JSON value")))


def write(path, data):
    text = json.dumps(data, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text)
    else:
        print(text, end="")


def quantile(values, p):
    """Nearest-rank quantiles. Never interpolate or present absent data as zero."""
    return sorted(values)[max(0, math.ceil(p * len(values)) - 1)] if values else None


def stats(values):
    if any(isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x) or x < 0 for x in values):
        raise ValueError("Measurements must be finite nonnegative numbers")
    return {"n": len(values), "mean": statistics.mean(values) if values else None,
            "p50": quantile(values, .5), "p95": quantile(values, .95), "p99": quantile(values, .99),
            "min": min(values) if values else None, "max": max(values) if values else None}


def environment_report():
    commands = {name: shutil.which(name) is not None for name in ("nvidia-smi", "kubectl", "docker", "kind", "helm")}
    gpu_devices = bool(list(Path("/dev").glob("nvidia[0-9]*")))
    return {"schema_version": 1, "checked_at": now(), "environment": platform.platform(),
            "python": platform.python_version(), "cpu_count": os.cpu_count(), "commands_present": commands,
            "nvidia_device_nodes_present": gpu_devices,
            "live_gpu_benchmark_ready": False,
            "blockers": (["No NVIDIA device nodes visible"] if not gpu_devices else []) +
                (["kubectl is not installed"] if not commands["kubectl"] else []) +
                ["No authorized target cluster/namespace or configured competitor endpoints supplied",
                 "Hardware, queue policies, approved image digest and runtime versions not attested"],
            "scope": "Local presence checks only; no kubeconfig, credentials, API requests, purchases or installation"}


def microbenchmark(iterations=200, repeats=5):
    from davano import __version__
    from davano.inventory import DemoInventory, normalize
    from davano.models import EvaluationRequest
    from davano.service import FleetService
    from davano.store import Store
    if not 1 <= iterations <= 100000 or not 1 <= repeats <= 100:
        raise ValueError("iterations/repeats outside safe bounds")
    fixture_path = ROOT / "fixtures/cluster.json"
    fixture = load(fixture_path)
    inventory = DemoInventory(fixture_path)
    store = Store(":memory:", seed=fixture["configuration"], source="demo")
    service = FleetService(inventory, store)
    req = EvaluationRequest(pool_id="training", namespace="research", gpu_count=4, vendor="NVIDIA")
    started = now()
    operations = {"normalize_fixture": lambda: normalize(fixture["nodes"], fixture["pods"]),
                  "state_in_memory": service.state, "evaluate_in_memory": lambda: service.evaluate(req)}
    results = {}
    try:
        for name, operation in operations.items():
            for _ in range(20):
                operation()
            runs = []
            for repetition in range(repeats):
                samples = []
                t0 = time.perf_counter_ns()
                for _ in range(iterations):
                    t = time.perf_counter_ns()
                    operation()
                    samples.append((time.perf_counter_ns() - t) / 1e6)
                wall_s = (time.perf_counter_ns() - t0) / 1e9
                runs.append({"repetition": repetition, "raw_ms": samples, "summary_ms": stats(samples),
                             "wall_seconds": wall_s, "operations_per_second": iterations / wall_s})
            results[name] = runs
    finally:
        store.close()
    source_files = sorted((ROOT / "davano").glob("*.py"))
    return {"schema_version": 1, "evidence_type": "local-cpu", "product": {"name": "Davano", "version": __version__},
            "suite": "synthetic-inventory-in-process/v1", "started_at": started, "finished_at": now(),
            "fixture_sha256": hashlib.sha256(fixture_path.read_bytes()).hexdigest(),
            "source_sha256": digest({str(f.relative_to(ROOT)): hashlib.sha256(f.read_bytes()).hexdigest() for f in source_files}),
            "input": {"nodes": len(fixture["nodes"]), "pods": len(fixture["pods"]), "synthetic": True},
            "environment": environment_report(), "warmup_iterations_per_operation": 20,
            "results": results, "comparative_gpu_claim": None,
            "limitations": ["Real CPU wall-clock measurements over synthetic inventory, in-process and single-threaded",
                            "No HTTP/network/cluster/queue/GPU execution measured; no competitor was run",
                            "Only repeatability smoke data, not a production capacity forecast",
                            "Warm in-memory SQLite; do not compare with remote or persistent database results"]}


def validate_run(run):
    if run.get("schema_version") != 1 or run.get("evidence_type") not in EVIDENCE_TYPES:
        raise ValueError("Unrecognized evidence schema/type")
    if not isinstance(run.get("product"), dict) or not all(isinstance(run["product"].get(k), str) and run["product"][k] for k in ("name", "version")):
        raise ValueError("Product name and exact version are required")
    if not isinstance(run.get("suite"), str) or not run["suite"]:
        raise ValueError("A named suite with version is required")
    if timestamp(run["finished_at"]) < timestamp(run["started_at"]):
        raise ValueError("Run end precedes start")
    if not isinstance(run.get("parity"), dict) or set(run["parity"]) != {"hardware", "runtime", "workload", "load", "policy"}:
        raise ValueError("Parity must contain hardware, runtime, workload, load and policy")
    for section, fields in PARITY_FIELDS.items():
        if not isinstance(run["parity"][section], dict) or not fields.issubset(run["parity"][section]):
            raise ValueError("Missing parity fields in " + section + ": " + ", ".join(sorted(fields)))
    if run.get("sampling_unit") not in ("request", "independent-run-block"):
        raise ValueError("Explicit sampling_unit required")
    if not isinstance(run.get("provenance"), dict):
        raise ValueError("Provenance is required")
    if not isinstance(run.get("metrics"), dict) or not run["metrics"]:
        raise ValueError("Explicit metric definitions are required")
    for key, definition in run["metrics"].items():
        if key not in METRICS or definition not in (METRICS[key], AGGREGATED_METRICS[key]):
            raise ValueError("Unknown or changed metric semantics: " + key)
    if not isinstance(run.get("samples"), list) or not run["samples"]:
        raise ValueError("All attempted samples are required")
    ids = set()
    for sample in run["samples"]:
        sid = sample.get("sample_id")
        if not isinstance(sid, str) or not sid or sid in ids:
            raise ValueError("Unique nonempty sample IDs required")
        ids.add(sid)
        if sample.get("status") not in ("succeeded", "failed", "timeout", "rejected", "incomplete"):
            raise ValueError("Unknown sample status")
        if not isinstance(sample.get("metrics"), dict) or set(sample["metrics"]) != set(run["metrics"]):
            raise ValueError("Sample metric keys must match definitions; use null for missing")
        stats([v for v in sample["metrics"].values() if v is not None])
    return run


def summarize(run):
    validate_run(run)
    successes = [s for s in run["samples"] if s["status"] == "succeeded"]
    return {"attempted": len(run["samples"]), "succeeded": len(successes),
            "failure_or_incomplete_rate": 1 - len(successes) / len(run["samples"]),
            "status_counts": {k: sum(s["status"] == k for s in run["samples"]) for k in ("succeeded", "failed", "timeout", "rejected", "incomplete")},
            "successful_sample_metrics": {key: stats([s["metrics"][key] for s in successes if s["metrics"][key] is not None]) for key in run["metrics"]}}


def unknown(value):
    if isinstance(value, dict):
        return not value or any(unknown(v) for v in value.values())
    if isinstance(value, list):
        return not value or any(unknown(v) for v in value)
    return value is None or (isinstance(value, str) and value.strip().lower() in ("", "unknown", "unverified", "todo", "replace-me"))


def aggregate_runs(runs):
    """Aggregate independent HTTP runs; never treat concurrent requests as runs."""
    if not runs:
        raise ValueError("At least one captured run required")
    first = validate_run(runs[0])
    if first["sampling_unit"] != "request":
        raise ValueError("Only request-level runs can be aggregated")
    samples, seen = [], set()
    for run in runs:
        validate_run(run)
        for key in ("product", "suite", "evidence_type", "parity", "metrics", "sampling_unit"):
            if run[key] != first[key]:
                raise ValueError("Run configuration changed: " + key)
        block_id = run.get("block_id")
        if not isinstance(block_id, str) or not block_id or block_id in seen:
            raise ValueError("Each run requires a unique preassigned block_id")
        seen.add(block_id)
        summary = summarize(run)
        all_success = summary["succeeded"] == summary["attempted"]
        samples.append({"sample_id": block_id, "status": "succeeded" if all_success else "failed",
            "metrics": {key: statistics.mean(s["metrics"][key] for s in run["samples"])
                if all_success and all(s["metrics"][key] is not None for s in run["samples"]) else None
                for key in run["metrics"]}, "source_summary": summary,
            "source_run_sha256": digest(run)})
    artifact_hash = digest([digest(run) for run in runs])
    return {"schema_version": 1, "product": first["product"], "suite": first["suite"],
        "evidence_type": first["evidence_type"], "parity": first["parity"],
        "sampling_unit": "independent-run-block", "samples": samples,
        "metrics": {key: AGGREGATED_METRICS[key] for key in first["metrics"]},
        "started_at": min((r["started_at"] for r in runs), key=timestamp),
        "finished_at": max((r["finished_at"] for r in runs), key=timestamp),
        "provenance": {**first["provenance"], "artifact_sha256": artifact_hash,
                       "capture_method": "independent HTTP run means with preserved source summaries"}}


def compare(baseline, candidate):
    """Paired candidate-minus-baseline effects; no leaderboard or causal claim."""
    bsum, csum = summarize(baseline), summarize(candidate)
    blockers = []
    for key in ("suite", "evidence_type", "metrics", "parity", "sampling_unit"):
        if baseline[key] != candidate[key]:
            blockers.append("Mismatch in " + key)
    for label, run in (("baseline", baseline), ("candidate", candidate)):
        if unknown(run["product"]["version"]):
            blockers.append(label + " has no exact product version")
        if run["sampling_unit"] != "independent-run-block":
            blockers.append(label + " samples are not independent run blocks; aggregate repeated runs first")
        if unknown(run["parity"]):
            blockers.append(label + " parity contains missing/unknown fields")
        provenance = run["provenance"]
        for key in ("source_revision", "artifact_sha256", "capture_method", "hardware_evidence_sha256"):
            if not provenance.get(key) or unknown(provenance[key]):
                blockers.append(label + " is missing provenance." + key)
        if run["evidence_type"] not in ("live-gpu", "live-http"):
            blockers.append(label + " is not live performance evidence")
    bm = {s["sample_id"]: s for s in baseline["samples"]}
    cm = {s["sample_id"]: s for s in candidate["samples"]}
    if set(bm) != set(cm):
        blockers.append("Attempted sample IDs differ; no dropping failed/slow samples")
    if bsum["failure_or_incomplete_rate"] or csum["failure_or_incomplete_rate"]:
        blockers.append("Failures/incomplete samples require reliability analysis before speed ranking")
    if len(bm) < 20 or len(cm) < 20:
        blockers.append("At least 20 independently paired run blocks are required; inner GPU iterations are not independent runs")
    result = {"schema_version": 1, "created_at": now(), "comparison_allowed": not blockers,
              "baseline": baseline["product"], "candidate": candidate["product"],
              "baseline_summary": bsum, "candidate_summary": csum, "blockers": blockers, "deltas": {},
              "scope": "Comparison of supplied evidence and declarations; not independent hardware attestation",
              "limitations": ["No causal attribution to scheduler/product from an end-to-end delta alone",
                              "live-http does not verify GPU execution; server/runtime evidence must be reviewed",
                              "Intervals describe repeated block means, not guarantees or multiple-comparison correction"]}
    if blockers:
        return result
    for key in baseline["metrics"]:
        pairs = [(bm[s]["metrics"][key], cm[s]["metrics"][key]) for s in sorted(bm)]
        if any(a is None or b is None for a, b in pairs):
            result["deltas"][key] = {"comparable": False, "reason": "Missing paired measurements; null is not zero"}
            continue
        deltas = [b - a for a, b in pairs]
        rng = random.Random(20261111)
        bootstrap = [statistics.mean(rng.choices(deltas, k=len(deltas))) for _ in range(2000)]
        bmean, cmean = statistics.mean(a for a, _ in pairs), statistics.mean(b for _, b in pairs)
        result["deltas"][key] = {"comparable": True, "paired_blocks": len(pairs), "baseline_mean": bmean,
            "candidate_mean": cmean, "candidate_minus_baseline": statistics.mean(deltas),
            "relative_percent": 100 * (cmean - bmean) / bmean if bmean else None,
            "paired_mean_delta_bootstrap_95pct": [quantile(bootstrap, .025), quantile(bootstrap, .975)],
            "direction": baseline["metrics"][key]["direction"], "unit": baseline["metrics"][key]["unit"]}
    return result


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def endpoint(value):
    parsed = urlparse(value)
    if parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path.rstrip("/") != "/v1":
        raise ValueError("Use a /v1 API base URL without userinfo, query or fragment")
    if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("127.0.0.1", "::1", "localhost")):
        raise ValueError("HTTPS required except explicit loopback test endpoints")
    if not parsed.hostname:
        raise ValueError("Host required")
    return value.rstrip("/") + "/chat/completions"


def inference(config, base_url, key_file=None):
    """Bounded, non-streaming GPUStack/OpenAI protocol runner, no provisioning.

    Only run against an endpoint approved for its workload and possible costs.
    Fixed public synthetic prompt; no private content or response text saved.
    """
    url = endpoint(base_url)
    count, concurrency = config["requests"], config["concurrency"]
    if not isinstance(config.get("block_id"), str) or not config["block_id"]:
        raise ValueError("Preassigned block_id required for paired repeated runs")
    if type(count) is not int or not 1 <= count <= 1000 or type(concurrency) is not int or not 1 <= concurrency <= 64:
        raise ValueError("requests must be 1..1000 and concurrency 1..64")
    timeout = config.get("timeout_seconds", 60)
    max_tokens = config.get("max_tokens", 128)
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 1 <= timeout <= 300:
        raise ValueError("timeout_seconds must be 1..300")
    if type(max_tokens) is not int or not 1 <= max_tokens <= 1024:
        raise ValueError("max_tokens must be 1..1024")
    if config.get("product", {}).get("name") not in ("GPUStack", "vLLM-direct"):
        raise ValueError("This lane supports GPUStack or vLLM-direct; Davano is not an inference server")
    if not isinstance(config.get("model"), str) or not config["model"]:
        raise ValueError("Explicit deployed model required")
    # Validate metadata before sending any request.
    trial = {"schema_version": 1, "evidence_type": "live-http", "suite": "openai-nonstream-chat/v1",
        "product": config["product"], "parity": config["parity"], "provenance": config["provenance"], "sampling_unit": "request",
        "started_at": now(), "finished_at": now(), "metrics": {k: METRICS[k] for k in ("request_latency_ms", "output_tokens")},
        "samples": [{"sample_id": "validate-only", "status": "incomplete", "metrics": {"request_latency_ms": None, "output_tokens": None}}]}
    validate_run(trial)
    key = Path(key_file).read_text().strip() if key_file else None
    headers = {"Content-Type": "application/json"}
    if key:
        headers["Authorization"] = "Bearer " + key
    body = {"model": config["model"], "messages": [{"role": "user", "content": "Explain matrix multiplication using three short sentences."}],
            "temperature": 0, "max_tokens": max_tokens, "stream": False}
    payload = json.dumps(body).encode()
    def one(index):
        sample = {"sample_id": str(index), "status": "failed", "metrics": {"request_latency_ms": None, "output_tokens": None}}
        opener = build_opener(HTTPSHandler(context=ssl.create_default_context()), NoRedirect())
        t0 = time.perf_counter_ns()
        try:
            with opener.open(Request(url, data=payload, headers=headers, method="POST"), timeout=timeout) as response:
                raw = response.read(1024 * 1024 + 1)
                elapsed = (time.perf_counter_ns() - t0) / 1e6
                if len(raw) > 1024 * 1024:
                    raise ValueError("Response too large")
                obj = json.loads(raw)
                if not obj.get("choices") or "message" not in obj["choices"][0]:
                    raise ValueError("Unexpected completion schema")
                tokens = obj.get("usage", {}).get("completion_tokens")
                if tokens is not None and (type(tokens) is not int or tokens < 0):
                    raise ValueError("Invalid token count")
                sample.update(status="succeeded", metrics={"request_latency_ms": elapsed, "output_tokens": tokens})
        except HTTPError as exc:
            sample["error"] = "HTTP " + str(exc.code)
        except TimeoutError:
            sample.update(status="timeout", error="Request timeout")
        except (URLError, OSError, ValueError, TypeError, KeyError, IndexError):
            sample["error"] = "Transport or response validation failure; details omitted to protect credentials"
        return sample
    # Warm-up failures abort so unavailable endpoints cannot look fast.
    for i in range(3):
        if one("warmup-" + str(i))["status"] != "succeeded":
            raise ValueError("Inference warm-up failed; no measured run recorded")
    started = now()
    t0 = time.perf_counter_ns()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        samples = list(pool.map(one, range(count)))
    elapsed = (time.perf_counter_ns() - t0) / 1e9
    trial.update(started_at=started, finished_at=now(), samples=samples, block_id=config["block_id"])
    trial["provenance"] = {**trial["provenance"], "artifact_sha256": digest(samples),
                           "capture_method": "bounded urllib non-streaming HTTP client, monotonic response completion timing"}
    trial["actual_request_sha256"] = digest(body)
    trial["actual_load"] = {"requests": count, "concurrency": concurrency, "warmup_requests": 3,
                            "timeout_seconds": timeout, "pattern": "closed-loop thread pool"}
    # Derived facts are part of parity; caller cannot declare them differently.
    trial["parity"] = dict(trial["parity"], workload={**trial["parity"]["workload"], "request_sha256": digest(body)},
                           load={**trial["parity"]["load"], **trial["actual_load"], "arrival_pattern": "closed-loop thread pool", "warmup_policy": "3 sequential requests"})
    succeeded = [s for s in samples if s["status"] == "succeeded"]
    usage_complete = len(succeeded) == count and all(s["metrics"]["output_tokens"] is not None for s in succeeded)
    trial["wall_seconds"] = elapsed
    trial["successful_requests_per_second"] = len(succeeded) / elapsed
    trial["reported_output_tokens_per_second"] = sum(s["metrics"]["output_tokens"] for s in succeeded) / elapsed if usage_complete else None
    trial["ttft_ms"] = None
    trial["gpu_execution_verified"] = False
    trial["summary"] = summarize(trial)
    trial["limitations"] = ["Non-streaming HTTP latency is not TTFT; token counts are server-reported",
        "Endpoint identity and hardware are operator declarations, not independently attested",
        "Closed-loop throughput includes queueing/network; no server-only attribution",
        "Repeated fixed prompt may hit caches; match and record cache settings across products",
        "A single request set is one run block; do not treat concurrent requests as independent repeated experiments"]
    return trial


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("environment"); p.add_argument("--output")
    p = sub.add_parser("micro"); p.add_argument("--iterations", type=int, default=200); p.add_argument("--repeats", type=int, default=5); p.add_argument("--output")
    p = sub.add_parser("plan"); p.add_argument("--config", required=True); p.add_argument("--product", choices=PRODUCTS, required=True); p.add_argument("--output")
    p = sub.add_parser("import-job"); p.add_argument("--job", required=True); p.add_argument("--pods", required=True); p.add_argument("--output")
    p = sub.add_parser("verify-smoke", help="Check local GPU smoke artifacts without execution or authentication"); p.add_argument("--job", required=True); p.add_argument("--pods", required=True); p.add_argument("--stdout", required=True); p.add_argument("--receipt", required=True); p.add_argument("--output")
    p = sub.add_parser("aggregate"); p.add_argument("--runs", nargs="+", required=True); p.add_argument("--output")
    p = sub.add_parser("compare"); p.add_argument("--baseline", required=True); p.add_argument("--candidate", required=True); p.add_argument("--output")
    p = sub.add_parser("inference"); p.add_argument("--config", required=True); p.add_argument("--base-url", required=True); p.add_argument("--key-file"); p.add_argument("--output")
    args = parser.parse_args()
    try:
        if args.command == "environment": result = environment_report()
        elif args.command == "micro": result = microbenchmark(args.iterations, args.repeats)
        elif args.command == "plan": result = make_job(args.product, **load(args.config))
        elif args.command == "import-job": result = import_job(load(args.job), load(args.pods)["items"])
        elif args.command == "verify-smoke":
            from .evidence import read_artifact, verify_smoke_bundle, write_report
            result = verify_smoke_bundle(*(read_artifact(getattr(args, name), name) for name in ("job", "pods", "stdout", "receipt")))
        elif args.command == "aggregate": result = aggregate_runs([load(p) for p in args.runs])
        elif args.command == "compare": result = compare(load(args.baseline), load(args.candidate))
        else: result = inference(load(args.config), args.base_url, args.key_file)
        if args.command == "verify-smoke" and args.output:
            write_report(args.output, result)
        else:
            write(args.output, result)
        if args.command == "verify-smoke" and result["status"] != "internally-consistent":
            raise SystemExit(1)
    except (ValueError, TypeError, KeyError, OSError) as exc:
        parser.error("verify-smoke: artifact I/O failed" if args.command == "verify-smoke" and isinstance(exc, OSError) else str(exc))


if __name__ == "__main__":
    main()
