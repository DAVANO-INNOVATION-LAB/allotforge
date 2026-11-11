import copy
import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from benchmarks.adapters import make_job, import_job, elapsed_ms, PRODUCTS
from benchmarks.harness import (AGGREGATED_METRICS, METRICS, PARITY_FIELDS, NoRedirect,
                                aggregate_runs, compare, endpoint, inference, microbenchmark, stats, summarize)


def run_fixture(evidence="live-gpu", count=20):
    return {"schema_version": 1, "evidence_type": evidence, "product": {"name": "fixture-only", "version": "test"},
        "suite": "test-fixture/v1", "sampling_unit": "independent-run-block",
        "started_at": "2026-11-11T12:00:00Z", "finished_at": "2026-11-11T12:02:00Z",
        "parity": {category: {key: "test-value" for key in fields} for category, fields in PARITY_FIELDS.items()},
        "provenance": {"source_revision": "test-only", "artifact_sha256": "a" * 64, "capture_method": "test-only", "hardware_evidence_sha256": "b" * 64},
        "metrics": {"submit_to_started_ms": METRICS["submit_to_started_ms"]},
        "samples": [{"sample_id": str(i), "status": "succeeded", "metrics": {"submit_to_started_ms": 10.0}} for i in range(count)]}


class BenchmarkStatsTests(unittest.TestCase):
    def test_unknown_not_zero_and_nearest_rank(self):
        self.assertIsNone(stats([])["mean"])
        self.assertEqual(stats([1, 2, 3, 4])["p50"], 2)
        self.assertEqual(stats([1, 2, 3, 4])["p95"], 4)
        for bad in (float("nan"), float("inf"), -1, True):
            with self.assertRaises(ValueError): stats([bad])

    def test_valid_paired_comparison_has_deterministic_interval(self):
        a, b = run_fixture(), run_fixture()
        for sample in b["samples"]: sample["metrics"]["submit_to_started_ms"] = 8
        result = compare(a, b)
        self.assertTrue(result["comparison_allowed"])
        delta = result["deltas"]["submit_to_started_ms"]
        self.assertEqual(delta["relative_percent"], -20)
        self.assertEqual(delta["paired_mean_delta_bootstrap_95pct"], [-2, -2])

    def test_rejects_parity_changes_unknown_provenance_and_non_live(self):
        a = run_fixture()
        variants = []
        b = copy.deepcopy(a); b["parity"]["hardware"]["gpu_model"] = "different"; variants.append(b)
        b = copy.deepcopy(a); b["parity"]["runtime"]["driver"] = None; variants.append(b)
        b = copy.deepcopy(a); b["provenance"] = {}; variants.append(b)
        b = copy.deepcopy(a); b["evidence_type"] = "synthetic"; variants.append(b)
        b = copy.deepcopy(a); b["sampling_unit"] = "request"; variants.append(b)
        b = copy.deepcopy(a); b["samples"] = b["samples"][:19]; variants.append(b)
        b = copy.deepcopy(a); b["samples"][0]["status"] = "timeout"; variants.append(b)
        for b in variants:
            with self.subTest(b=b):
                r = compare(a, b); self.assertFalse(r["comparison_allowed"]); self.assertEqual(r["deltas"], {})

    def test_missing_metrics_never_imply_improvement(self):
        a, b = run_fixture(), run_fixture()
        b["samples"][0]["metrics"]["submit_to_started_ms"] = None
        r = compare(a, b)
        self.assertFalse(r["deltas"]["submit_to_started_ms"]["comparable"])
        self.assertEqual(r["candidate_summary"]["successful_sample_metrics"]["submit_to_started_ms"]["n"], 19)

    def test_duplicate_and_changed_semantics_rejected(self):
        a = run_fixture(); a["samples"][1]["sample_id"] = "0"
        with self.assertRaises(ValueError): summarize(a)
        a = run_fixture(); a["metrics"] = {"submit_to_started_ms": {"unit": "seconds"}}
        with self.assertRaises(ValueError): summarize(a)

    def test_aggregate_preserves_failures_and_rejects_duplicate_blocks(self):
        a = run_fixture(count=2); a["sampling_unit"] = "request"; a["block_id"] = "a"
        b = copy.deepcopy(a); b["block_id"] = "b"; b["samples"][0]["status"] = "failed"
        result = aggregate_runs([a, b])
        self.assertEqual(result["sampling_unit"], "independent-run-block")
        self.assertEqual(result["samples"][1]["status"], "failed")
        self.assertIsNone(result["samples"][1]["metrics"]["submit_to_started_ms"])
        self.assertEqual(result["metrics"]["submit_to_started_ms"], AGGREGATED_METRICS["submit_to_started_ms"])
        with self.assertRaises(ValueError): aggregate_runs([a, a])

    def test_micro_explicitly_non_gpu(self):
        report = microbenchmark(2, 1)
        self.assertEqual(report["evidence_type"], "local-cpu")
        self.assertIsNone(report["comparative_gpu_claim"])
        self.assertTrue(report["input"]["synthetic"])
        self.assertEqual(report["results"]["normalize_fixture"][0]["summary_ms"]["n"], 2)


class ManifestAdapterTests(unittest.TestCase):
    def args(self):
        return dict(namespace="gpu-bench", queue="benchmark", image="approved.invalid/pytorch@sha256:" + "a" * 64,
                    node_selector={"bench.example.com/node": "fixed"}, sample_id="block-001")

    def test_same_gpu_payload_across_batch_products(self):
        jobs = [make_job(product, **self.args()) for product in PRODUCTS]
        containers = [j["spec"]["template"]["spec"]["containers"] for j in jobs]
        self.assertEqual(containers[0], containers[1]); self.assertEqual(containers[0], containers[2])
        self.assertTrue(jobs[0]["spec"]["suspend"])
        self.assertTrue(jobs[1]["spec"]["suspend"])
        self.assertFalse(jobs[2]["spec"]["suspend"])
        self.assertEqual(jobs[2]["spec"]["template"]["spec"]["schedulerName"], "kai-scheduler")
        self.assertTrue(all(j["spec"]["backoffLimit"] == 0 for j in jobs))
        self.assertTrue(all(j["spec"]["template"]["spec"]["automountServiceAccountToken"] is False for j in jobs))

    def test_unsafe_target_and_mutable_images_rejected(self):
        for key, value in (("namespace", "default"), ("namespace", "kai-scheduler"), ("image", "pytorch:latest"), ("node_selector", {})):
            args = self.args(); args[key] = value
            with self.assertRaises(ValueError): make_job("kueue", **args)
        with self.assertRaises(ValueError): make_job("gpustack", **self.args())

    def evidence(self):
        job = {"kind": "Job", "metadata": {"name": "bench", "namespace": "gpu-bench", "uid": "actual-job", "creationTimestamp": "2026-11-11T00:00:00Z"}}
        pod = {"metadata": {"namespace": "gpu-bench", "ownerReferences": [{"uid": "actual-job", "kind": "Job", "controller": True}]},
               "status": {"phase": "Succeeded", "conditions": [{"type": "PodScheduled", "status": "True", "lastTransitionTime": "2026-11-11T00:00:02Z"}],
                   "containerStatuses": [{"name": "benchmark", "restartCount": 0, "state": {"terminated": {"exitCode": 0, "startedAt": "2026-11-11T00:00:03Z", "finishedAt": "2026-11-11T00:00:10Z"}}}]}}
        return job, pod

    def test_export_measures_distinct_intervals(self):
        job, pod = self.evidence(); result = import_job(job, [pod])
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["metrics"], {"submit_to_scheduled_ms": 2000, "submit_to_started_ms": 3000, "submit_to_finished_ms": 10000, "container_runtime_ms": 7000})

    def test_owner_uid_missing_attempts_retries_and_clock_skew(self):
        job, pod = self.evidence(); wrong = copy.deepcopy(pod); wrong["metadata"]["ownerReferences"][0]["uid"] = "other"
        self.assertEqual(import_job(job, [wrong])["status"], "incomplete")
        self.assertTrue(all(x is None for x in import_job(job, [])["metrics"].values()))
        with self.assertRaises(ValueError): import_job(job, [pod, pod])
        pod["status"]["containerStatuses"][0]["restartCount"] = 1
        with self.assertRaises(ValueError): import_job(job, [pod])
        with self.assertRaises(ValueError): elapsed_ms("2026-11-11T00:01:00Z", "2026-11-11T00:00:00Z")
        with self.assertRaises(ValueError): elapsed_ms("2026-11-11T00:00:00", "2026-11-11T00:00:01Z")


class InferenceAdapterTests(unittest.TestCase):
    def test_endpoint_transport_boundary(self):
        for url in ("http://example.com/v1", "https://a:b@example.com/v1", "https://example.com/v1?key=x", "https://example.com/else"):
            with self.assertRaises(ValueError): endpoint(url)
        self.assertEqual(endpoint("https://example.com/v1/"), "https://example.com/v1/chat/completions")
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere.invalid"))

    def test_mock_http_is_labeled_not_gpu_and_never_ttft(self):
        # The local fixture only validates client accounting and the protocol.
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                raw = json.dumps({"choices": [{"message": {"content": "fixture"}}], "usage": {"completion_tokens": 3}}).encode()
                self.send_response(200); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw)
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True); worker.start()
        base = run_fixture()
        config = {"requests": 4, "concurrency": 2, "block_id": "test-block", "model": "fixture-only", "product": {"name": "GPUStack", "version": "mock-not-gpustack"}, "parity": base["parity"], "provenance": base["provenance"]}
        try:
            result = inference(config, "http://127.0.0.1:%d/v1" % server.server_port)
        finally:
            server.shutdown(); server.server_close(); worker.join()
        self.assertEqual(result["summary"]["succeeded"], 4)
        self.assertFalse(result["gpu_execution_verified"])
        self.assertIsNone(result["ttft_ms"])
        self.assertEqual(result["sampling_unit"], "request")
        self.assertGreater(result["reported_output_tokens_per_second"], 0)
        self.assertNotIn("fixture", json.dumps(result["samples"]))
        self.assertFalse(compare(result, result)["comparison_allowed"])


if __name__ == "__main__":
    unittest.main()
