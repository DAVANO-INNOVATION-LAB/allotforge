"""Synthetic offline fixtures only. These tests do not exercise a real GPU."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from benchmarks.adapters import make_job
from benchmarks.evidence import (EXPECTED_PROGRAM_SHA256, LIMITS, MAX_DEPTH,
                                 read_artifact, verify_smoke_bundle)
from benchmarks.harness import main, validate_run


def fixture():
    job = make_job("kueue", namespace="gpu-bench", queue="smoke",
                   image="registry.example/pytorch@sha256:" + "a" * 64,
                   node_selector={"bench.example/node": "fixed"}, sample_id="sample-001")
    job["metadata"].update(uid="job-uid", creationTimestamp="2026-11-11T00:00:00Z")
    job["spec"]["suspend"] = False
    job["status"] = {"succeeded": 1, "completionTime": "2026-11-11T00:00:11Z",
                     "conditions": [{"type": "Complete", "status": "True", "lastTransitionTime": "2026-11-11T00:00:11Z"}]}
    pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {
        "name": "bench-sample-001-abc", "namespace": "gpu-bench", "uid": "pod-uid",
        "labels": copy.deepcopy(job["spec"]["template"]["metadata"]["labels"]),
        "ownerReferences": [{"apiVersion": "batch/v1", "kind": "Job", "name": job["metadata"]["name"], "uid": "job-uid", "controller": True}]},
        "spec": copy.deepcopy(job["spec"]["template"]["spec"]),
        "status": {"phase": "Succeeded", "containerStatuses": [{"name": "benchmark", "restartCount": 0,
            "containerID": "containerd://" + "c" * 64, "imageID": "sha256:" + "b" * 64,
            "state": {"terminated": {"exitCode": 0, "startedAt": "2026-11-11T00:00:02Z", "finishedAt": "2026-11-11T00:00:10Z"}}}]}}
    pods = {"apiVersion": "v1", "kind": "PodList", "metadata": {}, "items": [pod]}
    payload = {"schema_version": 1, "evidence_type": "live-gpu-payload", "suite": "cuda-fp16-matmul-4096/v1",
               "started_at": "2026-11-11T00:00:04.000000+00:00", "finished_at": "2026-11-11T00:00:06.000000+00:00",
               "sample_id": "sample-001", "gpu_name": "Fixture GPU (not hardware evidence)", "gpu_memory_bytes": 1024,
               "torch_version": "fixture", "cuda_runtime": "fixture", "visible_gpu_count": 1, "output_finite": True,
               "warmup_iterations": 10, "iterations": 100, "step_ms": [10.0] * 100, "mean_step_ms": 10.0,
               "scope": "CUDA execution plus per-step host synchronization; not scheduler latency"}
    receipt = {"schema_version": 1, "namespace": "gpu-bench", "jobUID": "job-uid", "podUID": "pod-uid",
               "containerName": "benchmark", "containerID": pod["status"]["containerStatuses"][0]["containerID"],
               "sampleID": "sample-001", "imageID": pod["status"]["containerStatuses"][0]["imageID"],
               "stdoutSHA256": "", "capturedAt": "2026-11-11T00:00:12Z"}
    return job, pods, payload, receipt


def encode_bundle(job, pods, payload, receipt):
    stdout = (json.dumps(payload, allow_nan=False) + "\n").encode() if isinstance(payload, dict) else payload
    receipt = copy.deepcopy(receipt)
    if "stdoutSHA256" in receipt and receipt["stdoutSHA256"] == "":
        receipt["stdoutSHA256"] = hashlib.sha256(stdout).hexdigest()
    return [json.dumps(job).encode(), json.dumps(pods).encode(), stdout, json.dumps(receipt).encode()]


class SmokeEvidenceTests(unittest.TestCase):
    def verify(self, bundle=None):
        return verify_smoke_bundle(*encode_bundle(*(bundle or fixture())))

    def check_status(self, bundle, status="rejected"):
        result = self.verify(bundle)
        self.assertEqual(result["status"], status, result)
        self.assertNotIn("payload", result)
        self.assertNotIn("identity", result)
        self.assertFalse(result["gpu_execution_verified"])
        self.assertFalse(result["hardware_verified"])
        self.assertIsNone(result["comparative_claim"])
        return result

    def test_consistent_is_not_authenticated_or_live_gpu(self):
        bundle = fixture()
        raw = encode_bundle(*bundle)
        result = verify_smoke_bundle(*raw)
        self.assertEqual(result["status"], "internally-consistent")
        self.assertEqual(result["evidence_scope"], "offline-consistency-only")
        self.assertFalse(result["gpu_execution_verified"])
        self.assertFalse(result["hardware_verified"])
        self.assertEqual(result["whole_gpu_physical_mode"], "UNVERIFIED")
        self.assertIsNone(result["comparative_claim"])
        self.assertEqual(result["payload"]["step_ms"], [10.0] * 100)
        self.assertEqual(result["payload"]["total_step_ms"], 1000.0)
        self.assertEqual(result["provenance"]["expected_payload_sha256"], EXPECTED_PROGRAM_SHA256)
        self.assertEqual(result["artifact_sha256"], {name: hashlib.sha256(data).hexdigest() for name, data in zip(("job", "pods", "stdout", "receipt"), raw)})
        self.assertEqual(result["identity"]["observed_image_id"], "sha256:" + "b" * 64)
        self.assertNotEqual(result["identity"]["requested_image"].split("@")[-1], result["identity"]["observed_image_id"])
        with self.assertRaises(ValueError):
            validate_run(result)
        self.assertEqual(verify_smoke_bundle(*(data.decode() for data in raw)), result)

    def test_missing_evidence_is_incomplete(self):
        modifications = [lambda j, ps, p, r: r.pop("containerID"),
            lambda j, ps, p, r: j["metadata"].pop("uid"),
            lambda j, ps, p, r: ps["items"].clear(),
            lambda j, ps, p, r: ps["items"][0]["status"]["containerStatuses"].clear(),
            lambda j, ps, p, r: ps["items"][0]["status"]["containerStatuses"][0]["state"].clear(),
            lambda j, ps, p, r: p.pop("step_ms"),
            lambda j, ps, p, r: j["status"].pop("completionTime"),
            lambda j, ps, p, r: j["status"]["conditions"].clear(),
            lambda j, ps, p, r: ps["metadata"].update({"continue": "more"})]
        for modify in modifications:
            with self.subTest(modify=modify):
                b = fixture(); modify(*b)
                self.check_status(b, "incomplete")
        j, ps, p, r = fixture()
        self.check_status((j, ps, b"", r), "incomplete")

    def test_rejects_receipt_identity_hash_and_extension_mismatches(self):
        for field in ("namespace", "jobUID", "podUID", "containerName", "containerID", "sampleID", "imageID", "stdoutSHA256"):
            with self.subTest(field=field):
                b = fixture(); b[3][field] = "d" * 64 if field == "stdoutSHA256" else "other"
                self.check_status(b)
        b = fixture(); b[3]["operatorNotes"] = "private untrusted content"
        result = self.check_status(b)
        self.assertNotIn("private", json.dumps(result))
        b = fixture(); b[3]["schema_version"] = True
        self.check_status(b)

    def test_owner_uid_not_name_and_all_attempts_are_checked(self):
        b = fixture(); b[1]["items"].append(copy.deepcopy(b[1]["items"][0])); self.check_status(b)
        b = fixture(); b[1]["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "other"
        self.check_status(b, "incomplete")
        for key, value in (("controller", False), ("controller", 1), ("kind", "ReplicaSet"), ("apiVersion", "v1"), ("name", "different")):
            b = fixture(); b[1]["items"][0]["metadata"]["ownerReferences"][0][key] = value
            self.check_status(b)
        b = fixture(); b[1]["items"][0]["metadata"]["namespace"] = "elsewhere"; self.check_status(b)
        b = fixture(); b[1]["items"][0]["metadata"]["ownerReferences"] *= 2; self.check_status(b)
        b = fixture(); other = copy.deepcopy(b[1]["items"][0]); other["metadata"]["uid"] = "unrelated"; other["metadata"]["name"] = "unrelated"; other["metadata"]["ownerReferences"] = []
        b[1]["items"].append(other)
        self.assertEqual(self.verify(b)["status"], "internally-consistent")

    def test_container_ambiguity_restarts_and_unexpected_states(self):
        mutations = [lambda p: p["spec"]["containers"].append(copy.deepcopy(p["spec"]["containers"][0])),
            lambda p: p["spec"].update(initContainers=[{"name": "init"}]),
            lambda p: p["spec"].update(ephemeralContainers=[{"name": "debug"}]),
            lambda p: p["status"].update(initContainerStatuses=[{}]),
            lambda p: p["status"].update(ephemeralContainerStatuses=[{}]),
            lambda p: p["status"]["containerStatuses"].append(copy.deepcopy(p["status"]["containerStatuses"][0])),
            lambda p: p["status"]["containerStatuses"][0].update(restartCount=1),
            lambda p: p["status"]["containerStatuses"][0].update(restartCount=False),
            lambda p: p["status"]["containerStatuses"][0].update(lastState={"terminated": {}}),
            lambda p: p["status"]["containerStatuses"][0]["state"].update(running={}),
            lambda p: p["status"]["containerStatuses"][0]["state"]["terminated"].update(exitCode=True),
            lambda p: p["status"]["containerStatuses"][0]["state"]["terminated"].update(exitCode=1),
            lambda p: p["status"]["containerStatuses"][0]["state"]["terminated"].update(signal=9),
            lambda p: p["status"]["containerStatuses"][0]["state"]["terminated"].update(containerID="stale"),
            lambda p: p["status"].update(phase="Failed")]
        for modify in mutations:
            with self.subTest(modify=modify):
                b = fixture(); modify(b[1]["items"][0]); self.check_status(b)

    def test_job_completion_contradictions(self):
        variants = [lambda j: j["status"].update(failed=1), lambda j: j["status"].update(active=1),
            lambda j: j["status"].update(succeeded=True), lambda j: j["spec"].update(completions=2),
            lambda j: j["spec"].update(backoffLimit=1), lambda j: j["spec"].update(suspend=True),
            lambda j: j["status"]["conditions"].append({"type": "Failed", "status": "True"}),
            lambda j: j["status"]["conditions"].append({"type": "FailureTarget", "status": "True"}),
            lambda j: j["status"]["conditions"].append(copy.deepcopy(j["status"]["conditions"][0])),
            lambda j: j["status"].update(completionTime="2026-11-10T00:00:11Z")]
        for modify in variants:
            with self.subTest(modify=modify):
                b = fixture(); modify(b[0]); self.check_status(b)
        b = fixture(); b[0]["status"]["conditions"].append({"type": "Failed", "status": "False"})
        self.assertEqual(self.verify(b)["status"], "internally-consistent")

    def test_label_environment_image_command_program_and_resources(self):
        for on_job in (True, False):
            for mutate in (lambda s: s["containers"][0].update(image="registry.example/pytorch:mutable"),
                           lambda s: s["containers"][0].update(image="registry.example/pytorch@sha256:" + "b" * 64),
                           lambda s: s["containers"][0].update(command=["python", "-c", "print('fake')"]),
                           lambda s: s["containers"][0].update(args=["--different"]),
                           lambda s: s["containers"][0]["env"][0].update(value="other"),
                           lambda s: s["containers"][0]["env"].append({"name": "DAVANO_BENCH_SAMPLE", "value": "sample-001"}),
                           lambda s: s["containers"][0]["env"][0].update(valueFrom={"fieldRef": {}}),
                           lambda s: s["containers"][0].update(envFrom=[{"secretRef": {}}]),
                           lambda s: s["containers"][0]["resources"]["requests"].update({"nvidia.com/gpu": 2}),
                           lambda s: s["containers"][0]["resources"]["limits"].update({"nvidia.com/gpu": True}),
                           lambda s: s["containers"][0]["resources"]["limits"].update({"nvidia.com/mig-1g.5gb": "1"})):
                with self.subTest(on_job=on_job, mutate=mutate):
                    b = fixture(); s = b[0]["spec"]["template"]["spec"] if on_job else b[1]["items"][0]["spec"]
                    mutate(s); self.check_status(b)
        for metadata in ("job", "template", "pod"):
            b = fixture(); m = b[0]["metadata"] if metadata == "job" else b[0]["spec"]["template"]["metadata"] if metadata == "template" else b[1]["items"][0]["metadata"]
            m["labels"]["davano.dev/benchmark-sample"] = "other"
            self.check_status(b)
        b = fixture(); b[0]["metadata"]["annotations"]["davano.dev/payload-sha256"] = "f" * 64; self.check_status(b)
        b = fixture(); b[1]["items"][0]["metadata"]["annotations"] = {"davano.dev/payload-sha256": "f" * 64}; self.check_status(b)

    def test_exact_payload_schema_and_numeric_types(self):
        changes = [("schema_version", True), ("evidence_type", "live-gpu"), ("suite", "other"),
                   ("sample_id", "other"), ("visible_gpu_count", True), ("visible_gpu_count", 2),
                   ("output_finite", 1), ("output_finite", False), ("iterations", 99), ("warmup_iterations", 0),
                   ("gpu_memory_bytes", True), ("gpu_memory_bytes", 0), ("mean_step_ms", True),
                   ("mean_step_ms", 11), ("mean_step_ms", 10**400), ("step_ms", [10**400] * 100), ("step_ms", [10.0] * 99), ("step_ms", [10.0] * 101),
                   ("step_ms", [True] * 100), ("step_ms", [-1.0] * 100), ("step_ms", [0] * 100),
                   ("step_ms", ["10.0"] * 100), ("gpu_name", "bad\nname"), ("unexpected", "secret-value")]
        for key, value in changes:
            with self.subTest(key=key, value=value):
                b = fixture(); b[2][key] = value; self.check_status(b)
        b = fixture(); b[2]["mean_step_ms"] += 1e-10
        self.assertEqual(self.verify(b)["status"], "internally-consistent")

    def test_timezones_ordering_bounds_and_elapsed_duration(self):
        for key, value in (("started_at", "2026-11-11T00:00:04"), ("finished_at", "2026-11-11T00:00:03Z"),
                           ("started_at", "2026-11-10T00:00:04Z"), ("finished_at", "2026-11-11T00:00:13Z"),
                           ("finished_at", "2026-11-11T00:00:04.100000Z"), ("started_at", 0), ("started_at", "2026-11-11T00:00:04+00:99"), ("started_at", "2026-11-11T00:00:04+24:00")):
            b = fixture(); b[2][key] = value; self.check_status(b)
        b = fixture(); b[3]["capturedAt"] = "2026-11-11T00:00:07Z"; self.check_status(b)
        b = fixture(); b[3]["capturedAt"] = "2026-11-11T00:00:08Z"
        self.assertEqual(self.verify(b)["status"], "internally-consistent")
        b = fixture(); b[2]["started_at"] = "2026-11-11T01:00:04+01:00"
        self.assertEqual(self.verify(b)["status"], "internally-consistent")
        b = fixture(); b[2]["started_at"] = b[2]["finished_at"] = "2026-11-11T00:00:04Z"
        self.assertEqual(self.verify(b)["status"], "internally-consistent")  # 1-second endpoint bins
        b = fixture(); b[2]["started_at"] = b[2]["finished_at"] = "2026-11-11T00:00:04.000000Z"
        self.check_status(b)
        b = fixture(); b[1]["items"][0]["status"]["containerStatuses"][0]["state"]["terminated"]["finishedAt"] = "2026-11-11T00:00:01Z"
        self.check_status(b)

    def test_logs_are_not_cherry_picked_and_hashes_are_exact_bytes(self):
        j, ps, payload, r = fixture()
        raw = json.dumps(payload).encode()
        for bad in (raw + b"\n" + raw, b"warning\n" + raw, raw + b"\nwarning", b"\n" + raw,
                    json.dumps(payload, indent=2).encode()):
            self.check_status((j, ps, bad, r))
        for bad in (raw + b" {}", b'{} garbage', b'[]'):
            with self.assertRaises(ValueError):
                self.verify((j, ps, bad, r))
        r["stdoutSHA256"] = hashlib.sha256(raw).hexdigest()
        self.check_status((j, ps, raw + b"\n", r))

    def test_malformed_json_limits_duplicate_keys_and_error_redaction(self):
        raw = encode_bundle(*fixture())
        for index, artifact in enumerate(("job", "pods", "stdout", "receipt")):
            for bad in (b'{"password":"TOP-SECRET", "password":"OTHER"}', b'{"x":NaN}',
                        b'{"x":Infinity}', b'{"x":1e999}', b'{"secret":', b'\xff', b'[]',
                        b'{"nested":' + b'[' * MAX_DEPTH + b'0' + b']' * MAX_DEPTH + b'}',
                        b' ' * (LIMITS[artifact] + 1)):
                with self.subTest(artifact=artifact, bad=bad[:50]):
                    inputs = list(raw); inputs[index] = bad
                    with self.assertRaises(ValueError) as error:
                        verify_smoke_bundle(*inputs)
                    self.assertIn(artifact, str(error.exception))
                    self.assertNotIn("SECRET", str(error.exception))
        with self.assertRaises(ValueError):
            verify_smoke_bundle({}, *raw[1:])

    def test_output_does_not_copy_job_env_or_unrelated_pod_data(self):
        b = fixture()
        b[0]["metadata"]["annotations"]["private"] = "TOP-SECRET"
        b[1]["items"][0]["spec"]["containers"][0]["env"].append({"name": "PASSWORD", "value": "TOP-SECRET"})
        result = self.verify(b)
        self.assertEqual(result["status"], "internally-consistent")
        self.assertNotIn("TOP-SECRET", json.dumps(result))
        self.assertNotIn("PASSWORD", json.dumps(result))

    def test_fixed_resource_quantities_and_no_additional_accelerators(self):
        for owner in ("job", "pod"):
            for part in ("requests", "limits"):
                for key, value in (("cpu", "1"), ("memory", "3Gi"), ("nvidia.com/gpu", "1.0000000000000000000000000000001"),
                                   ("amd.com/gpu", "1"), ("custom.example/accelerator", "1"), ("cpu", "2e100000"),
                                   ("cpu", True), ("memory", 10**400)):
                    with self.subTest(owner=owner, part=part, key=key, value=value):
                        b = fixture(); spec = b[0]["spec"]["template"]["spec"] if owner == "job" else b[1]["items"][0]["spec"]
                        spec["containers"][0]["resources"][part][key] = value
                        self.check_status(b)
        b = fixture()
        for spec in (b[0]["spec"]["template"]["spec"], b[1]["items"][0]["spec"]):
            for part in ("requests", "limits"):
                spec["containers"][0]["resources"][part].update(cpu="2000m", memory="4096Mi", **{"nvidia.com/gpu": "1.0"})
        self.assertEqual(self.verify(b)["status"], "internally-consistent")
        b = fixture(); b[1]["items"][0]["spec"]["containers"][0]["resources"]["requests"].pop("cpu")
        self.check_status(b, "incomplete")

    def test_full_runtime_container_id_required(self):
        for value in ("container-one", "containerd://", "a\x01bad", "0"):
            b = fixture(); b[3]["containerID"] = value; b[1]["items"][0]["status"]["containerStatuses"][0]["containerID"] = value
            self.check_status(b)

    def test_extra_attempts_index_state_and_duplicate_pod_identities(self):
        variants = [("uncountedTerminatedPods", {"succeeded": ["other-uid"], "failed": []}),
                    ("uncountedTerminatedPods", {"succeeded": [], "failed": ["other-uid"]}),
                    ("terminating", 1), ("terminating", True), ("failedIndexes", "0"), ("completedIndexes", "0")]
        for key, value in variants:
            b = fixture(); b[0]["status"][key] = value; self.check_status(b)
        for key, value in (("completionMode", "Indexed"), ("backoffLimitPerIndex", 0), ("successPolicy", {}), ("activeDeadlineSeconds", 10**100)):
            b = fixture(); b[0]["spec"][key] = value; self.check_status(b)
        b = fixture(); b[1]["metadata"]["remainingItemCount"] = False; self.check_status(b)
        b = fixture(); b[1]["metadata"]["continue"] = 0; self.check_status(b)
        b = fixture(); b[0]["status"]["uncountedTerminatedPods"] = {"succeeded": [], "failed": []}
        b[0]["spec"]["completionMode"] = "NonIndexed"
        self.assertEqual(self.verify(b)["status"], "internally-consistent")
        for duplicate in ("uid", "name"):
            b = fixture(); other = copy.deepcopy(b[1]["items"][0]); other["metadata"]["ownerReferences"] = []
            other["metadata"]["name" if duplicate == "uid" else "uid"] = "different"
            b[1]["items"].append(other); self.check_status(b)

    def test_bounded_regular_file_reads(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); good = root / "good"
            good.write_bytes(b'{}')
            self.assertEqual(read_artifact(good, "job"), b'{}')
            large = root / "large"; large.write_bytes(b'x' * (LIMITS["receipt"] + 1))
            link = root / "link"; link.symlink_to(good)
            fifo = root / "fifo"; os.mkfifo(fifo)
            for path in (root, root / "not-found-SECRET", large, link, fifo, Path("/dev/null")):
                with self.subTest(path=path):
                    with self.assertRaises(ValueError) as error:
                        read_artifact(path, "receipt")
                    self.assertNotIn("SECRET", str(error.exception))
            with patch("benchmarks.evidence.os.read", return_value=b'x' * (LIMITS["receipt"] + 1)):
                with self.assertRaises(ValueError):
                    read_artifact(good, "receipt")

    def test_cli_is_offline_and_returns_nonzero_for_invalid_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def cli(raw, output=False):
                args = ["harness", "verify-smoke"]
                for name, value in zip(("job", "pods", "stdout", "receipt"), raw):
                    path = root / name; path.write_bytes(value); args.extend(["--" + name, str(path)])
                if output:
                    args.extend(["--output", str(root / "result.json" if output is True else output)])
                out, err = io.StringIO(), io.StringIO()
                with patch("sys.argv", args), patch("sys.stdout", out), patch("sys.stderr", err), \
                     patch.object(socket, "socket", side_effect=AssertionError("network forbidden")), \
                     patch.object(subprocess, "Popen", side_effect=AssertionError("subprocess forbidden")), \
                     patch("benchmarks.harness.build_opener", side_effect=AssertionError("HTTP forbidden")), \
                     patch("benchmarks.harness.environment_report", side_effect=AssertionError("environment probing forbidden")):
                    try:
                        main(); code = 0
                    except SystemExit as error:
                        code = error.code
                return code, out.getvalue(), err.getvalue()
            code, out, err = cli(encode_bundle(*fixture()))
            self.assertEqual(code, 0, err)
            self.assertEqual(json.loads(out)["status"], "internally-consistent")
            self.assertEqual(cli(encode_bundle(*fixture()), output=True)[0], 0)
            self.assertEqual(json.loads((root / "result.json").read_text())["status"], "internally-consistent")
            # Refuse existing report, source, symlink and hardlink paths.
            original_report = (root / "result.json").read_bytes()
            self.assertEqual(cli(encode_bundle(*fixture()), output=True)[0], 2)
            self.assertEqual((root / "result.json").read_bytes(), original_report)
            original_job = encode_bundle(*fixture())[0]
            self.assertEqual(cli(encode_bundle(*fixture()), output=root / "job")[0], 2)
            self.assertEqual((root / "job").read_bytes(), original_job)
            for name, make_link in (("symlink", lambda path: path.symlink_to(root / "job")),
                                    ("hardlink", lambda path: os.link(root / "job", path))):
                link = root / name; make_link(link)
                self.assertEqual(cli(encode_bundle(*fixture()), output=link)[0], 2)
                self.assertEqual((root / "job").read_bytes(), original_job)
            b = fixture(); b[1]["items"] = []
            code, out, err = cli(encode_bundle(*b)); self.assertEqual(code, 1)
            self.assertEqual(json.loads(out)["status"], "incomplete")
            b = fixture(); b[2]["output_finite"] = False
            self.assertEqual(cli(encode_bundle(*b))[0], 1)
            raw = encode_bundle(*fixture()); raw[0] = b'{"secret":"DO-NOT-ECHO",'
            code, out, err = cli(raw)
            self.assertEqual(code, 2)
            self.assertNotIn("DO-NOT-ECHO", err)


if __name__ == "__main__":
    unittest.main()
