"""Offline consistency checks for manually exported GPU smoke evidence.

This never runs a payload or contacts Kubernetes. A receipt is an operator
assertion, not authentication. Even a consistent bundle does not prove GPU
execution, physical whole-GPU allocation, hardware identity, or performance.
"""
import hashlib
import json
import math
import os
import re
import stat
import statistics
from datetime import datetime
from decimal import localcontext
from pathlib import Path

from davano.quantities import decimal_quantity

LIMITS = {"job": 1024 * 1024, "pods": 8 * 1024 * 1024,
          "stdout": 256 * 1024, "receipt": 64 * 1024}
MAX_DEPTH = 64
CLOCK_TOLERANCE_SECONDS = 2.0
SAMPLE_LABEL = "davano.dev/benchmark-sample"
PROGRAM_ANNOTATION = "davano.dev/payload-sha256"
# Trusted package source, read without importing/executing torch or the program.
EXPECTED_PROGRAM = Path(__file__).with_name("gpu_payload.py").read_text(encoding="utf-8")
EXPECTED_PROGRAM_SHA256 = hashlib.sha256(EXPECTED_PROGRAM.encode()).hexdigest()
RECEIPT_FIELDS = {"schema_version", "namespace", "jobUID", "podUID", "containerName",
                  "containerID", "sampleID", "imageID", "stdoutSHA256", "capturedAt"}
PAYLOAD_FIELDS = {"schema_version", "evidence_type", "suite", "started_at", "finished_at",
                  "sample_id", "gpu_name", "gpu_memory_bytes", "torch_version", "cuda_runtime",
                  "visible_gpu_count", "output_finite", "warmup_iterations", "iterations",
                  "step_ms", "mean_step_ms", "scope"}
DNS = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?\Z")
IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/@+\-]{0,511}\Z")
CONTAINER_ID = re.compile(r"[a-z][a-z0-9+.-]*://[A-Za-z0-9][A-Za-z0-9_.:/@+\-]{0,255}\Z")
IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/\-]*@sha256:[a-f0-9]{64}\Z")
SHA256 = re.compile(r"[a-f0-9]{64}\Z")
RFC3339 = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d{1,9}))?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)\Z")


def read_artifact(path, artifact):
    """Read a bounded regular file, refusing devices, FIFOs and symlinks.

    Errors contain only the fixed artifact name, never a path or file contents.
    O_NONBLOCK prevents a FIFO from hanging before its type can be checked.
    """
    if artifact not in LIMITS:
        raise ValueError("artifact: unknown input")
    fd = None
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | getattr(os, "O_NOFOLLOW", 0))
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ValueError(artifact + ": regular file required")
        limit = LIMITS[artifact]
        if before.st_size > limit:
            raise ValueError(artifact + ": size limit exceeded")
        data = bytearray()
        while len(data) <= limit:
            chunk = os.read(fd, min(65536, limit + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
        after = os.fstat(fd)
        if len(data) > limit:
            raise ValueError(artifact + ": size limit exceeded")
        if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
            raise ValueError(artifact + ": file changed during read")
        return bytes(data)
    except (OSError, TypeError):
        raise ValueError(artifact + ": cannot read regular file") from None
    finally:
        if fd is not None:
            os.close(fd)


def write_report(path, report):
    """Exclusively create a local report; never overwrite evidence or reports."""
    encoded = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
    except (OSError, TypeError):
        raise ValueError("output: cannot exclusively create new report") from None


def _quantity_equals(value, expected):
    # Bound strings and exponents before the shared exact decimal parser. Its
    # default decimal context otherwise rounds extremely precise quantities.
    if type(value) not in (str, int) or len(str(value)) > 100:
        return False
    raw = str(value)
    exponent = re.search(r"[eE]([+-]?\d+)$", raw)
    if exponent and not -30 <= int(exponent.group(1)) <= 30:
        return False
    try:
        with localcontext() as context:
            context.prec = 160
            return decimal_quantity(value) == expected
    except ValueError:
        return False


def _positive_finite(value):
    # Comparing a huge int to float is safe; converting it in isfinite is not.
    return type(value) in (int, float) and 0 < value <= float.fromhex("0x1.fffffffffffffp+1023")


def _raw(value, artifact):
    if isinstance(value, str):
        if len(value) > LIMITS[artifact]:
            raise ValueError(artifact + ": size limit exceeded")
        try:
            value = value.encode("utf-8")
        except UnicodeError:
            raise ValueError(artifact + ": invalid UTF-8") from None
    if not isinstance(value, bytes):
        raise ValueError(artifact + ": raw UTF-8 bytes or text required")
    if len(value) > LIMITS[artifact]:
        raise ValueError(artifact + ": size limit exceeded")
    return value


def _decode(raw, artifact):
    """Reject duplicate keys, nonfinite numbers and deep input before parsing."""
    try:
        text = raw.decode("utf-8")
    except UnicodeError:
        raise ValueError(artifact + ": invalid UTF-8") from None
    depth = 0
    quoted = escaped = False
    for char in text:
        if quoted:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                quoted = False
        elif char == '"':
            quoted = True
        elif char in "[{":
            depth += 1
            if depth > MAX_DEPTH:
                raise ValueError(artifact + ": JSON nesting limit exceeded")
        elif char in "]}":
            depth -= 1
    def pairs(items):
        obj = {}
        for key, value in items:
            if key in obj:
                raise ValueError("duplicate")
            obj[key] = value
        return obj
    def number(value):
        result = float(value)
        if not math.isfinite(result):
            raise ValueError("nonfinite")
        return result
    def invalid_constant(value):
        raise ValueError("nonfinite")
    try:
        obj = json.loads(text, object_pairs_hook=pairs, parse_float=number,
                         parse_constant=invalid_constant)
    except (ValueError, RecursionError, OverflowError):
        raise ValueError(artifact + ": invalid, duplicate-key, or nonfinite JSON") from None
    if not isinstance(obj, dict):
        raise ValueError(artifact + ": JSON object required")
    return obj


class _Gap(Exception):
    pass


class _Checks:
    def __init__(self):
        self.missing = []
        self.rejected = []

    def require(self, obj, key, field):
        if not isinstance(obj, dict):
            raise ValueError(field + ": object required")
        if key not in obj or obj[key] is None:
            self.missing.append(field + ": missing")
            raise _Gap
        return obj[key]

    def object(self, obj, key, field):
        result = self.require(obj, key, field)
        if not isinstance(result, dict):
            raise ValueError(field + ": object required")
        return result

    def array(self, obj, key, field):
        result = self.require(obj, key, field)
        if not isinstance(result, list):
            raise ValueError(field + ": array required")
        return result

    def text(self, obj, key, field, pattern=IDENTITY):
        result = self.require(obj, key, field)
        if not isinstance(result, str) or len(result) > 512 or not pattern.fullmatch(result):
            self.rejected.append(field + ": invalid string")
            raise _Gap
        return result

    def exact(self, obj, key, expected, field):
        result = self.require(obj, key, field)
        if type(result) is not type(expected) or result != expected:
            self.rejected.append(field + ": unexpected value")
        return result

    def reject(self, condition, field):
        if condition:
            self.rejected.append(field)

    def time(self, obj, key, field):
        value = self.require(obj, key, field)
        match = RFC3339.fullmatch(value) if isinstance(value, str) else None
        try:
            if not match:
                raise ValueError
            result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except (ValueError, OverflowError):
            self.rejected.append(field + ": timezone-aware RFC3339 timestamp required")
            raise _Gap from None
        # datetime retains microseconds; submicrosecond text is conservatively
        # represented by one microsecond. Whole-second timestamps have 1s bins.
        resolution = 10 ** -min(len(match.group(1) or ""), 6)
        return result, resolution, value

    def run(self, function):
        try:
            return function()
        except _Gap:
            return None


def _one_container(c, spec, field):
    containers = c.array(spec, "containers", field + ".containers")
    if len(containers) != 1:
        c.reject(bool(containers), field + ".containers: exactly one container required")
        if not containers:
            c.missing.append(field + ".containers: missing benchmark container")
        raise _Gap
    target = containers[0]
    c.exact(target, "name", "benchmark", field + ".containers.name")
    for key in ("initContainers", "ephemeralContainers"):
        c.reject(key in spec and spec[key] != [], field + "." + key + ": unsupported")
    c.exact(spec, "restartPolicy", "Never", field + ".restartPolicy")
    image = c.text(target, "image", field + ".containers.image", IMAGE)
    c.exact(target, "command", ["python", "-c", EXPECTED_PROGRAM], field + ".containers.command")
    c.reject(target.get("args", []) != [], field + ".containers.args: unexpected arguments")
    c.reject(bool(target.get("envFrom")), field + ".containers.envFrom: indirect environment unsupported")
    env = c.array(target, "env", field + ".containers.env")
    sample_env = []
    for item in env:
        if not isinstance(item, dict):
            raise ValueError(field + ".containers.env: object entries required")
        if item.get("name") == "DAVANO_BENCH_SAMPLE":
            sample_env.append(item)
    if not sample_env:
        c.missing.append(field + ".DAVANO_BENCH_SAMPLE: missing")
        raise _Gap
    if len(sample_env) != 1:
        c.rejected.append(field + ".DAVANO_BENCH_SAMPLE: duplicate variable")
        raise _Gap
    c.reject("valueFrom" in sample_env[0], field + ".DAVANO_BENCH_SAMPLE: indirect value unsupported")
    sample = c.text(sample_env[0], "value", field + ".DAVANO_BENCH_SAMPLE", DNS)
    resources = c.object(target, "resources", field + ".resources")
    for part in ("requests", "limits"):
        resource = c.object(resources, part, field + ".resources." + part)
        c.reject(bool(set(resource) - {"cpu", "memory", "nvidia.com/gpu"}),
                 field + ".resources." + part + ": additional resources unsupported")
        for key, expected in (("cpu", 2), ("memory", 4 * 1024**3), ("nvidia.com/gpu", 1)):
            value = c.require(resource, key, field + ".resources." + part + "." + key)
            c.reject(not _quantity_equals(value, expected),
                     field + ".resources." + part + "." + key + ": fixed resource quantity required")
    return {"requested_image": image, "sample_id": sample}


def _receipt(c, receipt):
    c.reject(bool(set(receipt) - RECEIPT_FIELDS), "receipt: unexpected fields")
    c.exact(receipt, "schema_version", 1, "receipt.schema_version")
    result = {}
    for key in ("namespace", "jobUID", "podUID", "containerName", "containerID", "sampleID", "imageID", "stdoutSHA256"):
        pattern = DNS if key in ("namespace", "sampleID", "containerName") else SHA256 if key == "stdoutSHA256" else CONTAINER_ID if key == "containerID" else IDENTITY
        result[key] = c.text(receipt, key, "receipt." + key, pattern)
    result["capturedAt"] = c.time(receipt, "capturedAt", "receipt.capturedAt")
    c.reject(result["containerName"] != "benchmark", "receipt.containerName: benchmark required")
    return result


def _job(c, job):
    c.exact(job, "kind", "Job", "job.kind")
    c.exact(job, "apiVersion", "batch/v1", "job.apiVersion")
    meta = c.object(job, "metadata", "job.metadata")
    result = {key: c.text(meta, key, "job.metadata." + key, DNS if key == "namespace" else IDENTITY)
              for key in ("uid", "name", "namespace")}
    labels = c.object(meta, "labels", "job.metadata.labels")
    result["sample_id"] = c.text(labels, SAMPLE_LABEL, "job.sample_label", DNS)
    annotations = c.object(meta, "annotations", "job.metadata.annotations")
    c.exact(annotations, PROGRAM_ANNOTATION, EXPECTED_PROGRAM_SHA256, "job.payload_sha256")
    result["created"] = c.time(meta, "creationTimestamp", "job.metadata.creationTimestamp")
    spec = c.object(job, "spec", "job.spec")
    for key, expected in (("completions", 1), ("parallelism", 1), ("backoffLimit", 0), ("activeDeadlineSeconds", 600)):
        c.exact(spec, key, expected, "job.spec." + key)
    c.reject(spec.get("suspend", False) is not False, "job.spec.suspend: job remains suspended")
    if "completionMode" in spec:
        c.exact(spec, "completionMode", "NonIndexed", "job.spec.completionMode")
    for key in ("backoffLimitPerIndex", "maxFailedIndexes", "successPolicy"):
        c.reject(key in spec, "job.spec." + key + ": unsupported indexed/retry policy")
    template = c.object(spec, "template", "job.spec.template")
    template_meta = c.object(template, "metadata", "job.spec.template.metadata")
    template_labels = c.object(template_meta, "labels", "job.spec.template.metadata.labels")
    c.exact(template_labels, SAMPLE_LABEL, result["sample_id"], "job.template.sample_label")
    template_spec = c.object(template, "spec", "job.spec.template.spec")
    result.update(_one_container(c, template_spec, "job.template.spec"))
    c.reject(result["sample_id"] != labels[SAMPLE_LABEL], "job.DAVANO_BENCH_SAMPLE: label mismatch")
    status = c.object(job, "status", "job.status")
    c.exact(status, "succeeded", 1, "job.status.succeeded")
    for key in ("failed", "active", "terminating"):
        c.reject(key in status and (type(status[key]) is not int or status[key] != 0), "job.status." + key + ": must be zero")
    for key in ("failedIndexes", "completedIndexes"):
        c.reject(key in status and status[key] != "", "job.status." + key + ": unsupported index state")
    if "uncountedTerminatedPods" in status:
        uncounted = c.object(status, "uncountedTerminatedPods", "job.status.uncountedTerminatedPods")
        c.reject(bool(set(uncounted) - {"succeeded", "failed"}), "job.status.uncountedTerminatedPods: unexpected fields")
        for key in ("succeeded", "failed"):
            c.reject(key in uncounted and uncounted[key] != [], "job.status.uncountedTerminatedPods: uncounted attempts")
    conditions = c.array(status, "conditions", "job.status.conditions")
    seen, complete = set(), []
    for condition in conditions:
        kind = c.text(condition, "type", "job.status.conditions.type")
        state = c.require(condition, "status", "job.status.conditions.status")
        c.reject(kind in seen, "job.status.conditions: duplicate condition")
        seen.add(kind)
        c.reject(kind not in {"Complete", "Failed", "FailureTarget", "SuccessCriteriaMet", "Suspended"}, "job.status.conditions: unsupported condition type")
        c.reject(state not in ("True", "False", "Unknown"), "job.status.conditions.status: invalid value")
        if kind == "Complete" and state == "True":
            complete.append(condition)
        c.reject(kind in ("Failed", "FailureTarget", "Suspended") and state == "True", "job.status.conditions: conflicting condition")
        c.reject(kind == "SuccessCriteriaMet" and state != "True", "job.status.conditions: conflicting success condition")
    if not complete:
        c.missing.append("job.status.conditions.Complete: missing true condition")
        raise _Gap
    result["completed"] = c.time(status, "completionTime", "job.status.completionTime")
    result["transition"] = c.time(complete[0], "lastTransitionTime", "job.status.conditions.Complete.lastTransitionTime")
    c.reject(result["completed"][0] < result["created"][0], "job.status.completionTime: precedes creation")
    c.reject(result["transition"][0] < result["completed"][0], "job.status.conditions.Complete: precedes completion")
    return result


def _pod(c, job, pods):
    c.exact(pods, "kind", "PodList", "pods.kind")
    c.exact(pods, "apiVersion", "v1", "pods.apiVersion")
    listing_meta = pods.get("metadata", {})
    if not isinstance(listing_meta, dict):
        raise ValueError("pods.metadata: object required")
    continuation = listing_meta.get("continue", "")
    remaining = listing_meta.get("remainingItemCount", 0)
    c.reject(not isinstance(continuation, str), "pods.metadata.continue: string required")
    c.reject(type(remaining) is not int or remaining < 0, "pods.metadata.remainingItemCount: nonnegative integer required")
    if continuation or remaining != 0:
        c.missing.append("pods: complete unpaginated PodList required")
    items = c.array(pods, "items", "pods.items")
    meta = c.object(job, "metadata", "job.metadata")
    uid = c.text(meta, "uid", "job.metadata.uid")
    namespace = c.text(meta, "namespace", "job.metadata.namespace", DNS)
    candidates, seen_uids, seen_names = [], set(), set()
    for item in items:
        c.exact(item, "kind", "Pod", "pods.items.kind")
        c.exact(item, "apiVersion", "v1", "pods.items.apiVersion")
        pm = c.object(item, "metadata", "pods.items.metadata")
        pod_uid = c.text(pm, "uid", "pods.items.metadata.uid")
        pod_name = c.text(pm, "name", "pods.items.metadata.name")
        pod_namespace = c.text(pm, "namespace", "pods.items.metadata.namespace", DNS)
        c.reject(pod_uid in seen_uids, "pods.items.metadata.uid: duplicate Pod UID")
        c.reject((pod_namespace, pod_name) in seen_names, "pods.items.metadata.name: duplicate namespaced Pod name")
        seen_uids.add(pod_uid)
        seen_names.add((pod_namespace, pod_name))
        owners = pm.get("ownerReferences", [])
        if not isinstance(owners, list) or any(not isinstance(o, dict) for o in owners):
            raise ValueError("pods.ownerReferences: object array required")
        matches = [o for o in owners if o.get("uid") == uid]
        if matches:
            c.reject(len(matches) != 1 or len([o for o in owners if o.get("controller") is True]) != 1,
                     "pod.ownerReferences: ambiguous ownership")
            c.reject(any(o.get("kind") != "Job" or o.get("controller") is not True or o.get("apiVersion") != "batch/v1" or o.get("name") != meta.get("name") for o in matches),
                     "pod.ownerReferences: inconsistent Job controller")
            c.reject(pm.get("namespace") != namespace, "pod.namespace: Job namespace mismatch")
            candidates.append(item)
    if len(candidates) != 1:
        if not candidates:
            c.missing.append("pods: no UID-owned Pod")
        else:
            c.rejected.append("pods: multiple UID-owned Pods")
        raise _Gap
    pod = candidates[0]
    c.exact(pod, "kind", "Pod", "pod.kind")
    c.exact(pod, "apiVersion", "v1", "pod.apiVersion")
    pm = pod["metadata"]
    result = {key: c.text(pm, key, "pod.metadata." + key) for key in ("uid", "name")}
    labels = c.object(pm, "labels", "pod.metadata.labels")
    label = c.text(labels, SAMPLE_LABEL, "pod.sample_label", DNS)
    spec = c.object(pod, "spec", "pod.spec")
    result.update(_one_container(c, spec, "pod.spec"))
    c.reject(result["sample_id"] != label, "pod.DAVANO_BENCH_SAMPLE: label mismatch")
    annotations = pm.get("annotations", {})
    if not isinstance(annotations, dict):
        raise ValueError("pod.metadata.annotations: object required")
    if PROGRAM_ANNOTATION in annotations:
        c.exact(annotations, PROGRAM_ANNOTATION, EXPECTED_PROGRAM_SHA256, "pod.payload_sha256")
    status = c.object(pod, "status", "pod.status")
    phase = c.require(status, "phase", "pod.status.phase")
    if phase in ("Pending", "Running", "Unknown"):
        c.missing.append("pod.status.phase: not complete")
    else:
        c.reject(phase != "Succeeded", "pod.status.phase: not succeeded")
    for key in ("initContainerStatuses", "ephemeralContainerStatuses"):
        c.reject(key in status and status[key] != [], "pod.status." + key + ": unsupported")
    statuses = c.array(status, "containerStatuses", "pod.status.containerStatuses")
    if len(statuses) != 1:
        if not statuses:
            c.missing.append("pod.status.containerStatuses: missing")
        else:
            c.rejected.append("pod.status.containerStatuses: ambiguous containers")
        raise _Gap
    target = statuses[0]
    c.exact(target, "name", "benchmark", "pod.containerStatus.name")
    c.exact(target, "restartCount", 0, "pod.containerStatus.restartCount")
    c.reject(target.get("lastState", {}) != {}, "pod.containerStatus.lastState: prior execution unsupported")
    result["containerID"] = c.text(target, "containerID", "pod.containerStatus.containerID", CONTAINER_ID)
    result["imageID"] = c.text(target, "imageID", "pod.containerStatus.imageID")
    state = c.object(target, "state", "pod.containerStatus.state")
    c.reject(bool(set(state) - {"terminated"}), "pod.containerStatus.state: not exclusively terminated")
    terminated = c.object(state, "terminated", "pod.containerStatus.state.terminated")
    c.exact(terminated, "exitCode", 0, "pod.terminated.exitCode")
    if "signal" in terminated:
        c.exact(terminated, "signal", 0, "pod.terminated.signal")
    if "containerID" in terminated:
        c.exact(terminated, "containerID", result["containerID"], "pod.terminated.containerID")
    result["started"] = c.time(terminated, "startedAt", "pod.terminated.startedAt")
    result["finished"] = c.time(terminated, "finishedAt", "pod.terminated.finishedAt")
    c.reject(result["finished"][0] < result["started"][0], "pod.terminated.finishedAt: precedes start")
    return result


def _payload(c, raw):
    if not raw.strip():
        c.missing.append("stdout: missing payload")
        raise _Gap
    # The fixed program emits one JSON line; do not cherry-pick a log object.
    if len(raw.splitlines()) != 1:
        c.rejected.append("stdout: exactly one JSON line required")
        raise _Gap
    payload = _decode(raw, "stdout")
    c.reject(bool(set(payload) - PAYLOAD_FIELDS), "stdout: unexpected payload fields")
    for key, expected in (("schema_version", 1), ("evidence_type", "live-gpu-payload"),
            ("suite", "cuda-fp16-matmul-4096/v1"), ("visible_gpu_count", 1),
            ("iterations", 100), ("warmup_iterations", 10), ("output_finite", True),
            ("scope", "CUDA execution plus per-step host synchronization; not scheduler latency")):
        c.exact(payload, key, expected, "stdout." + key)
    result = {"sample_id": c.text(payload, "sample_id", "stdout.sample_id", DNS)}
    for key in ("gpu_name", "torch_version", "cuda_runtime"):
        value = c.require(payload, key, "stdout." + key)
        if not isinstance(value, str) or not 1 <= len(value) <= 256 or any(ord(char) < 32 or ord(char) > 126 for char in value):
            c.rejected.append("stdout." + key + ": bounded printable string required")
            raise _Gap
        result[key] = value
    memory = c.require(payload, "gpu_memory_bytes", "stdout.gpu_memory_bytes")
    c.reject(type(memory) is not int or not 0 < memory <= 2**63 - 1, "stdout.gpu_memory_bytes: positive bounded integer required")
    result["gpu_memory_bytes"] = memory
    steps = c.array(payload, "step_ms", "stdout.step_ms")
    if len(steps) != 100 or any(not _positive_finite(x) for x in steps):
        c.rejected.append("stdout.step_ms: exactly 100 finite positive numbers required")
        raise _Gap
    mean = c.require(payload, "mean_step_ms", "stdout.mean_step_ms")
    if not _positive_finite(mean):
        c.rejected.append("stdout.mean_step_ms: finite positive number required")
        raise _Gap
    try:
        total = math.fsum(steps)
        recomputed = statistics.mean(steps)
    except (OverflowError, ValueError):
        c.rejected.append("stdout.step_ms: aggregate overflow")
        raise _Gap from None
    c.reject(not math.isclose(mean, recomputed, rel_tol=1e-9, abs_tol=1e-9), "stdout.mean_step_ms: sample mean mismatch")
    result.update(step_ms=steps, mean_step_ms=recomputed, total_step_ms=total)
    result["started"] = c.time(payload, "started_at", "stdout.started_at")
    result["finished"] = c.time(payload, "finished_at", "stdout.finished_at")
    elapsed = (result["finished"][0] - result["started"][0]).total_seconds()
    c.reject(elapsed < 0, "stdout.finished_at: precedes start")
    # Same-process endpoints get only their serialized resolution, not the
    # cross-component 2s clock allowance. A fabricated short interval must fail.
    c.reject(total / 1000 > elapsed + result["started"][1] + result["finished"][1] + 1e-9,
             "stdout.step_ms: sum exceeds payload interval and timestamp resolution")
    return result


def verify_smoke_bundle(job, pods, stdout, receipt):
    """Check four raw artifacts without I/O or execution; return a safe report.

    Accept UTF-8 bytes or text, never pre-parsed mappings (duplicate keys and
    exact byte hashes would otherwise be lost). Malformed inputs raise sanitized
    ValueError. Missing evidence returns incomplete; contradictions return
    rejected. Only internally-consistent means all offline checks passed.
    """
    raw = {name: _raw(value, name) for name, value in
           (("job", job), ("pods", pods), ("stdout", stdout), ("receipt", receipt))}
    objects = {name: _decode(raw[name], name) for name in ("job", "pods", "receipt")}
    c = _Checks()
    r = c.run(lambda: _receipt(c, objects["receipt"]))
    j = c.run(lambda: _job(c, objects["job"]))
    p = c.run(lambda: _pod(c, objects["job"], objects["pods"]))
    payload = c.run(lambda: _payload(c, raw["stdout"]))
    hashes = {name: hashlib.sha256(value).hexdigest() for name, value in raw.items()}
    if r:
        c.reject(r["stdoutSHA256"] != hashes["stdout"], "receipt.stdoutSHA256: raw stdout mismatch")
    if r and j:
        for source, key in (("namespace", "namespace"), ("jobUID", "uid"), ("sampleID", "sample_id")):
            c.reject(r[source] != j[key], "receipt." + source + ": Job mismatch")
    if r and p:
        for source, key in (("podUID", "uid"), ("containerID", "containerID"), ("imageID", "imageID"), ("sampleID", "sample_id")):
            c.reject(r[source] != p[key], "receipt." + source + ": Pod mismatch")
        c.reject((r["capturedAt"][0] - p["finished"][0]).total_seconds() < -CLOCK_TOLERANCE_SECONDS,
                 "receipt.capturedAt: precedes container finish beyond 2s clock allowance")
    if j and p:
        for key in ("sample_id", "requested_image"):
            c.reject(j[key] != p[key], "pod." + key + ": Job template mismatch")
        for earlier, later, field in ((j["created"], p["started"], "pod.startedAt: precedes Job creation"),
                                      (p["finished"], j["completed"], "job.completionTime: precedes container finish")):
            c.reject((later[0] - earlier[0]).total_seconds() < -CLOCK_TOLERANCE_SECONDS, field)
    if payload and p:
        c.reject(payload["sample_id"] != p["sample_id"], "stdout.sample_id: Pod mismatch")
        for earlier, later, field in ((p["started"], payload["started"], "stdout.started_at: precedes container start"),
                                      (payload["finished"], p["finished"], "stdout.finished_at: exceeds container finish")):
            c.reject((later[0] - earlier[0]).total_seconds() < -CLOCK_TOLERANCE_SECONDS, field)
        runtime = (p["finished"][0] - p["started"][0]).total_seconds()
        c.reject(payload["total_step_ms"] / 1000 > runtime + p["started"][1] + p["finished"][1] + 1e-9,
                 "stdout.step_ms: sum exceeds container interval and timestamp resolution")
    status = "rejected" if c.rejected else "incomplete" if c.missing else "internally-consistent"
    result = {"schema_version": 1, "evidence_type": "gpu-smoke-consistency", "status": status,
              "evidence_scope": "offline-consistency-only", "gpu_execution_verified": False,
              "hardware_verified": False, "whole_gpu_physical_mode": "UNVERIFIED", "comparative_claim": None,
              "missing": sorted(set(c.missing)), "contradictions": sorted(set(c.rejected)),
              "artifact_sha256": hashes,
              "provenance": {"capture_receipt": "operator-declaration-not-authentication",
                             "artifact_hashes": "byte-identity-not-authentication",
                             "pod_list_completeness": "operator-supplied; omissions cannot be independently detected",
                             "expected_payload_sha256": EXPECTED_PROGRAM_SHA256},
              "timing_policy": {"cross_component_tolerance_seconds": CLOCK_TOLERANCE_SECONDS,
                                "duration_allowance": "sum of serialized endpoint resolutions (minimum 1 microsecond each)",
                                "mean_relative_tolerance": 1e-9, "mean_absolute_tolerance_ms": 1e-9},
              "limitations": ["Offline consistency is not proof of execution or authentic collection.",
                              "One requested NVIDIA resource and one reported visible CUDA device do not prove whole physical GPU allocation.",
                              "Observed imageID is preserved, not equated to an OCI index, manifest or config digest.",
                              "No scheduler comparison, campaign promotion or performance claim is permitted."]}
    # Only emit matched identities and measurements when every check passed;
    # failed bundles cannot accidentally look like accepted samples downstream.
    if status == "internally-consistent":
        result["identity"] = {"namespace": j["namespace"], "job_uid": j["uid"], "job_name": j["name"],
                              "pod_uid": p["uid"], "pod_name": p["name"], "container_name": "benchmark",
                              "container_id": p["containerID"], "requested_image": p["requested_image"],
                              "observed_image_id": p["imageID"], "sample_id": payload["sample_id"]}
        result["timing"] = {"container_started_at": p["started"][2], "container_finished_at": p["finished"][2],
                            "payload_started_at": payload["started"][2], "payload_finished_at": payload["finished"][2],
                            "captured_at": r["capturedAt"][2]}
        result["payload"] = {key: payload[key] for key in ("gpu_name", "gpu_memory_bytes", "torch_version", "cuda_runtime", "step_ms", "mean_step_ms", "total_step_ms")}
        result["payload"].update(suite="cuda-fp16-matmul-4096/v1", visible_gpu_count=1,
                                 warmup_iterations=10, iterations=100, output_finite=True)
    return result
