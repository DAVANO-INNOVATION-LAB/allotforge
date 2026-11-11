"""Offline adapters. Generate manifests and parse exports; never contact a cluster."""
import copy
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

PRODUCTS = ("davano-kueue", "kueue", "kai")
LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")


def digest(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def make_job(product, *, namespace, queue, image, node_selector, sample_id):
    """Same pod payload/resources across schedulers; no queue/CRD installation.

    Kueue jobs must begin suspended for admission. KAI jobs begin unsuspended
    because KAI does not unsuspend Jobs; manifests are only inert local output.
    """
    if product not in PRODUCTS:
        raise ValueError("Unsupported batch adapter; GPUStack is an inference lane")
    for label in (namespace, queue, sample_id):
        if not isinstance(label, str) or not LABEL.fullmatch(label):
            raise ValueError("Namespace, queue and sample_id must be DNS labels")
    if namespace in ("default", "davano", "kai-scheduler") or namespace.startswith(("kube-", "openshift")):
        raise ValueError("Use a dedicated benchmark namespace")
    if not re.fullmatch(r"[^\s@]+@sha256:[a-f0-9]{64}", image):
        raise ValueError("An administrator-verified immutable image digest is required")
    if not isinstance(node_selector, dict) or not node_selector:
        raise ValueError("An explicit fixed node selector is required")
    from davano.models import PoolCreate
    PoolCreate.validate_selector(node_selector)
    payload = Path(__file__).with_name("gpu_payload.py").read_text()
    pod = {
        "restartPolicy": "Never", "automountServiceAccountToken": False,
        "nodeSelector": copy.deepcopy(node_selector),
        "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
        "containers": [{"name": "benchmark", "image": image, "imagePullPolicy": "IfNotPresent",
            "command": ["python", "-c", payload],
            "env": [{"name": "DAVANO_BENCH_SAMPLE", "value": sample_id}],
            "resources": {"requests": {"cpu": "2", "memory": "4Gi", "nvidia.com/gpu": "1"},
                          "limits": {"cpu": "2", "memory": "4Gi", "nvidia.com/gpu": "1"}},
            "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]}},
            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}]}],
        "volumes": [{"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}}],
    }
    labels = {"app.kubernetes.io/name": "davano-benchmark", "davano.dev/benchmark-sample": sample_id}
    job_labels = dict(labels)
    if product == "kai":
        labels["kai.scheduler/queue"] = queue
        pod["schedulerName"] = "kai-scheduler"
    else:
        job_labels["kueue.x-k8s.io/queue-name"] = queue
    return {"apiVersion": "batch/v1", "kind": "Job", "metadata": {
        "name": "bench-" + sample_id[:45] + "-" + hashlib.sha256(sample_id.encode()).hexdigest()[:10], "namespace": namespace, "labels": job_labels,
        "annotations": {"davano.dev/benchmark-adapter": product, "davano.dev/payload-sha256": hashlib.sha256(payload.encode()).hexdigest()}},
        "spec": {"suspend": product != "kai", "backoffLimit": 0, "completions": 1, "parallelism": 1,
                 "activeDeadlineSeconds": 600, "template": {"metadata": {"labels": labels}, "spec": pod}}}


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError("Timestamp must be a timezone-aware RFC3339 string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Timezone missing from timestamp")
    return result


def elapsed_ms(start, end):
    if not start or not end:
        return None
    elapsed = (timestamp(end) - timestamp(start)).total_seconds() * 1000
    if elapsed < 0:
        raise ValueError("Timestamp ordering is invalid; investigate clock skew")
    return elapsed


def import_job(job, pods):
    """Join by controller owner UID, not names. One attempt, one main container.

    Missing events stay null. PodScheduled/terminated fields come from multiple
    components; clock synchronization and its error bound remain prerequisites.
    """
    meta = job.get("metadata", {})
    uid = meta.get("uid")
    if not uid or job.get("kind") != "Job":
        raise ValueError("Job export must include its UID and kind")
    attempts = [p for p in pods if p.get("metadata", {}).get("namespace") == meta.get("namespace") and
                any(o.get("uid") == uid and o.get("kind") == "Job" and o.get("controller") is True
                    for o in p.get("metadata", {}).get("ownerReferences", []))]
    if len(attempts) > 1:
        raise ValueError("Multiple attempts require explicit retry analysis; do not cherry-pick one")
    sample = {"sample_id": meta.get("labels", {}).get("davano.dev/benchmark-sample", meta.get("name")),
              "status": "incomplete", "source_export_sha256": digest({"job": job, "pods": attempts}), "metrics": {"submit_to_scheduled_ms": None, "submit_to_started_ms": None,
                                                    "submit_to_finished_ms": None, "container_runtime_ms": None}}
    if not attempts:
        return sample
    pod = attempts[0]
    conditions = pod.get("status", {}).get("conditions", [])
    scheduled = next((c.get("lastTransitionTime") for c in conditions if c.get("type") == "PodScheduled" and c.get("status") == "True"), None)
    statuses = pod.get("status", {}).get("containerStatuses", [])
    target = next((c for c in statuses if c.get("name") == "benchmark"), None)
    if target and target.get("restartCount", 0) != 0:
        raise ValueError("Container restarts require explicit retry analysis")
    state = (target or {}).get("state", {})
    terminated = state.get("terminated", {})
    started = terminated.get("startedAt") or state.get("running", {}).get("startedAt")
    finished = terminated.get("finishedAt")
    if terminated:
        if type(terminated.get("exitCode")) is not int:
            raise ValueError("Integer container exitCode required")
        sample["status"] = "failed" if terminated["exitCode"] != 0 else ("succeeded" if pod.get("status", {}).get("phase") == "Succeeded" else "incomplete")
    elif pod.get("status", {}).get("phase") == "Failed":
        sample["status"] = "failed"
    submitted = meta.get("creationTimestamp")
    sample["metrics"] = {"submit_to_scheduled_ms": elapsed_ms(submitted, scheduled),
                         "submit_to_started_ms": elapsed_ms(submitted, started),
                         "submit_to_finished_ms": elapsed_ms(submitted, finished),
                         "container_runtime_ms": elapsed_ms(started, finished)}
    if scheduled and started:
        elapsed_ms(scheduled, started)
    return sample
