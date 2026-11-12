"""Fixed CPU-only Kueue acceptance. Plan offline; execute only on an explicit kind target.

This tests AllotForge-generated queue specifications, not a successful managed
GPUPool reconciliation. Synthetic pool/policy inputs are never sent to the API.
No Pods, Jobs, node patches, credentials, RBAC grants or installations are made.
"""
from __future__ import annotations

import argparse
import base64
import binascii
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time

CONTEXT = "kind-allotforge-acceptance"
KUBERNETES = "v1.34.11"
KUEUE_VERSION = "0.20.5"
API = "kueue.x-k8s.io/v1beta2"
ROOT = "/apis/" + API
KUEUE_IMAGE = "registry.k8s.io/kueue/kueue:v0.20.5@sha256:c91c4d4ffb3e38a686dbb2a4af936edcc6e613acf4b7074d84e0b990930203ea"
DISPOSABLE_LABEL = "allotforge.dev/disposable"
RUN_LABEL = "allotforge.dev/acceptance-run"
MANAGER_LABEL = "app.kubernetes.io/managed-by"
MANAGER = "allotforge-acceptance"
MAX_BYTES = 2 * 1024 * 1024
MAX_INPUT = 65536
REQUEST_SECONDS = 10
RUN_SECONDS = 180
CLEANUP_SECONDS = 60
MAX_CALLS = 240
POLL_SECONDS = 1
UID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
UNVERIFIED = ["physical_gpu", "gpu_placement", "workload_execution", "managed_reconciler_success", "tenant_rbac", "leader_lease", "native_admission", "production_readiness"]


class Rejected(RuntimeError):
    """Only fixed, non-sensitive reason codes may leave this module."""


def require(condition, code):
    if not condition:
        raise Rejected(code)


def strict_json(data):
    """Reject ambiguous, oversized, nonfinite or deeply nested API responses."""
    require(isinstance(data, bytes) and len(data) <= MAX_BYTES, "response_size")
    def pairs(entries):
        result = {}
        for key, value in entries:
            require(key not in result, "duplicate_json_key")
            result[key] = value
        return result
    def nonfinite(_):
        raise Rejected("nonfinite_json")
    try:
        result = json.loads(data, object_pairs_hook=pairs, parse_constant=nonfinite)
        require(isinstance(result, dict), "response_shape")
        stack = [(result, 0)]
        count = 0
        while stack:
            value, depth = stack.pop()
            count += 1
            require(depth <= 32 and count <= 100000, "response_complexity")
            if isinstance(value, dict):
                stack.extend((item, depth + 1) for item in value.values())
            elif isinstance(value, list):
                stack.extend((item, depth + 1) for item in value)
            elif isinstance(value, float):
                # API evidence needs no floating point values, including 1e999.
                require(False, "noninteger_json_number")
        return result
    except (ValueError, UnicodeError, RecursionError):
        raise Rejected("invalid_json") from None


@dataclass(frozen=True)
class Target:
    context: str
    server: str
    kube_system_uid: str
    kubernetes_version: str
    run_id: str

    def __post_init__(self):
        require(self.context == CONTEXT, "context_refused")
        require(isinstance(self.server, str) and re.fullmatch(r"https://127\.0\.0\.1:([1-9][0-9]{0,4})", self.server), "server_refused")
        require(1 <= int(self.server.rsplit(":", 1)[1]) <= 65535, "server_refused")
        require(isinstance(self.kube_system_uid, str) and UID.fullmatch(self.kube_system_uid), "target_uid_refused")
        require(self.kubernetes_version == KUBERNETES, "version_refused")
        require(isinstance(self.run_id, str) and re.fullmatch(r"[0-9a-f]{12}", self.run_id), "run_id_refused")

    @property
    def namespace(self):
        return "allotforge-acceptance-" + self.run_id


def generator_inputs(run_id):
    """These UIDs/quotas/selectors are fixture inputs, NOT observed GPU capacity."""
    ns = "allotforge-acceptance-" + run_id
    pool = {"apiVersion": "fleet.davano.io/v1alpha1", "kind": "GPUPool", "metadata": {"name": "fixture-pool", "uid": run_id},
            "spec": {"nodeSelector": {"allotforge.dev.node-restriction.kubernetes.io/fixture": run_id}, "resourceName": "nvidia.com/gpu", "nominalQuota": 0, "mode": "whole-gpu"}}
    policy = {"apiVersion": "fleet.davano.io/v1alpha1", "kind": "GPUAccessPolicy", "metadata": {"name": "fixture-policy", "namespace": ns, "uid": run_id},
              "spec": {"poolRef": "fixture-pool", "gpuQuota": 0, "maxGPUsPerPod": 1, "subjects": [{"kind": "Group", "name": "acceptance-fixture-no-grants"}]}}
    return pool, policy


def fixtures(run_id):
    require(isinstance(run_id, str) and re.fullmatch(r"[0-9a-f]{12}", run_id), "run_id_refused")
    from davano import reconciler
    require(reconciler.KUEUE_API == API and reconciler.KUEUE == ROOT, "generator_api_version")
    pool, policy = generator_inputs(run_id)
    ns = policy["metadata"]["namespace"]
    objects = [("/api/v1/namespaces", {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": ns}})]
    objects.extend(reconciler.pool_resources(pool, [ns], "1", "64Mi"))
    objects.extend((path, obj) for path, obj in reconciler.access_resources(policy, pool) if obj["kind"] == "LocalQueue")
    for name, cpu in (("cpu-admitted", "100m"), ("cpu-pending", "1")):
        objects.append((ROOT + "/namespaces/" + ns + "/workloads", {
            "apiVersion": API, "kind": "Workload", "metadata": {"name": name, "namespace": ns},
            "spec": {"queueName": reconciler.policy_name(policy), "podSets": [{"name": "main", "count": 1, "template": {"spec": {
                "containers": [{"name": "cpu", "image": "registry.k8s.io/pause:3.10", "resources": {"requests": {"cpu": cpu, "memory": "16Mi"}}}],
                "restartPolicy": "Never", "automountServiceAccountToken": False}}}]}}))
    for _, obj in objects:
        obj["metadata"]["labels"] = {RUN_LABEL: run_id, MANAGER_LABEL: MANAGER}
        obj["metadata"]["annotations"] = {"allotforge.dev/evidence-scope": "synthetic-generated-resource-cpu-only"}
    return objects


def plan(run_id="000000000000"):
    return {
        "schema": "allotforge.cluster-acceptance.plan.v1", "mode": "offline_plan", "target_context": CONTEXT,
        "kubernetes_version": KUBERNETES, "kueue_version": KUEUE_VERSION, "kueue_api": API, "kueue_controller_image": KUEUE_IMAGE,
        "required_target": {"kubeconfig_json": "private 0600 single-context kind JSON with embedded CA and client certificate/key only", "server": "https://127.0.0.1:<explicit-port>", "kube_system_uid": "<explicit-UUID>", "kube_system_labels": {DISPOSABLE_LABEL: "cluster-acceptance"}, "node": "allotforge-acceptance-control-plane"},
        "bounds": {"run_seconds": RUN_SECONDS, "cleanup_seconds": CLEANUP_SECONDS, "request_seconds": REQUEST_SECONDS, "maximum_calls_per_phase": MAX_CALLS, "maximum_response_bytes": MAX_BYTES},
        "permissions": {"read": ["version", "nodes", "kube-system namespace", "kueue-system/kueue-controller-manager deployment", "Kueue v1beta2 discovery"], "create_get_delete": ["one owned namespace", "one ResourceFlavor", "one ClusterQueue", "one LocalQueue", "two standalone Workloads"], "namespace_read_only": ["Pods", "Jobs"]},
        "cases": ["generated_queue_activation", "cpu_quota_admitted", "cpu_quota_pending", "quota_release_and_readmission", "no_pods_or_jobs", "uid_fenced_cleanup"],
        "fixtures": [{"collection": path, "object": obj} for path, obj in fixtures(run_id)],
        "fixture_inputs": {"pool_policy_metadata": "synthetic, not real GPUPool/GPUAccessPolicy objects", "gpu_quota": "0", "cpu_quota": "1", "memory_quota": "64Mi", "protected_selector": "synthetic, never applied to any node"},
        "unverified": UNVERIFIED, "production_pass": False,
        "never": ["install", "apply", "patch", "create Pod or Job", "GPU request", "node write", "Secret read", "RBAC grant", "impersonation", "operator deployment", "finalizer removal"],
    }


class Budget:
    def __init__(self, seconds, clock=time.monotonic):
        self.clock = clock
        self.end = clock() + seconds
        self.calls = 0

    def remaining(self):
        remaining = self.end - self.clock()
        require(remaining > 0, "time_budget")
        return remaining

    def call_timeout(self):
        require(self.calls < MAX_CALLS, "call_budget")
        self.calls += 1
        return min(REQUEST_SECONDS, self.remaining())


def bounded_process(argv, payload, timeout, *, pass_fds=()):
    """No shell, raw errors, unbounded capture, or output on disk. Kill on limits."""
    require(len(payload or b"") <= MAX_INPUT, "request_size")
    process = None
    try:
        environment = {key: value for key, value in os.environ.items() if key.lower() not in {"kubeconfig", "kuberc", "http_proxy", "https_proxy", "all_proxy", "no_proxy"}}
        # kuberc defaults/aliases must not override the reviewed command.
        environment["KUBECTL_KUBERC"] = "false"
        process = subprocess.Popen(argv, stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL,
                                   stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, start_new_session=True, env=environment, pass_fds=pass_fds)
        # Input is at most 64 KiB. Nonblocking I/O bounds both directions, even
        # if kubectl/credential helpers stop consuming stdin or never exit.
        selector = selectors.DefaultSelector()
        os.set_blocking(process.stdout.fileno(), False)
        selector.register(process.stdout, selectors.EVENT_READ, "read")
        if payload is not None:
            os.set_blocking(process.stdin.fileno(), False)
            selector.register(process.stdin, selectors.EVENT_WRITE, "write")
        output = bytearray()
        sent = 0
        deadline = time.monotonic() + timeout
        with selector:
            while selector.get_map():
                remaining = deadline - time.monotonic()
                require(remaining > 0, "request_timeout")
                for key, _ in selector.select(remaining):
                    if key.data == "write":
                        try:
                            sent += os.write(key.fd, payload[sent:])
                        except BrokenPipeError:
                            sent = len(payload)
                        if sent == len(payload):
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                    else:
                        chunk = os.read(key.fd, 65536)
                        if not chunk:
                            selector.unregister(key.fileobj)
                        else:
                            output.extend(chunk)
                            require(len(output) <= MAX_BYTES, "response_size")
            remaining = deadline - time.monotonic()
            require(remaining > 0, "request_timeout")
            try:
                code = process.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                raise Rejected("request_timeout") from None
        require(code == 0, "kubectl_failed")
        return bytes(output)
    except OSError:
        raise Rejected("kubectl_unavailable") from None
    finally:
        if process is not None:
            # Also kill descendants when the leader already exited but a
            # helper inherited stdout. Never leave an unbounded child behind.
            import signal
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()


class PinnedConfig:
    """Private JSON input copied to an unlinked, inherited read-only descriptor.

    No ambient kubeconfig, auth execution, proxy, insecure TLS, credential file
    reference or token. Only kind's embedded CA and client certificate/key.
    The descriptor is fixed for the entire run, including cleanup.
    """
    def __init__(self, path, target):
        self.fd = None
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        source = os.open(path, flags)
        with os.fdopen(source, "rb") as stream:
            info = os.fstat(stream.fileno())
            require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077 and info.st_size <= MAX_INPUT, "kubeconfig_not_private")
            raw = stream.read(MAX_INPUT + 1)
        require(len(raw) <= MAX_INPUT, "kubeconfig_size")
        config = strict_json(raw)
        require(set(config) <= {"apiVersion", "kind", "preferences", "clusters", "contexts", "current-context", "users"} and config.get("apiVersion") == "v1" and config.get("kind") == "Config" and config.get("preferences", {}) == {}, "kubeconfig_shape")
        require(config.get("current-context") == target.context, "kubeconfig_context")
        values = {}
        for plural, singular in (("clusters", "cluster"), ("contexts", "context"), ("users", "user")):
            entries = config.get(plural)
            require(isinstance(entries, list) and len(entries) == 1 and isinstance(entries[0], dict) and set(entries[0]) == {"name", singular} and entries[0]["name"] == CONTEXT and isinstance(entries[0][singular], dict), "kubeconfig_shape")
            values[singular] = entries[0][singular]
        require(values["context"] == {"cluster": CONTEXT, "user": CONTEXT}, "kubeconfig_context")
        require(set(values["cluster"]) == {"server", "certificate-authority-data"} and values["cluster"]["server"] == target.server, "kubeconfig_cluster")
        require(set(values["user"]) == {"client-certificate-data", "client-key-data"}, "kubeconfig_credentials")
        for value, key_material in ((values["cluster"]["certificate-authority-data"], False), (values["user"]["client-certificate-data"], False), (values["user"]["client-key-data"], True)):
            require(isinstance(value, str) and 0 < len(value) < MAX_INPUT, "kubeconfig_credentials")
            try:
                decoded = base64.b64decode(value, validate=True)
            except (ValueError, binascii.Error):
                raise Rejected("kubeconfig_credentials") from None
            if key_material:
                require(decoded.startswith((b"-----BEGIN PRIVATE KEY-----", b"-----BEGIN RSA PRIVATE KEY-----", b"-----BEGIN EC PRIVATE KEY-----")), "kubeconfig_credentials")
            else:
                require(decoded.startswith(b"-----BEGIN CERTIFICATE-----"), "kubeconfig_credentials")
        # GitHub Linux kind target only. Reopening /proc/self/fd shares the
        # unlinked inode rather than resolving a mutable kubeconfig pathname.
        with tempfile.TemporaryDirectory(prefix="allotforge-acceptance-") as directory:
            snapshot = Path(directory) / "config.json"
            descriptor = os.open(snapshot, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
            self.fd = os.open(snapshot, os.O_RDONLY)
        self.path = "/proc/self/fd/" + str(self.fd)
        require(Path(self.path).exists(), "kubeconfig_descriptor_unavailable")

    def close(self):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def object_path(collection, obj):
    return collection + "/" + obj["metadata"]["name"]


class Kubectl:
    """Closed command vocabulary. No user-provided kubectl arguments or API paths."""
    def __init__(self, target, config, process=bounded_process):
        self.target = target
        self.config = config
        self.process = process
        self.desired = fixtures(target.run_id)
        self.paths = {object_path(collection, obj): (collection, obj) for collection, obj in self.desired}
        self.reads = {"/api/v1/namespaces/kube-system": ("namespaces", "kube-system", None),
                      "/api/v1/nodes": ("nodes", None, None),
                      "/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager": ("deployments.apps", "kueue-controller-manager", "kueue-system")}
        self.reads["/api/v1/namespaces/" + target.namespace + "/pods"] = ("pods", None, target.namespace)
        self.reads["/apis/batch/v1/namespaces/" + target.namespace + "/jobs"] = ("jobs.batch", None, target.namespace)
        plurals = {"Namespace": "namespaces", "ResourceFlavor": "resourceflavors.kueue.x-k8s.io", "ClusterQueue": "clusterqueues.kueue.x-k8s.io", "LocalQueue": "localqueues.kueue.x-k8s.io", "Workload": "workloads.kueue.x-k8s.io"}
        for path, (_, obj) in self.paths.items():
            self.reads[path] = (plurals[obj["kind"]], obj["metadata"]["name"], obj["metadata"].get("namespace"))

    def _call(self, args, budget, body=None):
        data = None if body is None else json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        argv = ["kubectl", "--kubeconfig=" + self.config.path, "--context=" + CONTEXT, "--request-timeout=" + str(REQUEST_SECONDS) + "s", *args]
        return self.process(argv, data, budget.call_timeout(), pass_fds=(self.config.fd,))

    def server(self, budget):
        raw = self._call(["config", "view", "--minify", "--output=jsonpath={.clusters[0].cluster.server}"], budget)
        require(len(raw) < 256, "server_refused")
        try:
            return raw.decode("ascii")
        except UnicodeError:
            raise Rejected("server_refused") from None

    def get(self, path, budget):
        if path in ("/version", ROOT):
            return strict_json(self._call(["get", "--raw=" + path], budget))
        require(path in self.reads, "get_not_allowed")
        resource, name, namespace = self.reads[path]
        args = ["get", resource]
        if name is not None:
            args.extend([name, "--ignore-not-found=true"])
        else:
            args.append("--chunk-size=0")
        if namespace:
            args.append("--namespace=" + namespace)
        raw = self._call([*args, "--output=json"], budget)
        return strict_json(raw) if raw else None

    def create(self, collection, body, budget):
        path = object_path(collection, body)
        require(path in self.paths, "create_not_allowed")
        expected = deepcopy(self.paths[path][1])
        actual = deepcopy(body)
        owner = actual["metadata"].pop("ownerReferences", None)
        if body["kind"] not in ("Namespace", "Workload"):
            require(isinstance(owner, list) and len(owner) == 1, "owner_invalid")
            require(owner[0].keys() == {"apiVersion", "kind", "name", "uid"} and owner[0]["apiVersion"] == "v1" and owner[0]["kind"] == "Namespace" and owner[0]["name"] == self.target.namespace and UID.fullmatch(owner[0]["uid"]), "owner_invalid")
        else:
            require(owner is None, "owner_invalid")
        require(actual == expected, "create_payload_refused")
        return strict_json(self._call(["create", "--filename=-", "--output=json"], budget, body))

    def delete(self, path, uid, resource_version, budget):
        require(path in self.paths and isinstance(uid, str) and UID.fullmatch(uid) and isinstance(resource_version, str) and re.fullmatch(r"[0-9]{1,30}", resource_version), "delete_not_allowed")
        body = {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {"uid": uid, "resourceVersion": resource_version}, "propagationPolicy": "Foreground"}
        raw = self._call(["delete", "--raw=" + path, "--filename=-"], budget, body)
        if raw:
            strict_json(raw)


def metadata(obj, kind=None, name=None, namespace=None):
    require(isinstance(obj, dict) and isinstance(obj.get("metadata"), dict), "object_shape")
    meta = obj["metadata"]
    if kind is not None:
        require(obj.get("kind") == kind, "object_identity")
    if name is not None:
        require(meta.get("name") == name and meta.get("namespace") == namespace, "object_identity")
    require(isinstance(meta.get("uid"), str) and UID.fullmatch(meta["uid"]), "object_uid")
    require(isinstance(meta.get("resourceVersion"), str) and re.fullmatch(r"[0-9]{1,30}", meta["resourceVersion"]), "resource_version")
    return meta


def current_condition(obj, condition, state):
    meta = obj.get("metadata", {})
    generation = meta.get("generation")
    require(type(generation) is int and generation > 0, "generation_invalid")
    status = obj.get("status", {})
    require(isinstance(status, dict), "status_shape")
    conditions = status.get("conditions", [])
    require(isinstance(conditions, list) and len(conditions) <= 64, "conditions_shape")
    types = set()
    match = None
    for item in conditions:
        require(isinstance(item, dict) and isinstance(item.get("type"), str) and item["type"] not in types, "condition_duplicate_or_shape")
        types.add(item["type"])
        require(item.get("status") in ("True", "False", "Unknown"), "condition_state")
        if item["type"] == condition:
            match = item
    return bool(match and match.get("status") == state and type(match.get("observedGeneration")) is int and match["observedGeneration"] == generation)


def items(obj):
    require(isinstance(obj, dict) and isinstance(obj.get("items"), list) and len(obj["items"]) <= 100, "list_shape")
    require(isinstance(obj.get("metadata", {}), dict) and not obj.get("metadata", {}).get("continue"), "list_incomplete")
    require(all(isinstance(item, dict) for item in obj["items"]), "list_shape")
    return obj["items"]


def quantities(actual, expected):
    from davano.quantities import decimal_quantity
    require(isinstance(actual, dict) and set(actual) == set(expected), "reservation_resources")
    try:
        require(all(decimal_quantity(actual[key]) == decimal_quantity(value) for key, value in expected.items()), "reservation_quantity")
    except (ValueError, TypeError, ArithmeticError):
        raise Rejected("reservation_quantity") from None


def contains_spec(actual, expected):
    """Admission may add API defaults but cannot change any submitted field."""
    if isinstance(expected, dict):
        return isinstance(actual, dict) and all(key in actual and contains_spec(actual[key], value) for key, value in expected.items())
    if isinstance(expected, list):
        return isinstance(actual, list) and len(actual) == len(expected) and all(contains_spec(a, e) for a, e in zip(actual, expected))
    return type(actual) is type(expected) and actual == expected


def source_identity():
    root = Path(__file__).resolve().parents[1]
    profile_bytes = (root / "acceptance/profile.json").read_bytes()
    profile = strict_json(profile_bytes)
    require(profile.get("kind", {}).get("context") == CONTEXT and profile["kind"].get("kubernetes_version") == KUBERNETES and profile.get("kueue", {}).get("image") == KUEUE_IMAGE and profile["kueue"].get("api_version") == "v1beta2", "profile_mismatch")
    commit = bounded_process(["git", "-C", str(root), "rev-parse", "--verify", "HEAD"], None, REQUEST_SECONDS).decode("ascii").strip()
    require(re.fullmatch(r"[0-9a-f]{40}", commit), "source_commit_invalid")
    dirty = bounded_process(["git", "-C", str(root), "status", "--porcelain", "--untracked-files=normal", "--", "acceptance", "davano"], None, REQUEST_SECONDS)
    sources = sorted((root / "davano").glob("*.py")) + sorted((root / "acceptance").glob("*.py"))
    hashes = {str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources}
    return {"git_commit": commit, "source_tree_dirty": bool(dirty), "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(), "source_sha256": hashes}


class Runner:
    def __init__(self, target, transport, clock=time.monotonic, sleep=time.sleep):
        self.target, self.api, self.clock, self.sleep = target, transport, clock, sleep
        self.desired = fixtures(target.run_id)
        self.created = []
        self.uncertain_create = False
        self.report = {"schema": "allotforge.cluster-acceptance.evidence.v1", "status": "failed", "scope": "generated_queue_specs_and_standalone_cpu_workloads", "production_pass": False,
                       "run_id": target.run_id, "target_identity_sha256": hashlib.sha256((target.server + "\n" + target.kube_system_uid).encode()).hexdigest(),
                       "kubernetes_version": KUBERNETES, "kueue_version": KUEUE_VERSION, "kueue_api": API,
                       "fixture_inputs": {"metadata": "synthetic pool/policy identities", "gpu_quota": "0", "cpu_quota": "1", "memory_quota": "64Mi", "protected_node_selector": "synthetic and not written to nodes"},
                       "observations": [], "stage": "source_provenance", "cleanup": {"status": "not_needed", "deleted": 0}, "unverified": list(UNVERIFIED)}

    def preflight(self, budget):
        require(self.api.server(budget) == self.target.server, "server_mismatch")
        version = self.api.get("/version", budget)
        require(isinstance(version, dict) and version.get("gitVersion") == KUBERNETES, "version_mismatch")
        system = self.api.get("/api/v1/namespaces/kube-system", budget)
        meta = metadata(system, "Namespace", "kube-system")
        require(meta["uid"] == self.target.kube_system_uid and not meta.get("deletionTimestamp"), "target_uid_mismatch")
        require(meta.get("labels", {}).get(DISPOSABLE_LABEL) == "cluster-acceptance", "disposable_marker_missing")
        nodes = items(self.api.get("/api/v1/nodes", budget))
        require(len(nodes) == 1, "kind_nodes_refused")
        node = nodes[0]
        node_meta = metadata(node, "Node", "allotforge-acceptance-control-plane")
        require("node-role.kubernetes.io/control-plane" in node_meta.get("labels", {}) and not node_meta.get("deletionTimestamp"), "kind_nodes_refused")
        status = node.get("status", {})
        require(isinstance(status, dict) and status.get("nodeInfo", {}).get("kubeletVersion") == KUBERNETES, "node_version_mismatch")
        for field in ("capacity", "allocatable"):
            resources = status.get(field)
            require(isinstance(resources, dict) and "cpu" in resources and "memory" in resources, "node_resources_unknown")
            require(not any(key.startswith(("nvidia.com/", "amd.com/", "gpu.intel.com/")) for key in resources), "gpu_target_refused")
        discovery = self.api.get(ROOT, budget)
        require(isinstance(discovery, dict) and discovery.get("groupVersion") == API and isinstance(discovery.get("resources"), list), "kueue_api_missing")
        seen = set()
        for entry in discovery["resources"]:
            require(isinstance(entry, dict) and isinstance(entry.get("name"), str) and entry["name"] not in seen, "discovery_invalid")
            seen.add(entry["name"])
        require({"resourceflavors", "clusterqueues", "localqueues", "workloads"} <= seen, "kueue_resources_missing")
        deployment = self.api.get("/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager", budget)
        dm = metadata(deployment, "Deployment", "kueue-controller-manager", "kueue-system")
        require(not dm.get("deletionTimestamp"), "kueue_controller_unready")
        ds, spec = deployment.get("status", {}), deployment.get("spec", {})
        require(type(dm.get("generation")) is int and dm["generation"] > 0 and type(ds.get("observedGeneration")) is int and ds["observedGeneration"] == dm["generation"], "kueue_controller_unready")
        require(type(spec.get("replicas")) is int and spec["replicas"] == 1 and all(type(ds.get(key)) is int and ds[key] == 1 for key in ("updatedReplicas", "readyReplicas", "availableReplicas", "replicas")), "kueue_controller_unready")
        containers = spec.get("template", {}).get("spec", {}).get("containers", [])
        require(isinstance(containers, list) and len(containers) == 1 and containers[0].get("name") == "manager" and containers[0].get("image") == KUEUE_IMAGE, "kueue_image_mismatch")
        for collection, desired in self.desired:
            require(self.api.get(object_path(collection, desired), budget) is None, "target_already_exists")
        self.report["observations"].append({"case": "target_guards", "status": "passed", "source": "live_api_and_explicit_context", "reported_gpu_resources": "absent"})

    def owned(self, actual, desired, uid=None):
        wanted = desired["metadata"]
        meta = metadata(actual, desired["kind"], wanted["name"], wanted.get("namespace"))
        require(actual.get("apiVersion") == desired["apiVersion"], "object_api")
        require(uid is None or meta["uid"] == uid, "ownership_changed")
        require(all(meta.get("labels", {}).get(key) == value for key, value in wanted["labels"].items()), "ownership_changed")
        require(meta.get("ownerReferences") == wanted.get("ownerReferences"), "ownership_changed")
        if "spec" in desired:
            require(contains_spec(actual.get("spec"), desired["spec"]), "fixture_spec_changed")
        return meta

    def create(self, collection, desired, budget):
        # Do not retry a failed POST. The server may have accepted it. Unknown
        # outcomes retain the namespace rather than cascading an unknown object.
        self.uncertain_create = True
        actual = self.api.create(collection, desired, budget)
        meta = self.owned(actual, desired)
        self.created.append((object_path(collection, desired), deepcopy(desired), meta["uid"]))
        self.uncertain_create = False
        return actual

    def wait_for(self, path, desired, uid, predicate, budget):
        while True:
            actual = self.api.get(path, budget)
            meta = self.owned(actual, desired, uid)
            require(not meta.get("deletionTimestamp"), "resource_terminating")
            require(type(meta.get("generation")) is int and meta["generation"] == 1, "fixture_generation_changed")
            if predicate(actual):
                return actual
            self.sleep(min(POLL_SECONDS, budget.remaining()))

    def admitted(self, actual, cpu="100m"):
        if not current_condition(actual, "QuotaReserved", "True") or not current_condition(actual, "Admitted", "True"):
            return False
        status = actual["status"]
        admission = status.get("admission")
        require(isinstance(admission, dict) and admission.get("clusterQueue") == "dgp-" + self.target.run_id, "reservation_queue")
        assignments = admission.get("podSetAssignments")
        require(isinstance(assignments, list) and len(assignments) == 1 and isinstance(assignments[0], dict), "reservation_podsets")
        assignment = assignments[0]
        require(assignment.get("name") == "main" and type(assignment.get("count")) is int and assignment["count"] == 1, "reservation_podsets")
        require(assignment.get("flavors") == {"cpu": "dgp-" + self.target.run_id, "memory": "dgp-" + self.target.run_id}, "reservation_flavors")
        quantities(assignment.get("resourceUsage"), {"cpu": cpu, "memory": "16Mi"})
        return True

    def pending(self, actual):
        require(not actual.get("status", {}).get("admission"), "over_quota_admitted")
        require(not current_condition(actual, "Admitted", "True"), "over_quota_admitted")
        return current_condition(actual, "QuotaReserved", "False")

    def no_execution(self, budget):
        for path in ("/api/v1/namespaces/" + self.target.namespace + "/pods", "/apis/batch/v1/namespaces/" + self.target.namespace + "/jobs"):
            require(not items(self.api.get(path, budget)), "unexpected_pod_or_job")

    def wait_queues(self, cpu, pending, budget):
        for entry in self.created:
            path, desired, uid = entry
            if desired["kind"] not in ("ClusterQueue", "LocalQueue"):
                continue
            def accounted(actual):
                if not current_condition(actual, "Active", "True"):
                    return False
                status = actual["status"]
                if any(type(status.get(key)) is not int or status[key] != count for key, count in (("reservingWorkloads", 1), ("admittedWorkloads", 1), ("pendingWorkloads", pending))):
                    return False
                # Queue status can lag Workload admission. Wait rather than
                # treating an earlier controller snapshot as success.
                flavors = status.get("flavorsReservation", [])
                require(isinstance(flavors, list) and len(flavors) <= 1, "queue_reservation_shape")
                if not flavors:
                    return False
                flavor = flavors[0]
                require(isinstance(flavor, dict) and flavor.get("name") == "dgp-" + self.target.run_id and isinstance(flavor.get("resources"), list), "queue_reservation_shape")
                amounts = {}
                for resource in flavor["resources"]:
                    require(isinstance(resource, dict) and resource.get("name") in ("cpu", "memory", "nvidia.com/gpu") and resource["name"] not in amounts, "queue_reservation_shape")
                    amounts[resource["name"]] = resource.get("total")
                # Upstream may omit zero-usage GPU from reservation status.
                if "nvidia.com/gpu" in amounts:
                    quantities({"nvidia.com/gpu": amounts.pop("nvidia.com/gpu")}, {"nvidia.com/gpu": "0"})
                try:
                    quantities(amounts, {"cpu": cpu, "memory": "16Mi"})
                except Rejected:
                    return False
                return True
            self.wait_for(path, desired, uid, accounted, budget)

    def delete_one(self, entry, budget):
        path, desired, uid = entry
        actual = self.api.get(path, budget)
        if actual is None:
            return
        meta = self.owned(actual, desired, uid)
        if desired["kind"] == "Namespace":
            # A foreign workload appearing in our fresh namespace is a stop
            # signal, never permission to cascade-delete someone else's work.
            self.no_execution(budget)
        if not meta.get("deletionTimestamp"):
            self.api.delete(path, uid, meta["resourceVersion"], budget)
        while True:
            actual = self.api.get(path, budget)
            if actual is None:
                return
            self.owned(actual, desired, uid)
            self.sleep(min(POLL_SECONDS, budget.remaining()))

    def cleanup(self):
        if not self.created and not self.uncertain_create:
            return
        budget = Budget(CLEANUP_SECONDS, self.clock)
        self.report["cleanup"] = {"status": "incomplete", "deleted": 0}
        try:
            for path, desired, uid in reversed(self.created):
                if desired["kind"] == "Namespace" and self.uncertain_create:
                    raise Rejected("create_outcome_uncertain")
                self.delete_one((path, desired, uid), budget)
                self.report["cleanup"]["deleted"] += 1
            require(not self.uncertain_create, "create_outcome_uncertain")
            self.report["cleanup"]["status"] = "complete"
        except Rejected as error:
            self.report["cleanup"]["reason"] = str(error)
        except Exception:
            self.report["cleanup"]["reason"] = "cleanup_response_invalid"

    def run(self):
        budget = Budget(RUN_SECONDS, self.clock)
        success = False
        try:
            self.report["source"] = source_identity()
            self.report["stage"] = "target_preflight"
            self.preflight(budget)
            ns_collection, namespace = self.desired[0]
            self.report["stage"] = "create_namespace"
            ns = self.create(ns_collection, namespace, budget)
            owner = {"apiVersion": "v1", "kind": "Namespace", "name": self.target.namespace, "uid": ns["metadata"]["uid"]}
            for collection, desired in self.desired[1:]:
                if desired["kind"] != "Workload":
                    desired["metadata"]["ownerReferences"] = [owner]
                self.report["stage"] = "create_" + desired["kind"].lower()
                created = self.create(collection, desired, budget)
                predicate = None
                case = None
                if desired["kind"] in ("ClusterQueue", "LocalQueue"):
                    predicate = lambda actual: current_condition(actual, "Active", "True")
                    case = desired["kind"].lower() + "_active"
                elif desired["kind"] == "Workload":
                    predicate = self.admitted if desired["metadata"]["name"] == "cpu-admitted" else self.pending
                    case = "cpu_quota_admitted" if desired["metadata"]["name"] == "cpu-admitted" else "cpu_quota_pending"
                if predicate:
                    self.report["stage"] = case
                    actual = self.wait_for(object_path(collection, desired), desired, created["metadata"]["uid"], predicate, budget)
                    self.report["observations"].append({"case": case, "status": "passed", "source": "live_kueue_controller_status", "observed_generation": actual["metadata"]["generation"]})
            self.report["stage"] = "quota_release_and_readmission"
            self.no_execution(budget)
            # Confirm queue accounting before releasing the first reservation.
            self.wait_queues("100m", pending=1, budget=budget)
            first = next(entry for entry in self.created if entry[1]["metadata"]["name"] == "cpu-admitted")
            self.delete_one(first, budget)
            second = next(entry for entry in self.created if entry[1]["metadata"]["name"] == "cpu-pending")
            self.wait_for(*second, lambda obj: self.admitted(obj, "1"), budget)
            self.wait_queues("1", pending=0, budget=budget)
            self.no_execution(budget)
            self.report["observations"].extend([
                {"case": "quota_release_and_readmission", "status": "passed", "source": "live_kueue_controller_status", "admitted_cpu": "1", "memory": "16Mi"},
                {"case": "no_pods_or_jobs", "status": "passed", "source": "live_namespace_lists", "observation": "empty_at_checkpoints_not_continuous_audit"}])
            success = True
        except Rejected as error:
            self.report["failure"] = str(error)
        except Exception:
            self.report["failure"] = "response_or_runner_invalid"
        finally:
            self.cleanup()
        if success and self.report["cleanup"]["status"] == "complete":
            self.report["status"] = "passed_bounded_cpu_integration"
            self.report["stage"] = "complete"
        return self.report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--plan", action="store_true")
    mode.add_argument("--execute", action="store_true")
    parser.add_argument("--context")
    parser.add_argument("--kubeconfig-json", type=Path)
    parser.add_argument("--expected-server")
    parser.add_argument("--expected-kube-system-uid")
    parser.add_argument("--expected-kubernetes-version")
    parser.add_argument("--run-id")
    parser.add_argument("--evidence", type=Path)
    args = parser.parse_args(argv)
    # Must be selected before importing reconciler, which supports beta1 and
    # beta2. Conflicting environment/imports are refused, never silently reset.
    os.environ.setdefault("DAVANO_KUEUE_API_VERSION", "v1beta2")
    evidence = None
    config = None
    try:
        if args.plan:
            require(not any((args.context, args.expected_server, args.expected_kube_system_uid, args.expected_kubernetes_version, args.evidence, args.kubeconfig_json)), "plan_execution_arguments")
            print(json.dumps(plan(args.run_id or "000000000000"), indent=2, allow_nan=False))
            return 0
        require(all((args.context, args.expected_server, args.expected_kube_system_uid, args.expected_kubernetes_version, args.run_id, args.evidence, args.kubeconfig_json)), "explicit_execution_arguments_required")
        target = Target(args.context, args.expected_server, args.expected_kube_system_uid, args.expected_kubernetes_version, args.run_id)
        config = PinnedConfig(args.kubeconfig_json, target)
        transport = Kubectl(target, config)
        # Reserve evidence output before ANY cluster command. Refuse overwrite
        # and symlinks. No kubeconfig, token, raw body or stderr is persisted.
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        evidence = os.fdopen(os.open(args.evidence, flags, 0o600), "w", encoding="utf-8")
        report = Runner(target, transport).run()
        json.dump(report, evidence, indent=2, allow_nan=False)
        evidence.write("\n")
        evidence.flush()
        os.fsync(evidence.fileno())
        print(json.dumps({"status": report["status"], "cleanup": report["cleanup"]["status"], "production_pass": False}))
        return 0 if report["status"] == "passed_bounded_cpu_integration" else 1
    except Rejected as error:
        print(json.dumps({"status": "refused", "reason": str(error), "production_pass": False}))
        return 2
    except (OSError, ValueError, UnicodeError):
        print(json.dumps({"status": "failed", "reason": "local_evidence_or_configuration_error", "production_pass": False}))
        return 2
    finally:
        if evidence is not None:
            evidence.close()
        if config is not None:
            config.close()


if __name__ == "__main__":
    sys.exit(main())
