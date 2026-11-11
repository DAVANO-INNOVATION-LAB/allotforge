"""Normalize advertised Kubernetes resources, never fabricate device telemetry."""
import json
import os
import re
import ssl
from collections import defaultdict
from copy import deepcopy
from decimal import Decimal, DecimalException, localcontext
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, HTTPRedirectHandler, HTTPSHandler, build_opener
from .store import now


class InventoryUnavailable(RuntimeError):
    pass


def pod_phase(pod):
    value = pod.get("status", {}).get("phase")
    return value if isinstance(value, str) and value in ("Pending", "Running", "Succeeded", "Failed", "Unknown") else "Unknown"


def gpu_vendor(key):
    if key == "nvidia.com/gpu" or key.startswith("nvidia.com/mig-") or key == "nvidia.com/gpu.shared":
        return "NVIDIA"
    if key in ("amd.com/gpu", "amd.com/gpu.shared"):
        return "AMD"
    if key in ("gpu.intel.com/i915", "gpu.intel.com/xe"):
        return "Intel"
    return None


def quantity(value):
    """K8s Quantity to nonnegative integer; ambiguous/non-integral fails closed."""
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return None
    raw = str(value)
    if len(raw) > 100:
        return None
    match = re.fullmatch(r"([+]?[0-9]+(?:\.[0-9]+)?)([numkMGTPE]|[KMGTPE]i|[eE][+-]?[0-9]+)?", raw)
    if not match:
        return None
    suffix = match[2] or ""
    scales = {"": 1, "n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal("1e-3"), "k": 1000}
    scales.update({x: 1000 ** (i + 2) for i, x in enumerate("MGTPE")})
    scales.update({x + "i": 1024 ** (i + 1) for i, x in enumerate("KMGTPE")})
    try:
        with localcontext() as context:
            # Preserve every bounded input digit; the default Decimal precision
            # can round a fractional GPU request into an apparently whole GPU.
            context.prec = 160
            if suffix.startswith(("e", "E")) and len(suffix) > 1:
                exponent = int(suffix[1:])
                if not -30 <= exponent <= 30:
                    return None
                multiplier = Decimal(10) ** exponent
            else:
                multiplier = scales[suffix]
            result = Decimal(match[1]) * multiplier
            if not result.is_finite() or not 0 <= result <= 1_000_000 or (result == 0 and Decimal(match[1]) != 0):
                return None
            return int(result) if result == int(result) else None
    except (DecimalException, KeyError, ValueError, OverflowError):
        return None


def resource_requests(container):
    if not isinstance(container, dict):
        raise InventoryUnavailable("Malformed Pod container; refusing incomplete resource accounting.")
    resources = container.get("resources", {})
    if not isinstance(resources, dict):
        raise InventoryUnavailable("Malformed Pod resource map; refusing incomplete resource accounting.")
    requests = resources.get("requests", {})
    limits = resources.get("limits", {})
    if not isinstance(requests, dict) or not isinstance(limits, dict):
        raise InventoryUnavailable("Malformed Pod requests or limits; refusing incomplete resource accounting.")
    return {k: quantity(requests.get(k, limits.get(k))) for k in set(requests) | set(limits) if gpu_vendor(k)}


def pod_requests(pod):
    """Effective requests: app + restartable init sidecars, init peaks, overhead."""
    spec = pod.get("spec", {})
    apps, init_containers, pod_overhead = spec.get("containers", []), spec.get("initContainers", []), spec.get("overhead", {})
    if not isinstance(apps, list) or not isinstance(init_containers, list) or not isinstance(pod_overhead, dict):
        raise InventoryUnavailable("Malformed Pod containers or overhead; refusing incomplete resource accounting.")
    containers = [resource_requests(c) for c in apps]
    inits = [(resource_requests(c), c.get("restartPolicy") == "Always") for c in init_containers]
    overhead = {k: quantity(v) for k, v in pod_overhead.items() if gpu_vendor(k)}
    keys = set(overhead)
    for d in containers + [d for d, _ in inits]:
        keys.update(d)
    out = {}
    for key in keys:
        values = [d.get(key, 0) for d in containers + [d for d, _ in inits]] + [overhead.get(key, 0)]
        if any(v is None for v in values):
            out[key] = None
            continue
        sidecars = peak = 0
        for req, restartable in inits:
            count = req.get(key, 0)
            if restartable:
                sidecars += count
                peak = max(peak, sidecars)
            else:
                peak = max(peak, sidecars + count)
        out[key] = max(sum(d.get(key, 0) for d in containers) + sidecars, peak) + overhead.get(key, 0)
    return out


def allocation_mode(key, labels):
    vendor = gpu_vendor(key)
    if key.endswith(".shared"):
        return "shared"
    if vendor == "NVIDIA":
        product = labels.get("nvidia.com/gpu.product", "").upper()
        sharing = labels.get("nvidia.com/gpu.sharing-strategy", "")
        replicas = quantity(labels.get(key + ".replicas", labels.get("nvidia.com/gpu.replicas", "1")))
        if "SHARED" in product or sharing in ("time-slicing", "mps") or (replicas is not None and replicas > 1):
            return "shared"
        if key.startswith("nvidia.com/mig-") or labels.get("nvidia.com/mig.strategy") == "single" or "MIG" in product:
            return "mig"
        if replicas != 1 or sharing not in ("", "none"):
            return "unknown"
    # An administrator assertion is required for non-NVIDIA devices. This is
    # provenance, not physical-device discovery; conflicting plugin evidence wins.
    if labels.get("davano.dev/allocation-mode") == "whole-gpu":
        return "whole-gpu"
    if vendor == "NVIDIA" and labels.get("nvidia.com/gpu.sharing-strategy") == "none":
        return "whole-gpu"
    return "unknown"


class NoRedirectHandler(HTTPRedirectHandler):
    """Never forward the Kubernetes bearer token to a redirected destination."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def open_without_redirect(request, *, context, timeout):
    return build_opener(HTTPSHandler(context=context), NoRedirectHandler()).open(request, timeout=timeout)


def normalize(nodes, pods):
    from .observability import conditions, pending_reasons
    from .compute import compute_usage, node_compute
    compute_requested = compute_usage(pods)
    requested = defaultdict(lambda: defaultdict(int))
    workloads = []
    for pod in pods:
        phase = pod_phase(pod)
        if phase in ("Succeeded", "Failed"):
            continue
        reqs = pod_requests(pod)
        if not reqs:
            continue
        spec, meta = pod.get("spec", {}), pod.get("metadata", {})
        node_name = spec.get("nodeName")
        for key, count in reqs.items():
            if node_name:
                existing = requested[node_name][key]
                requested[node_name][key] = None if existing is None or count is None else existing + count
        for key, count in reqs.items():
            if count == 0:
                continue
            workloads.append({"id": f"{meta.get('namespace', 'default')}/{meta.get('name', 'unknown')}:{key}", "name": meta.get("name", "unknown"), "namespace": meta.get("namespace", "default"), "status": phase, "node": node_name, "gpu_count": count, "resource": key, "queue": meta.get("labels", {}).get("kueue.x-k8s.io/queue-name"), "origin": "observed-pod", "conditions": conditions(pod), "pending_reasons": pending_reasons(pod)})
    result = []
    for node in nodes:
        meta, status, spec = node.get("metadata", {}), node.get("status", {}), node.get("spec", {})
        labels, name = meta.get("labels", {}), meta.get("name", "unknown")
        ready = next((c.get("status") == "True" for c in status.get("conditions", []) if c.get("type") == "Ready"), False)
        allocatable, capacity = status.get("allocatable", {}), status.get("capacity", {})
        resources, warnings = [], []
        for key in sorted(set(allocatable) | set(capacity)):
            vendor = gpu_vendor(key)
            if not vendor:
                continue
            cap, alloc, req = quantity(capacity.get(key, 0)), quantity(allocatable.get(key, 0)), requested[name].get(key, 0)
            mode = allocation_mode(key, labels)
            available = None if alloc is None or req is None else max(0, alloc - req)
            if None in (cap, alloc, req):
                warnings.append(f"Invalid or unknown count for {key}; availability is not trusted.")
                available = None
            if alloc is not None and req is not None and req > alloc:
                warnings.append(f"Observed requests for {key} exceed allocatable resources.")
            if mode == "unknown":
                warnings.append(f"Allocation mode for {key} is unknown; excluded from whole-GPU evaluation.")
            resources.append({"name": key, "vendor": vendor, "mode": mode, "capacity": cap, "allocatable": alloc, "requested": req, "available": available})
        if not resources:
            continue
        whole = [r for r in resources if r["mode"] == "whole-gpu"]
        def total(field):
            return None if any(r[field] is None for r in whole) else sum(r[field] for r in whole)
        taints = [{"key": t.get("key"), "value": t.get("value"), "effect": t.get("effect")} for t in spec.get("taints", [])]
        blocking = any(t["effect"] in ("NoSchedule", "NoExecute") for t in taints)
        modes = set(r["mode"] for r in resources)
        vendors = set(r["vendor"] for r in resources)
        model = labels.get("nvidia.com/gpu.product") or labels.get("amd.com/gpu.product-name") or labels.get("davano.dev/model") or "Not reported"
        result.append({"id": name, "name": name, "status": "Ready" if ready else "NotReady", "schedulable": ready and not spec.get("unschedulable", False) and not blocking, "vendor": next(iter(vendors)) if len(vendors) == 1 else "Mixed", "model": model, "mode": next(iter(modes)) if len(modes) == 1 else "mixed", "gpu_capacity": total("capacity"), "gpu_allocatable": total("allocatable"), "gpu_requested": total("requested"), "gpu_available": total("available"), "zone": labels.get("topology.kubernetes.io/zone", "Not reported"), "labels": labels, "taints": taints, "resources": resources, "warnings": warnings, "pool_ids": [], "compute": node_compute(status, compute_requested.get(name, {}), warnings)})
    return result, workloads


class DemoInventory:
    source = "demo"
    def __init__(self, path):
        self.fixture = json.loads(Path(path).read_text())
    def snapshot(self):
        from .workload_advisor import workload_inventory
        nodes, workloads = normalize(deepcopy(self.fixture["nodes"]), deepcopy(self.fixture["pods"]))
        return {"nodes": nodes, "workloads": workloads, "workload_inventory": workload_inventory(self.fixture["pods"], self.fixture["nodes"]), "observed_at": now(), "warnings": ["Demo fixture, not a connected cluster. Counts are synthetic."]}


class KubernetesInventory:
    source = "kubernetes"
    def __init__(self, api=None, token_path=None, ca_path=None, opener=None):
        host = os.getenv("KUBERNETES_SERVICE_HOST", "")
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        self.api = (api or os.getenv("DAVANO_K8S_API") or (f"https://{host}:{os.getenv('KUBERNETES_SERVICE_PORT_HTTPS', '443')}" if host else "")).rstrip("/")
        parsed = urlparse(self.api)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path:
            raise InventoryUnavailable("Configure an HTTPS Kubernetes API origin, without embedded credentials or path.")
        self.token_path = token_path or os.getenv("DAVANO_K8S_TOKEN_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token")
        self.ca_path = ca_path or os.getenv("DAVANO_K8S_CA", "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        self.opener = opener or open_without_redirect

    def _list(self, path):
        items, continuation = [], ""
        for _ in range(100):
            query = {"limit": "500"}
            if continuation:
                query["continue"] = continuation
            try:
                token = Path(self.token_path).read_text().strip()  # Re-read rotating SA token.
                context = ssl.create_default_context(cafile=self.ca_path)
                req = Request(self.api + path + "?" + urlencode(query), headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, method="GET")
                with self.opener(req, context=context, timeout=10) as response:
                    raw = response.read(32 * 1024 * 1024 + 1)
                if len(raw) > 32 * 1024 * 1024:
                    raise InventoryUnavailable("Kubernetes response exceeded safety limit.")
                data = json.loads(raw)
                if not isinstance(data.get("items"), list):
                    raise InventoryUnavailable("Unexpected Kubernetes list response.")
                items.extend(data["items"])
                continuation = data.get("metadata", {}).get("continue", "")
                if not continuation:
                    return items
            except HTTPError as e:
                raise InventoryUnavailable(f"Kubernetes inventory read failed (HTTP {e.code}). Check read-only RBAC and connectivity.") from None
            except (OSError, URLError, ValueError) as e:
                raise InventoryUnavailable("Kubernetes inventory unavailable. Check HTTPS API, CA, service-account token, and connectivity.") from None
        raise InventoryUnavailable("Kubernetes pagination limit exceeded; refusing incomplete inventory.")

    def snapshot(self):
        # Nodes and pods are separate list snapshots, not an atomic allocation view.
        nodes = self._list("/api/v1/nodes")
        pods = self._list("/api/v1/pods")
        from .workload_advisor import workload_inventory
        normalized, workloads = normalize(nodes, pods)
        return {"nodes": normalized, "workloads": workloads, "workload_inventory": workload_inventory(pods, nodes), "observed_at": now(), "warnings": ["Read-only Kubernetes inventory. Node and pod lists are not an atomic snapshot.", "Unrequested resources are not physical GPU utilization or guaranteed schedulable capacity."]}
