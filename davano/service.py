import os
from concurrent.futures import ThreadPoolExecutor
from .inventory import InventoryUnavailable
from . import __version__
from .observability import ObservabilityService
from .workload_advisor import WorkloadAdvisor


def sum_known(items, key):
    values = [item[key] for item in items]
    return None if any(v is None for v in values) else sum(values)


def pool_resources(pool, node):
    if not all(node["labels"].get(k) == v for k, v in pool["selector"].items()):
        return []
    return [r for r in node["resources"] if r["mode"] == pool["mode"] and (pool["vendor"] == "Any" or r["vendor"] == pool["vendor"])]


class FleetService:
    def __init__(self, inventory, store, observability=None, workload_advisor=None):
        self.inventory, self.store = inventory, store
        self.observability = observability or ObservabilityService(inventory.source)
        self.workload_advisor = workload_advisor or WorkloadAdvisor(inventory.source)

    def state(self):
        snapshot = self.inventory.snapshot()
        nodes, workloads = snapshot["nodes"], snapshot["workloads"]
        policies = [{**p, "enforcement": "advisory-only"} for p in self.store.list("policies")]
        pools = []
        for pool in self.store.list("pools"):
            matches = [(node, pool_resources(pool, node)) for node in nodes]
            matches = [(node, resources) for node, resources in matches if resources]
            resources = [resource for _, rs in matches for resource in rs]
            warnings = []
            for node, _ in matches:
                node["pool_ids"].append(pool["id"])
                if not node["schedulable"]:
                    warnings.append(f"{node['id']} is not eligible for this conservative evaluation (readiness, cordon, or taints).")
            if not matches:
                warnings.append("No verified whole-GPU nodes match this selector and vendor.")
            pools.append({**pool, "node_ids": [n["id"] for n, _ in matches], "node_count": len(matches), "gpu_capacity": sum_known(resources, "capacity"), "gpu_requested": sum_known(resources, "requested"), "gpu_available": sum_known(resources, "available"), "policy_ids": [p["id"] for p in policies if p["pool_id"] == pool["id"]], "status": "advisory", "warnings": warnings})
        for pool in pools:
            overlap = [n["name"] for n in nodes if pool["id"] in n["pool_ids"] and len(n["pool_ids"]) > 1]
            if overlap:
                pool["warnings"].append("Overlapping membership; pools do not reserve resources: " + ", ".join(overlap))
        source = self.inventory.source
        # Independent bounded readers run together so the optional workload read
        # does not add a second Prometheus timeout to inventory refresh latency.
        if source == "kubernetes" and getattr(self.workload_advisor, "reader", None) is not None:
            with ThreadPoolExecutor(max_workers=2) as readers:
                observation_future = readers.submit(self.observability.snapshot, nodes, snapshot["observed_at"])
                advisor_future = readers.submit(self.workload_advisor.snapshot, snapshot.get("workload_inventory", []), snapshot["observed_at"])
                observability, workload_advisor = observation_future.result(), advisor_future.result()
        else:
            observability = self.observability.snapshot(nodes, snapshot["observed_at"])
            workload_advisor = self.workload_advisor.snapshot(snapshot.get("workload_inventory", []), snapshot["observed_at"])
        return {
            "meta": {"source": source, "source_label": "Demo fixture" if source == "demo" else "Live Kubernetes · read-only", "observed_at": snapshot["observed_at"], "read_only_cluster": True, "enforcement": "advisory-only", "version": __version__, "warnings": snapshot["warnings"] + ["Pool and policy configuration is local metadata. No scheduler enforcement or allocation is implemented."]},
            "summary": {"nodes": len(nodes), "ready_nodes": sum(n["status"] == "Ready" for n in nodes), "whole_gpu_capacity": sum_known(nodes, "gpu_capacity"), "whole_gpu_allocatable": sum_known(nodes, "gpu_allocatable"), "whole_gpu_requested": sum_known(nodes, "gpu_requested"), "whole_gpu_available": sum_known(nodes, "gpu_available"), "pending_workloads": len({(w["namespace"], w["name"]) for w in workloads if w["status"] == "Pending"}), "pool_count": len(pools)},
            "nodes": nodes, "pools": pools, "policies": policies, "workloads": workloads,
            "integrations": [
                {"id": "kubernetes", "name": "Kubernetes", "status": "demo" if source == "demo" else "connected-read-only", "detail": "Fixture inventory; no cluster connected" if source == "demo" else "Inventory adapter lists node and pod resources. Cluster writes, when enabled, use the separate authenticated control API."},
                {"id": "kueue", "name": "Kueue", "status": "configured" if os.getenv("DAVANO_CONTROL_ENABLED", "false").lower() == "true" else "available-disabled", "detail": "Optional controller reconciles owned ResourceFlavor, ClusterQueue, LocalQueue, quota and native admission guards. Inspect current-generation CRD conditions; configuration is not proof of enforcement."},
                {"id": "ray", "name": "Ray", "status": "planning-only", "detail": "Validated suspended RayJob manifest planning and export. No live RayJob submission, Ray cluster reconciliation or runtime execution."},
                {"id": "slurm", "name": "Slurm", "status": "not-implemented", "detail": "No Slurm inventory, partition management, or job submission."},
                {"id": "gpu-telemetry", "name": "GPU telemetry", "status": observability["telemetry"]["status"], "detail": observability["telemetry"]["detail"]}
            ],
            "observability": observability,
            "workload_advisor": workload_advisor,
            "events": self.store.events()
        }

    def evaluate(self, req):
        state = self.state()
        pool = next((p for p in state["pools"] if p["id"] == req.pool_id), None)
        if pool is None:
            raise KeyError("Pool not found")
        reasons, candidates = [], []
        policy_matches = [p for p in state["policies"] if p["pool_id"] == req.pool_id and p["namespace"] == req.namespace]
        blocked = False
        if not policy_matches:
            reasons.append("No matching local policy for this pool and namespace.")
            blocked = True
        else:
            cap = min(p["max_gpus_per_workload"] for p in policy_matches)
            if req.gpu_count > cap:
                reasons.append(f"Request exceeds the strictest local policy limit of {cap} GPUs per workload.")
                blocked = True
        if req.vendor != "Any" and pool["vendor"] not in ("Any", req.vendor):
            reasons.append("Requested vendor does not match the pool vendor.")
            blocked = True
        unknown = False
        node_checks = []
        for node in state["nodes"]:
            rejected, uncertain, notes = False, False, []
            if not node["schedulable"]:
                rejected = True
                notes.append("Node is not eligible: readiness, cordon, or blocking taints.")
            resources = [r for r in pool_resources(pool, node) if req.vendor == "Any" or r["vendor"] == req.vendor]
            matching = [r for r in resources if r["available"] is not None and r["available"] >= req.gpu_count]
            if not resources:
                rejected = True
                notes.append("No verified whole-GPU resource matches the pool selector and requested vendor.")
            elif not matching:
                if any(r["available"] is None for r in resources):
                    uncertain = True
                    notes.append("Whole-GPU availability is unknown.")
                else:
                    rejected = True
                    notes.append("Insufficient whole-GPU availability on this single node.")
            else:
                notes.append("Enough observed whole-GPU units on this node.")
            for dimension, label in (("cpu_millicores", "CPU millicores"), ("memory_mib", "memory MiB")):
                amount = getattr(req, dimension)
                available = node.get("compute", {}).get(dimension, {}).get("available")
                if amount is None:
                    notes.append(f"{label} not requested; not checked.")
                elif amount == 0:
                    notes.append(f"Zero {label} requested; no additional capacity required.")
                elif available is None:
                    uncertain = True
                    notes.append(f"{label} availability is unknown; cannot verify requested {amount}.")
                elif available < amount:
                    rejected = True
                    notes.append(f"Insufficient {label}: requested {amount}, observed available {available}.")
                else:
                    notes.append(f"{label} fits: requested {amount}, observed available {available}.")
            if blocked:
                rejected = True
                notes.append("Local policy or pool vendor checks block this request.")
            status = "rejected" if rejected else "unknown" if uncertain else "eligible"
            node_checks.append({"id": node["id"], "status": status, "reasons": notes})
            if status == "unknown":
                unknown = True
            elif status == "eligible":
                for resource in matching:
                    candidates.append({"id": node["id"], "available": resource["available"], "resource": resource["name"],
                                       "cpu_available_millicores": node.get("compute", {}).get("cpu_millicores", {}).get("available"),
                                       "memory_available_mib": node.get("compute", {}).get("memory_mib", {}).get("available")})
        if blocked:
            verdict = "blocked"
            candidates = []
        elif candidates:
            verdict = "fits"
            reasons.append("Observed requests leave enough whole-GPU units and any requested CPU/memory on at least one eligible node.")
        elif unknown:
            verdict = "unknown"
            reasons.append("Unknown resource counts prevent a safe fit result.")
        else:
            verdict = "blocked"
            reasons.append("No eligible single node has enough verified unrequested resources for the requested dimensions. Resources are never combined across nodes.")
        return {"verdict": verdict, "advisory_only": True, "allocated": False, "submitted": False, "source": state["meta"]["source"], "observed_at": state["meta"]["observed_at"], "gpu_count": req.gpu_count, "cpu_millicores": req.cpu_millicores, "memory_mib": req.memory_mib, "node_checks": node_checks, "pool_id": req.pool_id, "candidate_nodes": candidates, "policy_ids": [p["id"] for p in policy_matches], "reasons": reasons, "limitations": ["No resources were allocated and no workload was submitted.", "Advisory only: local policies do not enforce Kubernetes admission, priority, quota, or scheduling.", "CPU and memory are checked only when requested, using conservative rounded observed requests, not utilization. Does not check topology, affinity, storage, quotas, Pod count, or concurrent scheduling; any NoSchedule/NoExecute taint is conservatively excluded.", "Unrequested GPU resources are not utilization or an exclusive reservation."]}
