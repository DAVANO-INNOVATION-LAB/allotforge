"""Independent safety regression cases for GPU units and Kubernetes accounting."""
import unittest
from davano.inventory import allocation_mode, normalize, pod_requests, quantity

GPU = "nvidia.com/gpu"


def container(n, restart=False):
    result = {"resources": {"requests": {GPU: str(n)}}}
    if restart:
        result["restartPolicy"] = "Always"
    return result


def node(labels=None, taints=None, ready="True", alloc="8"):
    return {"metadata": {"name": "worker", "labels": labels or {}},
            "spec": {"taints": taints or []},
            "status": {"capacity": {GPU: "8"}, "allocatable": {GPU: alloc},
                       "conditions": [{"type": "Ready", "status": ready}]}}


def pod(phase, count, assigned=True):
    spec = {"containers": [container(count)]}
    if assigned:
        spec["nodeName"] = "worker"
    return {"metadata": {"name": "job", "namespace": "research"}, "spec": spec,
            "status": {"phase": phase}}


class InventorySafetyTests(unittest.TestCase):
    def test_resource_name_alone_is_not_whole_gpu_evidence(self):
        for resource in [GPU, "amd.com/gpu", "gpu.intel.com/xe", "gpu.intel.com/i915"]:
            self.assertEqual(allocation_mode(resource, {}), "unknown")

    def test_whole_assertion_cannot_override_mig_or_sharing(self):
        asserted = {"davano.dev/allocation-mode": "whole-gpu"}
        for evidence, expected in [({"nvidia.com/mig.strategy": "single"}, "mig"),
                                   ({"nvidia.com/gpu.product": "A100-MIG-1g.5gb"}, "mig"),
                                   ({"nvidia.com/gpu.replicas": "4"}, "shared"),
                                   ({"nvidia.com/gpu.sharing-strategy": "time-slicing"}, "shared"),
                                   ({"nvidia.com/gpu.sharing-strategy": "mps"}, "shared")]:
            self.assertEqual(allocation_mode(GPU, {**asserted, **evidence}), expected)

    def test_shared_mig_is_not_presented_as_exclusive_mig(self):
        self.assertEqual(allocation_mode("nvidia.com/mig-1g.10gb.shared", {}), "shared")
        self.assertEqual(allocation_mode("nvidia.com/mig-1g.10gb", {
            "nvidia.com/gpu.sharing-strategy": "time-slicing"}), "shared")

    def test_shared_product_marker_defeats_whole_assertion(self):
        self.assertEqual(allocation_mode(GPU, {"davano.dev/allocation-mode": "whole-gpu",
                         "nvidia.com/gpu.product": "NVIDIA-H100-80GB-HBM3-SHARED"}), "shared")

    def test_invalid_replica_evidence_does_not_prove_whole_device(self):
        self.assertEqual(allocation_mode(GPU, {"davano.dev/allocation-mode": "whole-gpu",
                         "nvidia.com/gpu.replicas": "not-a-count"}), "unknown")

    def test_mig_and_unknown_are_excluded_from_whole_aggregate(self):
        for labels in [{}, {"nvidia.com/mig.strategy": "single"},
                       {"nvidia.com/gpu.replicas": "8"}]:
            nodes, _ = normalize([node(labels)], [])
            self.assertEqual(nodes[0]["gpu_capacity"], 0)
            self.assertNotEqual(nodes[0]["resources"][0]["mode"], "whole-gpu")

    def test_assigned_pending_counts_but_unassigned_and_completed_do_not(self):
        inputs = [pod("Pending", 2), pod("Running", 1), pod("Succeeded", 8),
                  pod("Failed", 8), pod("Pending", 8, assigned=False)]
        nodes, workloads = normalize([node({"davano.dev/allocation-mode": "whole-gpu"})], inputs)
        self.assertEqual(nodes[0]["gpu_requested"], 3)
        self.assertEqual(nodes[0]["gpu_available"], 5)
        self.assertEqual(len(workloads), 3)

    def test_init_peak_includes_previous_restartable_sidecars(self):
        data = {"spec": {"containers": [container(2)], "initContainers": [
            container(1, restart=True), container(5), container(2, restart=True), container(4)]}}
        # Steady: 2+1+2=5. Initialization peak: 1+2+4=7.
        self.assertEqual(pod_requests(data)[GPU], 7)

    def test_init_peak_is_not_sum_of_all_ordinary_inits(self):
        data = {"spec": {"containers": [container(2)],
                         "initContainers": [container(4), container(3)]}}
        self.assertEqual(pod_requests(data)[GPU], 4)

    def test_requests_override_limits_and_limits_fill_only_absent_requests(self):
        data = {"spec": {"containers": [{"resources": {
            "requests": {GPU: "2"}, "limits": {GPU: "8"}}}]}}
        self.assertEqual(pod_requests(data)[GPU], 2)
        del data["spec"]["containers"][0]["resources"]["requests"]
        self.assertEqual(pod_requests(data)[GPU], 8)

    def test_invalid_gpu_quantity_is_unknown_and_never_available(self):
        for invalid in ["0.5", "-1", "hello", True, "2.3Gi"]:
            self.assertIsNone(quantity(invalid))
        nodes, _ = normalize([node({"davano.dev/allocation-mode": "whole-gpu"}, alloc="garbage")], [])
        self.assertIsNone(nodes[0]["gpu_available"])

    def test_node_ready_is_not_enough_to_bypass_taints_or_unknown_readiness(self):
        nodes, _ = normalize([node({"davano.dev/allocation-mode": "whole-gpu"},
                                  taints=[{"key": "dedicated", "effect": "NoSchedule"}])], [])
        self.assertEqual(nodes[0]["status"], "Ready")
        self.assertFalse(nodes[0]["schedulable"])
        nodes, _ = normalize([node({"davano.dev/allocation-mode": "whole-gpu"}, ready="Unknown")], [])
        self.assertFalse(nodes[0]["schedulable"])


if __name__ == "__main__":
    unittest.main()
