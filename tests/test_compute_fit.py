import unittest
from davano.compute import compute_quantity, pod_compute_request
from davano.inventory import normalize, InventoryUnavailable
from test_inventory import node, pod
import test_api


def compute_node(name="one", cpu="4", memory="8Gi", **kwargs):
    n = node(name, **kwargs)
    for section in ("capacity", "allocatable"):
        n["status"][section].update(cpu=cpu, memory=memory)
    return n


def container(cpu="0", memory="0", sidecar=False):
    return {"resources": {"requests": {"cpu": cpu, "memory": memory}}, **({"restartPolicy": "Always"} if sidecar else {})}


class ComputeAccountingTests(unittest.TestCase):
    def test_quantities_and_conservative_rounding(self):
        for value, dim, expected in [("1.5", "cpu_millicores", 1500), ("500m", "cpu_millicores", 500), ("1Gi", "memory_mib", 1024), ("1G", "memory_mib", 953), ("1e3", "cpu_millicores", 1000000), ("1u", "cpu_millicores", 0)]:
            self.assertEqual(compute_quantity(value, dim), expected)
        self.assertEqual(compute_quantity("1u", "cpu_millicores", request=True), 1)
        self.assertEqual(compute_quantity("1048577", "memory_mib", request=True), 2)
        for value in (True, 1.5, "-1", "bad", None, "1e9999999", "1e-9999999", "1" * 101):
            self.assertIsNone(compute_quantity(value, "cpu_millicores"))

    def test_cpu_only_bound_pods_and_terminal_exclusion(self):
        p = pod(0); p["spec"]["containers"] = [container("500m", "1Gi")]
        terminal = pod(0, phase="Succeeded"); terminal["spec"]["containers"] = [container("4", "8Gi")]
        unbound = pod(0, node_name=""); unbound["spec"]["containers"] = [container("4", "8Gi")]
        n, _ = normalize([compute_node()], [p, terminal, unbound])
        self.assertEqual(n[0]["compute"]["cpu_millicores"]["available"], 3500)
        self.assertEqual(n[0]["compute"]["memory_mib"]["available"], 7168)

    def test_init_sidecars_overhead_and_limit_default(self):
        p = pod(0); p["spec"]["containers"] = [container("2", "2Gi")]
        p["spec"]["initContainers"] = [container("3", "3Gi", True), container("4", "4Gi"), container("2", "2Gi", True)]
        p["spec"]["overhead"] = {"cpu": "100m", "memory": "128Mi"}
        self.assertEqual(pod_compute_request(p, "cpu_millicores"), 7100)
        self.assertEqual(pod_compute_request(p, "memory_mib"), 7296)
        p["spec"] = {"containers": [{"resources": {"limits": {"cpu": "2"}}}]}
        self.assertEqual(pod_compute_request(p, "cpu_millicores"), 2000)

    def test_unknown_resize_and_pod_level_resources(self):
        for patch in ({"resources": {"requests": {"cpu": "2"}}}, {"containers": [{"resources": None}]}, {"containers": [container("broken")]}):
            p = pod(0); p["spec"].update(patch)
            self.assertIsNone(pod_compute_request(p, "cpu_millicores"))
            if patch == {"containers": [{"resources": None}]}:
                with self.assertRaises(InventoryUnavailable):
                    normalize([compute_node()], [p])
            else:
                n, _ = normalize([compute_node()], [p])
                self.assertIsNone(n[0]["compute"]["cpu_millicores"]["available"])
        p = pod(0); p["status"]["containerStatuses"] = [{"allocatedResources": {"cpu": "2"}}]
        self.assertIsNone(pod_compute_request(p, "cpu_millicores"))
        self.assertEqual(pod_compute_request(p, "memory_mib"), 0)

    def test_rounding_never_overstates_and_overrequests_clamp(self):
        p = pod(0, phase="Pending")
        p["spec"]["containers"] = [container("1u", "1"), container("1u", "1")]
        n, _ = normalize([compute_node(cpu="1500u", memory="1048577")], [p])
        self.assertEqual(n[0]["compute"]["cpu_millicores"], {"capacity": 1, "allocatable": 1, "requested": 2, "available": 0})
        self.assertEqual(n[0]["compute"]["memory_mib"]["available"], 0)
        self.assertTrue(any("exceed" in warning for warning in n[0]["warnings"]))

    def test_demo_scenarios(self):
        from fastapi.testclient import TestClient
        from davano.app import create_app
        with TestClient(create_app(db_path=":memory:")) as client:
            req = {"pool_id": "training", "namespace": "research", "gpu_count": 4, "cpu_millicores": 8000, "memory_mib": 32768}
            result = client.post("/api/v1/evaluate", json=req).json()
            self.assertEqual(result["verdict"], "fits")
            self.assertEqual(result["candidate_nodes"][0]["cpu_available_millicores"], 10000)
            self.assertEqual(client.post("/api/v1/evaluate", json={**req, "cpu_millicores": 12000}).json()["verdict"], "blocked")
            self.assertEqual(client.post("/api/v1/evaluate", json={**req, "pool_id": "research", "gpu_count": 2}).json()["verdict"], "unknown")


class ComputeFitAPITests(unittest.TestCase):
    setUp = test_api.APITests.setUp
    tearDown = test_api.APITests.tearDown
    evaluate = test_api.APITests.evaluate

    def test_malformed_maps_return_unavailable_not_gpu_fit(self):
        p = pod(0)
        p["spec"]["containers"] = [{"resources": {"requests": ["nvidia.com/gpu", 8]}}]
        self.inventory.pods = [p]
        self.assertEqual(self.evaluate(gpu_count=8).status_code, 503)
        self.assertEqual(self.client.get("/api/v1/state").status_code, 503)

    def test_null_cpu_only_conditions_return_unknown_not_server_error(self):
        self.inventory.nodes = [compute_node()]
        p = pod(0)
        p["status"]["conditions"] = None
        self.inventory.pods = [p]
        response = self.evaluate(cpu_millicores=1)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["verdict"], "unknown")

    def test_compute_same_node_fit_and_rejection(self):
        self.inventory.nodes = [compute_node(cpu="2", memory="8Gi"), compute_node("two", cpu="8", memory="1Gi")]
        result = self.evaluate(cpu_millicores=4000, memory_mib=4096).json()
        self.assertEqual(result["verdict"], "blocked")
        self.assertEqual([c["status"] for c in result["node_checks"]], ["rejected", "rejected"])
        result = self.evaluate(cpu_millicores=1000, memory_mib=4096).json()
        self.assertEqual(result["verdict"], "fits")
        self.assertEqual([n["id"] for n in result["candidate_nodes"]], ["one"])
        self.assertFalse(result["allocated"]); self.assertFalse(result["submitted"])

    def test_unknown_only_when_not_otherwise_rejected(self):
        self.assertEqual(self.evaluate(cpu_millicores=1).json()["verdict"], "unknown")
        self.assertEqual(self.evaluate(cpu_millicores=0).json()["verdict"], "fits")
        self.assertEqual(self.evaluate(cpu_millicores=None).json()["verdict"], "fits")
        self.inventory.nodes = [compute_node(cpu="bad", memory="1Gi")]
        self.assertEqual(self.evaluate(cpu_millicores=1, memory_mib=2048).json()["verdict"], "blocked")

    def test_compute_strict_validation(self):
        for key in ("cpu_millicores", "memory_mib"):
            for value in (-1, True, "100", 1.5, 10**13):
                with self.subTest(key=key, value=value):
                    self.assertEqual(self.evaluate(**{key: value}).status_code, 422)

    def test_cpu_only_pod_can_block_gpu_fit(self):
        self.inventory.nodes = [compute_node()]
        p = pod(0); p["spec"]["containers"] = [container("3500m")]
        self.inventory.pods = [p]
        result = self.evaluate(cpu_millicores=1000).json()
        self.assertEqual(result["verdict"], "blocked")
        self.assertTrue(any("Insufficient CPU" in r for r in result["node_checks"][0]["reasons"]))


class MalformedAccountingTests(unittest.TestCase):
    def test_invalid_gpu_resource_maps_never_become_zero(self):
        for resources in (None, [], "bad", {"requests": None}, {"limits": []}, {"requests": {"nvidia.com/gpu": "2"}, "limits": "bad"}):
            p = pod(4)
            p["spec"]["containers"][0]["resources"] = resources
            with self.subTest(resources=resources), self.assertRaises(InventoryUnavailable):
                normalize([compute_node()], [p])

    def test_malformed_conditions_on_cpu_only_pod_are_unknown(self):
        for conditions in (None, {}, "bad", [None]):
            p = pod(0)
            p["spec"]["containers"] = [container("1", "1Gi")]
            p["status"]["conditions"] = conditions
            with self.subTest(conditions=conditions):
                nodes, _ = normalize([compute_node()], [p])
                self.assertIsNone(nodes[0]["compute"]["cpu_millicores"]["available"])
                self.assertIsNone(nodes[0]["compute"]["memory_mib"]["available"])

    def test_malformed_container_and_overhead_structures_fail_closed(self):
        for patch in ({"containers": None}, {"containers": [None]}, {"initContainers": {}}, {"initContainers": [None]}, {"overhead": None}):
            p = pod(4); p["spec"].update(patch)
            with self.subTest(patch=patch), self.assertRaises(InventoryUnavailable):
                normalize([compute_node()], [p])
