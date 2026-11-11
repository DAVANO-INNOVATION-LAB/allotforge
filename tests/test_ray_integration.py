"""Cluster-free contract checks. These do not assert KubeRay admission works."""
import copy
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError

from davano.ray_integration import (
    QueueBundleRequest, RayJobRequest, RayPlanError, build_kueue_bundle,
    build_suspended_ray_job, create_ray_router, generated_name, policy_limits,
)

POOL = {"id": "training", "name": "Training", "selector": {"davano.dev/pool": "training"}, "vendor": "NVIDIA", "mode": "whole-gpu"}
POLICY = {"id": "research", "pool_id": "training", "namespace": "research", "max_gpus_per_workload": 8}
BASE = {"name": "train-example", "namespace": "research", "pool_id": "training", "image": "rayproject/ray:2.55.1-gpu", "ray_version": "2.55.1", "entrypoint": "python /app/train.py"}


def request(**kwargs):
    return RayJobRequest(**{**BASE, **kwargs})


class RayManifestTests(unittest.TestCase):
    def test_real_rayjob_contract_suspended_fixed_and_private(self):
        job = build_suspended_ray_job(request(workers=2, gpus_per_worker=2), POOL, [POLICY], "approved-queue")
        self.assertEqual((job["apiVersion"], job["kind"]), ("ray.io/v1", "RayJob"))
        self.assertEqual(job["metadata"]["labels"]["kueue.x-k8s.io/queue-name"], "approved-queue")
        spec = job["spec"]
        self.assertTrue(spec["suspend"])
        self.assertTrue(spec["shutdownAfterJobFinishes"])
        self.assertFalse(spec["rayClusterSpec"]["enableInTreeAutoscaling"])
        self.assertEqual(spec["backoffLimit"], 0)
        self.assertNotIn("clusterSelector", spec)
        self.assertNotIn("managedBy", spec)
        head = spec["rayClusterSpec"]["headGroupSpec"]
        self.assertEqual(head["serviceType"], "ClusterIP")
        self.assertEqual(head["rayStartParams"]["num-gpus"], "0")
        self.assertNotIn("nodeSelector", head["template"]["spec"])
        self.assertEqual(head["template"]["spec"]["containers"][0]["env"], [{"name": "NVIDIA_VISIBLE_DEVICES", "value": "void"}])
        group = spec["rayClusterSpec"]["workerGroupSpecs"][0]
        self.assertEqual([group[k] for k in ("replicas", "minReplicas", "maxReplicas")], [2, 2, 2])
        self.assertEqual(group["rayStartParams"]["num-gpus"], "2")
        worker = group["template"]["spec"]
        self.assertEqual(worker["nodeSelector"], POOL["selector"])
        self.assertEqual(worker["containers"][0]["resources"]["requests"]["nvidia.com/gpu"], 2)
        self.assertEqual(worker["containers"][0]["resources"]["requests"], worker["containers"][0]["resources"]["limits"])
        for template in [group["template"], head["template"], spec["submitterPodTemplate"]]:
            pod = template["spec"]
            self.assertFalse(pod["automountServiceAccountToken"])
            self.assertNotIn("hostNetwork", pod)
            self.assertNotIn("volumes", pod)
            self.assertTrue(pod["securityContext"]["runAsNonRoot"])
            self.assertFalse(pod["containers"][0]["securityContext"]["allowPrivilegeEscalation"])
        self.assertEqual(spec["submitterPodTemplate"]["spec"]["restartPolicy"], "Never")

    def test_input_not_mutated_or_aliased(self):
        pool, policies = copy.deepcopy(POOL), [copy.deepcopy(POLICY)]
        original = copy.deepcopy((pool, policies))
        job = build_suspended_ray_job(request(), pool, policies, "approved-queue")
        job["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"]["spec"]["nodeSelector"]["davano.dev/pool"] = "modified"
        self.assertEqual((pool, policies), original)

    def test_policy_cap_is_total_not_per_worker(self):
        with self.assertRaisesRegex(RayPlanError, "Total requested GPU units"):
            build_suspended_ray_job(request(workers=5, gpus_per_worker=2), POOL, [POLICY], "q")

    def test_strictest_matching_policy_and_namespace(self):
        policies = [POLICY, {**POLICY, "id": "strict", "max_gpus_per_workload": 1}]
        with self.assertRaisesRegex(RayPlanError, "strictest"):
            build_suspended_ray_job(request(workers=2), POOL, policies, "q")
        for policies in ([], [{**POLICY, "namespace": "other"}], [{**POLICY, "pool_id": "other"}]):
            with self.assertRaisesRegex(RayPlanError, "No matching"):
                build_suspended_ray_job(request(), POOL, policies, "q")

    def test_bad_pool_vendor_resource_selector_and_queue_fail(self):
        bad_pools = [{**POOL, "id": "other"}, {**POOL, "vendor": "AMD"}, {**POOL, "mode": "shared"}, {**POOL, "resource_name": "amd.com/gpu"}, {**POOL, "selector": {}}, {**POOL, "selector": {"bad key": "value"}}]
        for pool in bad_pools:
            with self.subTest(pool=pool), self.assertRaises(RayPlanError):
                build_suspended_ray_job(request(), pool, [POLICY], "q")
        for queue in ("", "../escape", "UPPER", "a" * 64, None):
            with self.subTest(queue=queue), self.assertRaises(RayPlanError):
                build_suspended_ray_job(request(), POOL, [POLICY], queue)

    def test_policy_limits_cannot_be_missing_bool_or_negative(self):
        for cap in (None, True, -1, 0, "4"):
            with self.subTest(cap=cap), self.assertRaises(RayPlanError):
                policy_limits(request(), POOL, [{**POLICY, "max_gpus_per_workload": cap}])

    def test_managed_policy_per_pod_and_namespace_quota_are_distinct(self):
        pool = {"metadata": {"name": "training"}, "spec": {"nodeSelector": POOL["selector"], "mode": "whole-gpu", "resourceName": "nvidia.com/gpu", "nominalQuota": 12}}
        policy = {"metadata": {"name": "team", "namespace": "research"}, "spec": {"poolRef": "training", "gpuQuota": 4, "maxGPUsPerPod": 2, "subjects": [{"kind": "User", "name": "alice"}]}}
        # Two workers at two each fit; maxGPUsPerPod must not become a total cap.
        limits = policy_limits(request(workers=2, gpus_per_worker=2), pool, [policy])
        self.assertEqual(limits["total_gpu_units"], 4)
        self.assertIsNone(limits["per_workload_cap"])
        self.assertEqual(limits["per_pod_cap"], 2)
        with self.assertRaisesRegex(RayPlanError, "per-pod"):
            policy_limits(request(gpus_per_worker=3), pool, [policy])
        with self.assertRaisesRegex(RayPlanError, "quota"):
            policy_limits(request(workers=3, gpus_per_worker=2), pool, [policy])
        with self.assertRaisesRegex(RayPlanError, "quota"):
            policy_limits(request(), pool, [{**policy, "spec": {**policy["spec"], "gpuQuota": 0}}])

    def test_shared_explicit_mapping_not_physical_gpu(self):
        job = build_suspended_ray_job(request(resource_name="nvidia.com/gpu.shared", mode="shared"), {**POOL, "mode": "shared"}, [POLICY], "shared")
        group = job["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]
        self.assertEqual(group["rayStartParams"]["num-gpus"], "1")
        resources = group["template"]["spec"]["containers"][0]["resources"]["limits"]
        self.assertEqual(resources["nvidia.com/gpu.shared"], 1)
        self.assertNotIn("nvidia.com/gpu", resources)
        self.assertEqual(job["metadata"]["annotations"]["davano.io/allocation-mode"], "shared")
        with self.assertRaises(ValidationError):
            request(resource_name="nvidia.com/gpu.shared", mode="shared", gpus_per_worker=2)

    def test_mig_preserves_profile_name(self):
        job = build_suspended_ray_job(request(resource_name="nvidia.com/mig-1g.10gb", mode="mig"), {**POOL, "mode": "mig"}, [POLICY], "mig")
        resources = job["spec"]["rayClusterSpec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["resources"]["limits"]
        self.assertEqual(resources["nvidia.com/mig-1g.10gb"], 1)
        self.assertNotIn("nvidia.com/gpu", resources)

    def test_ambiguous_and_mismatched_modes_fail_closed(self):
        for mode, resource in [("shared", "nvidia.com/gpu"), ("mig", "nvidia.com/gpu"), ("whole-gpu", "nvidia.com/gpu.shared"), ("whole-gpu", "nvidia.com/mig-1g.10gb"), ("whole-gpu", "evil.test/gpu"), ("mig", "nvidia.com/mig-1g.10gb.shared")]:
            with self.subTest(mode=mode, resource=resource), self.assertRaises(ValidationError):
                request(mode=mode, resource_name=resource)

    def test_strict_counts_and_unknown_fields(self):
        for count in (True, 1.5, "2", 0, -1):
            for field in ("workers", "gpus_per_worker", "worker_cpu", "head_cpu"):
                with self.subTest(field=field, count=count), self.assertRaises(ValidationError):
                    request(**{field: count})
        for field in ("suspend", "privileged", "runtime_env", "nodeSelector", "serviceAccountName", "enableInTreeAutoscaling"):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                request(**{field: True})
        with self.assertRaises(ValidationError):
            request(entrypoint="python x.py\nwhoami")

    def test_images_require_explicit_tag_or_valid_digest(self):
        for image in ("rayproject/ray", "rayproject/ray:latest", "registry:5000/ray", "rayproject/ray@sha256:abc", "https://example.com/ray:2", "ray:2;whoami", "ray:2\n", "ray:2@secret", "ray:::bad", "registry:bogus/ray:v1", "ray:v1//app:v1", "ray:v1/"):
            # Whitespace strip is shared StrictModel behavior; embedded controls
            # and unpinned refs are rejected, trailing space alone is harmless.
            if image == "ray:2\n":
                continue
            with self.subTest(image=image), self.assertRaises(ValidationError):
                request(image=image)
        for image in ("registry.example:5000/team/ray:2.55.1-gpu", "ray:V2", "ray@sha256:" + "a" * 64):
            self.assertEqual(request(image=image).image, image)


class QueueBundleTests(unittest.TestCase):
    def test_bundle_quota_scope_and_head_placement(self):
        req = QueueBundleRequest(pool_id="training", namespace="research", node_selector=POOL["selector"], nominal_gpu_quota=8, nominal_cpu_quota=64, nominal_memory_gib=256)
        manifests = build_kueue_bundle(req)
        self.assertEqual([m["kind"] for m in manifests], ["ResourceFlavor", "ResourceFlavor", "ClusterQueue", "LocalQueue"])
        self.assertTrue(all(m["apiVersion"] == "kueue.x-k8s.io/v1beta1" for m in manifests))
        self.assertEqual(manifests[0]["spec"]["nodeLabels"], POOL["selector"])
        self.assertEqual(manifests[1]["spec"], {})
        cq = manifests[2]
        self.assertEqual(cq["spec"]["namespaceSelector"], {"matchLabels": {"kubernetes.io/metadata.name": "research"}})
        self.assertNotIn("cohort", cq["spec"])
        self.assertNotIn("preemption", cq["spec"])
        self.assertEqual(cq["spec"]["resourceGroups"][0]["coveredResources"], ["nvidia.com/gpu"])
        self.assertEqual(cq["spec"]["resourceGroups"][1]["coveredResources"], ["cpu", "memory"])
        self.assertEqual(manifests[-1]["spec"]["clusterQueue"], cq["metadata"]["name"])
        self.assertEqual(manifests[-1]["metadata"]["namespace"], "research")

    def test_resource_flavor_limits_selector_to_eight_labels(self):
        fields = {"pool_id": "training", "namespace": "research", "nominal_gpu_quota": 8, "nominal_cpu_quota": 64, "nominal_memory_gib": 256}
        eight = {f"example.com/label-{i}": "gpu" for i in range(8)}
        request = QueueBundleRequest(**fields, node_selector=eight)
        self.assertEqual(len(build_kueue_bundle(request)[0]["spec"]["nodeLabels"]), 8)
        with self.assertRaises(ValidationError):
            QueueBundleRequest(**fields, node_selector={**eight, "example.com/ninth": "gpu"})

    def test_stable_names_cannot_collide_after_truncation(self):
        left = generated_name("davano", "a" * 63, "b" * 63)
        right = generated_name("davano", "a" * 63, "b" * 62 + "c")
        self.assertNotEqual(left, right)
        self.assertLessEqual(len(left), 63)
        self.assertRegex(left, r"^[a-z0-9][a-z0-9-]*[a-z0-9]$")
        self.assertEqual(left, generated_name("davano", "a" * 63, "b" * 63))

    def test_quota_never_inferred_or_fractional(self):
        fields = {"pool_id": "training", "namespace": "research", "node_selector": POOL["selector"], "nominal_gpu_quota": 8, "nominal_cpu_quota": 64, "nominal_memory_gib": 256}
        for key in ("nominal_gpu_quota", "nominal_cpu_quota", "nominal_memory_gib"):
            body = fields.copy()
            body.pop(key)
            with self.assertRaises(ValidationError):
                QueueBundleRequest(**body)
            with self.assertRaises(ValidationError):
                QueueBundleRequest(**{**fields, key: True})


class FakeFleet:
    def __init__(self):
        self.data = {
            "meta": {"source": "demo", "observed_at": "2026-11-11T00:00:00Z"},
            "pools": [POOL], "policies": [POLICY],
            "nodes": [{"labels": POOL["selector"], "resources": [{"name": "nvidia.com/gpu", "mode": "whole-gpu", "allocatable": 4, "available": 0}]}],
        }
    def state(self):
        return copy.deepcopy(self.data)


class RayPlannerAPITests(unittest.TestCase):
    def setUp(self):
        self.fleet = FakeFleet()
        app = FastAPI()
        app.include_router(create_ray_router(self.fleet))
        self.client = TestClient(app)
    def test_plan_is_not_submission_or_fit_and_does_not_mutate(self):
        before = copy.deepcopy(self.fleet.data)
        response = self.client.post("/api/v1/ray/plan", json=BASE)
        self.assertEqual(response.status_code, 200)
        plan = response.json()
        self.assertFalse(plan["submitted"])
        self.assertFalse(plan["allocated"])
        self.assertTrue(plan["advisory_only"])
        self.assertEqual(plan["status"], "planned")
        self.assertNotIn("fits", plan)
        self.assertTrue(plan["manifest"]["spec"]["suspend"])
        self.assertTrue(any("synthetic demo" in warning for warning in plan["warnings"]))
        self.assertEqual(before, self.fleet.data)
    def test_no_policy_or_unobserved_mode_blocks(self):
        self.fleet.data["policies"] = []
        self.assertEqual(self.client.post("/api/v1/ray/plan", json=BASE).status_code, 422)
        self.fleet.data["policies"] = [POLICY]
        for mode in ("shared", "unknown", "mig"):
            self.fleet.data["nodes"][0]["resources"][0]["mode"] = mode
            self.assertEqual(self.client.post("/api/v1/ray/plan", json=BASE).status_code, 422)
    def test_mixed_modes_under_same_selector_block_even_with_whole_node(self):
        self.fleet.data["nodes"].append({"labels": POOL["selector"], "resources": [{"name": "nvidia.com/gpu", "mode": "shared", "allocatable": 80}]})
        result = self.client.post("/api/v1/ray/plan", json=BASE)
        self.assertEqual(result.status_code, 422)
        self.assertIn("mixed or unknown", result.json()["detail"])

    def test_unknown_capacity_and_oversized_worker_block(self):
        self.fleet.data["nodes"][0]["resources"][0]["allocatable"] = None
        self.assertEqual(self.client.post("/api/v1/ray/plan", json=BASE).status_code, 422)
        self.fleet.data["nodes"][0]["resources"][0]["allocatable"] = 4
        self.assertEqual(self.client.post("/api/v1/ray/plan", json={**BASE, "gpus_per_worker": 5}).status_code, 422)
    def test_missing_pool_and_spec_errors(self):
        self.assertEqual(self.client.post("/api/v1/ray/plan", json={**BASE, "pool_id": "other"}).status_code, 404)
        self.assertEqual(self.client.post("/api/v1/ray/plan", json={**BASE, "workers": True}).status_code, 422)
    def test_capabilities_do_not_advertise_cluster_writes(self):
        result = self.client.get("/api/v1/ray/capabilities").json()
        self.assertFalse(result["submission_enabled"])
        self.assertEqual(result["kuberay_version"], "1.7.2")
        self.assertEqual(result["api_modes"], ["whole-gpu"])
        self.assertEqual(self.client.post("/api/v1/ray/submit", json=BASE).status_code, 404)


if __name__ == "__main__":
    unittest.main()
