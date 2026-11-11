"""Static security and draft-contract checks; never contacts a Kubernetes API.

Install optional dev dependencies from deploy/requirements-test.txt. These tests
are deliberately not a substitute for kustomize rendering or API-server dry-run.
"""
import copy
import json
import unittest
from pathlib import Path

try:
    import yaml
    import jsonschema
except ImportError:
    yaml = None
    jsonschema = None

ROOT = Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy"


def read(path):
    with (DEPLOY / path).open() as stream:
        return yaml.safe_load(stream)


@unittest.skipUnless(yaml is not None and jsonschema is not None,
                     "Install deploy/requirements-test.txt for static manifest checks")
class ManifestSafetyTests(unittest.TestCase):
    def setUp(self):
        self.deployment = read("base/deployment.yaml")
        self.pod = self.deployment["spec"]["template"]["spec"]
        self.container = self.pod["containers"][0]

    def test_base_demo_has_no_credentials_or_rbac(self):
        self.assertFalse(self.pod["automountServiceAccountToken"])
        self.assertFalse(read("base/serviceaccount.yaml")["automountServiceAccountToken"])
        env = {v["name"]: v["value"] for v in self.container["env"]}
        self.assertEqual(env["DAVANO_SOURCE"], "demo")
        for resource in read("base/kustomization.yaml")["resources"]:
            self.assertNotIn(read("base/" + resource)["kind"],
                             {"Role", "RoleBinding", "ClusterRole", "ClusterRoleBinding"})

    def test_live_permission_surface_is_exactly_inventory_reads(self):
        role = read("overlays/live-readonly/clusterrole.yaml")
        self.assertEqual(role["rules"], [{"apiGroups": [""],
                         "resources": ["nodes", "pods"], "verbs": ["get", "list"]}])
        binding = read("overlays/live-readonly/clusterrolebinding.yaml")
        self.assertEqual(binding["roleRef"]["name"], role["metadata"]["name"])
        self.assertEqual(binding["subjects"], [{"kind": "ServiceAccount",
                                               "name": "davano", "namespace": "davano"}])
        pod = read("overlays/live-readonly/deployment-patch.yaml")["spec"]["template"]["spec"]
        self.assertTrue(pod["automountServiceAccountToken"])
        self.assertEqual(pod["containers"][0]["env"],
                         [{"name": "DAVANO_SOURCE", "value": "kubernetes"}])

    def test_container_is_restricted_nonroot(self):
        self.assertTrue(self.pod["securityContext"]["runAsNonRoot"])
        self.assertEqual(self.pod["securityContext"]["seccompProfile"]["type"], "RuntimeDefault")
        security = self.container["securityContext"]
        self.assertFalse(security["allowPrivilegeEscalation"])
        self.assertTrue(security["readOnlyRootFilesystem"])
        self.assertEqual(security["capabilities"]["drop"], ["ALL"])
        self.assertFalse(security.get("privileged", False))
        for key in ["hostNetwork", "hostPID", "hostIPC"]:
            self.assertFalse(self.pod.get(key, False))
        self.assertNotIn("initContainers", self.pod)
        self.assertTrue(all("hostPath" not in v for v in self.pod["volumes"]))

    def test_database_mount_matches_server_configuration(self):
        env = {v["name"]: v["value"] for v in self.container["env"]}
        self.assertEqual(env["DAVANO_DB_PATH"], "/data/davano.db")
        mounts = {v["name"]: v["mountPath"] for v in self.container["volumeMounts"]}
        self.assertEqual(mounts, {"data": "/data", "tmp": "/tmp"})
        volumes = {v["name"]: v for v in self.pod["volumes"]}
        self.assertIn("emptyDir", volumes["data"])
        self.assertGreater(self.pod["securityContext"]["fsGroup"], 0)

    def test_sqlite_single_writer_and_explicit_persistence(self):
        self.assertEqual(self.deployment["spec"]["replicas"], 1)
        self.assertEqual(self.deployment["spec"]["strategy"]["type"], "Recreate")
        patch = read("overlays/persistent/deployment-patch.yaml")
        volume = patch["spec"]["template"]["spec"]["volumes"][0]
        self.assertIsNone(volume["emptyDir"])
        pvc = read("overlays/persistent/pvc.yaml")
        self.assertEqual(volume["persistentVolumeClaim"]["claimName"], pvc["metadata"]["name"])
        self.assertEqual(pvc["spec"]["accessModes"], ["ReadWriteOnce"])
        self.assertIn("../live-readonly", read("overlays/persistent/kustomization.yaml")["resources"])

    def test_openshift_defers_group_assignment_to_scc(self):
        patch = read("overlays/openshift/deployment-patch.yaml")
        security = patch["spec"]["template"]["spec"]["securityContext"]
        self.assertIn("fsGroup", security)
        self.assertIsNone(security["fsGroup"])
        self.assertNotIn("runAsUser", self.pod["securityContext"])
        self.assertNotIn("runAsUser", self.container["securityContext"])

    def test_default_ingress_is_denied_without_breaking_api_egress(self):
        policy = read("base/networkpolicy.yaml")
        self.assertEqual(policy["spec"]["policyTypes"], ["Ingress"])
        self.assertEqual(policy["spec"]["ingress"], [])
        self.assertEqual(policy["spec"]["podSelector"]["matchLabels"],
                         self.deployment["spec"]["selector"]["matchLabels"])
        self.assertIn("networkpolicy.yaml", read("base/kustomization.yaml")["resources"])

    def test_no_gpu_requested_by_control_plane(self):
        for quantities in self.container["resources"].values():
            self.assertEqual(set(quantities), {"cpu", "memory"})

    def test_private_service_and_consistent_health_ports(self):
        service = read("base/service.yaml")
        self.assertEqual(service["spec"]["type"], "ClusterIP")
        self.assertNotIn("externalIPs", service["spec"])
        self.assertEqual(service["spec"]["selector"], self.deployment["spec"]["selector"]["matchLabels"])
        self.assertEqual(service["spec"]["ports"][0]["targetPort"], "http")
        self.assertEqual(self.container["ports"], [{"name": "http", "containerPort": 8080}])
        for probe, path in [("startupProbe", "/healthz"), ("livenessProbe", "/healthz"),
                            ("readinessProbe", "/readyz")]:
            self.assertEqual(self.container[probe]["httpGet"], {"path": path, "port": "http"})

    def test_every_deployment_root_excludes_crds_and_public_ingress(self):
        def walk(path, seen):
            path = path.resolve()
            if path in seen:
                return
            seen.add(path)
            self.assertTrue(path.is_relative_to(DEPLOY.resolve()))
            if path.is_dir():
                kustomization = yaml.safe_load((path / "kustomization.yaml").read_text())
                for target in kustomization.get("resources", []):
                    walk(path / target, seen)
            else:
                self.assertNotIn("contracts", path.parts)
                self.assertNotIn("examples", path.parts)
                obj = yaml.safe_load(path.read_text())
                self.assertNotIn(obj["kind"], {"CustomResourceDefinition", "Ingress", "Route", "SecurityContextConstraints"})
        for root in [DEPLOY / "base/kustomization.yaml", * (DEPLOY / "overlays").rglob("kustomization.yaml")]:
            walk(root.parent, set())

    def test_future_crds_are_namespaced_advisory_and_have_status_subresources(self):
        for path in ["contracts/davanopools.yaml", "contracts/davanoownershippolicies.yaml"]:
            crd = read(path)
            self.assertEqual(crd["spec"]["scope"], "Namespaced")
            self.assertIn("DRAFT CONTRACT ONLY", crd["metadata"]["annotations"]["davano.io/lifecycle"])
            version = crd["spec"]["versions"][0]
            self.assertEqual(version["subresources"], {"status": {}})
            fields = version["schema"]["openAPIV3Schema"]["properties"]
            for name in ["spec", "status"]:
                self.assertEqual(fields[name]["properties"]["enforcement"]["enum"], ["advisory-only"])

    def test_future_crd_examples_validate_structurally(self):
        # jsonschema does not evaluate Kubernetes CEL; those rules require an API server.
        names = {"DavanoPool": "davanopools", "DavanoOwnershipPolicy": "davanoownershippolicies"}
        examples = list(yaml.safe_load_all((DEPLOY / "examples/future-contracts.yaml").read_text()))
        self.assertEqual(len(examples), 2)
        for obj in examples:
            crd = read("contracts/" + names[obj["kind"]] + ".yaml")
            schema = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            jsonschema.validate(obj, schema)
            invalid = copy.deepcopy(obj)
            invalid["spec"]["enforcement"] = "enforced"
            with self.assertRaises(jsonschema.ValidationError):
                jsonschema.validate(invalid, schema)

    def test_resource_units_are_distinct_and_cel_guards_are_declared(self):
        crd = read("contracts/davanopools.yaml")
        resource = crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]["properties"]["resource"]
        self.assertEqual(set(resource["properties"]["kind"]["enum"]), {"whole-gpu", "mig", "time-sliced", "mps"})
        self.assertEqual(set(resource["properties"]["unit"]["enum"]), {"device", "mig-instance", "shared-slot"})
        rules = [v["rule"] for v in resource["x-kubernetes-validations"]]
        self.assertIn("self.kind != 'whole-gpu' || self.unit == 'device'", rules)
        self.assertIn("self.kind != 'mig' || (self.unit == 'mig-instance' && has(self.migProfile))", rules)
        self.assertIn("!(self.kind in ['time-sliced', 'mps']) || self.unit == 'shared-slot'", rules)

    def test_proposed_capability_schema_cannot_advertise_enforcement(self):
        schema = json.loads((DEPLOY / "contracts/capabilities.schema.json").read_text())
        jsonschema.Draft202012Validator.check_schema(schema)
        example = {"schema_version": "1", "source": "kubernetes", "read_only_cluster": True,
                   "enforcement": "advisory-only", "observation": {"state": "unavailable", "observed_at": None},
                   "capabilities": [{"id": "admission", "level": "unsupported", "detail": "No adapter installed."}]}
        jsonschema.validate(example, schema)
        example["capabilities"][0]["level"] = "enforced"
        with self.assertRaises(jsonschema.ValidationError):
            jsonschema.validate(example, schema)


if __name__ == "__main__":
    unittest.main()
