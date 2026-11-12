"""Synthetic runner/transport tests only. None contact Kubernetes or run GPU jobs."""
import base64
from contextlib import redirect_stdout
from copy import deepcopy
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from acceptance import cluster as c
from davano import reconciler
from davano.kube import ApiError

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "0123456789ab"
SERVER = "https://127.0.0.1:6443"
SYSTEM_UID = "11111111-1111-4111-8111-111111111111"
SOURCE = {"git_commit": "a" * 40, "source_tree_dirty": False, "profile_sha256": "b" * 64, "source_sha256": {"acceptance/cluster.py": "c" * 64}}


def target(**changes):
    return c.Target(**{"context": c.CONTEXT, "server": SERVER, "kube_system_uid": SYSTEM_UID, "kubernetes_version": c.KUBERNETES, "run_id": RUN_ID, **changes})


def meta(name, index=2, namespace=None):
    value = {"name": name, "uid": f"00000000-0000-4000-8000-{index:012d}", "resourceVersion": str(index), "generation": 1}
    if namespace:
        value["namespace"] = namespace
    return value


def condition(kind, state="True", generation=1):
    return {"type": kind, "status": state, "observedGeneration": generation}


def config_body():
    certificate = base64.b64encode(b"-----BEGIN CERTIFICATE-----\nsynthetic-test-only\n-----END CERTIFICATE-----").decode()
    key = base64.b64encode(b"-----BEGIN PRIVATE KEY-----\nsynthetic-test-only\n-----END PRIVATE KEY-----").decode()
    return {"apiVersion": "v1", "kind": "Config", "preferences": {}, "current-context": c.CONTEXT,
            "clusters": [{"name": c.CONTEXT, "cluster": {"server": SERVER, "certificate-authority-data": certificate}}],
            "contexts": [{"name": c.CONTEXT, "context": {"cluster": c.CONTEXT, "user": c.CONTEXT}}],
            "users": [{"name": c.CONTEXT, "user": {"client-certificate-data": certificate, "client-key-data": key}}]}


class Clock:
    def __init__(self):
        self.now = 0
    def __call__(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


class FakeAPI:
    """A deliberate test double, never evidence of actual controller behavior."""
    def __init__(self):
        self.target = target()
        self.calls = []
        self.objects = {}
        self.counter = 20
        self.server_value = SERVER
        self.on_get = None
        self.on_create = None
        self.on_delete = None
        self.hold_deletions = False
        self.fixed = {
            "/version": {"gitVersion": c.KUBERNETES},
            "/api/v1/namespaces/kube-system": {"apiVersion": "v1", "kind": "Namespace", "metadata": {**meta("kube-system"), "uid": SYSTEM_UID, "labels": {c.DISPOSABLE_LABEL: "cluster-acceptance"}}},
            "/api/v1/nodes": {"metadata": {}, "items": [{"apiVersion": "v1", "kind": "Node", "metadata": {**meta("allotforge-acceptance-control-plane", 3), "labels": {"node-role.kubernetes.io/control-plane": ""}}, "status": {"nodeInfo": {"kubeletVersion": c.KUBERNETES}, "capacity": {"cpu": "2", "memory": "4Gi"}, "allocatable": {"cpu": "2", "memory": "4Gi"}}}]},
            c.ROOT: {"groupVersion": c.API, "resources": [{"name": name} for name in ("resourceflavors", "clusterqueues", "localqueues", "workloads")]},
            "/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager": {"apiVersion": "apps/v1", "kind": "Deployment", "metadata": meta("kueue-controller-manager", 4, "kueue-system"), "spec": {"replicas": 1, "template": {"spec": {"containers": [{"name": "manager", "image": c.KUEUE_IMAGE}]}}}, "status": {"observedGeneration": 1, "updatedReplicas": 1, "readyReplicas": 1, "availableReplicas": 1, "replicas": 1}},
            "/api/v1/namespaces/" + self.target.namespace + "/pods": {"metadata": {}, "items": []},
            "/apis/batch/v1/namespaces/" + self.target.namespace + "/jobs": {"metadata": {}, "items": []},
        }

    def server(self, budget):
        budget.call_timeout()
        self.calls.append(("SERVER", None, None))
        return self.server_value

    def _status(self):
        from davano.quantities import decimal_quantity
        workloads = [obj for obj in self.objects.values() if obj["kind"] == "Workload"]
        used = decimal_quantity("0")
        admitted = 0
        pending = 0
        for obj in workloads:
            cpu = obj["spec"]["podSets"][0]["template"]["spec"]["containers"][0]["resources"]["requests"]["cpu"]
            if used + decimal_quantity(cpu) <= 1:
                used += decimal_quantity(cpu)
                admitted += 1
                obj["status"] = {"conditions": [condition("QuotaReserved"), condition("Admitted")], "admission": {"clusterQueue": "dgp-" + RUN_ID, "podSetAssignments": [{"name": "main", "count": 1, "flavors": {"cpu": "dgp-" + RUN_ID, "memory": "dgp-" + RUN_ID}, "resourceUsage": {"cpu": cpu, "memory": "16Mi"}}]}}
            else:
                pending += 1
                obj["status"] = {"conditions": [condition("QuotaReserved", "False")]}
        for obj in self.objects.values():
            if obj["kind"] in ("ClusterQueue", "LocalQueue"):
                obj["status"] = {"conditions": [condition("Active")], "reservingWorkloads": admitted, "admittedWorkloads": admitted, "pendingWorkloads": pending, "flavorsReservation": [{"name": "dgp-" + RUN_ID, "resources": [{"name": "cpu", "total": str(used)}, {"name": "memory", "total": str(admitted * 16 * 1024 * 1024)}, {"name": "nvidia.com/gpu", "total": "0"}]}]}

    def get(self, path, budget):
        budget.call_timeout()
        self.calls.append(("GET", path, None))
        self._status()
        actual = deepcopy(self.fixed[path] if path in self.fixed else self.objects.get(path))
        return self.on_get(path, actual) if self.on_get else actual

    def create(self, collection, desired, budget):
        budget.call_timeout()
        self.calls.append(("POST", collection, deepcopy(desired)))
        self.counter += 1
        actual = deepcopy(desired)
        actual["metadata"].update(uid=meta("unused", self.counter)["uid"], resourceVersion=str(self.counter), generation=1)
        path = c.object_path(collection, desired)
        if path in self.objects:
            raise c.Rejected("kubectl_failed")
        self.objects[path] = actual
        self._status()
        if self.on_create:
            return self.on_create(collection, deepcopy(actual))
        return deepcopy(actual)

    def delete(self, path, uid, resource_version, budget):
        budget.call_timeout()
        self.calls.append(("DELETE", path, {"uid": uid, "resourceVersion": resource_version}))
        actual = self.objects[path]
        if actual["metadata"]["uid"] != uid or actual["metadata"]["resourceVersion"] != resource_version:
            raise c.Rejected("kubectl_failed")
        if self.on_delete:
            self.on_delete(path, actual)
        if self.hold_deletions:
            actual["metadata"]["deletionTimestamp"] = "2026-11-12T00:00:00Z"
        else:
            del self.objects[path]


class IsolatedGenerators(unittest.TestCase):
    def setUp(self):
        environment = patch.dict(os.environ, {}, clear=False)
        environment.start()
        self.addCleanup(environment.stop)
        # Production never rewrites generator API versions. Scoped test-only
        # patch isolates an existing default-beta1 module cache from the fixed
        # beta2 acceptance profile. CLI subprocess tests use the real import.
        self.api_patch = patch.multiple(reconciler, KUEUE_API=c.API, KUEUE=c.ROOT)
        self.api_patch.start()
        self.addCleanup(self.api_patch.stop)

    def run_fake(self, api=None):
        api = api or FakeAPI()
        clock = Clock()
        with patch.object(c, "source_identity", return_value=SOURCE):
            report = c.Runner(target(), api, clock, clock.sleep).run()
        return report, api


class TargetAndFixtureTests(IsolatedGenerators):
    def test_fixed_target_arguments_and_injection_rejected(self):
        for changes in ({"context": "current"}, {"context": "kind-production"}, {"context": "--server=https://evil"}, {"server": "http://127.0.0.1:6443"}, {"server": "https://localhost:6443"}, {"server": "https://127.0.0.1:65536"}, {"server": "https://127.0.0.1:6443/path"}, {"server": "https://user@127.0.0.1:6443"}, {"kube_system_uid": ""}, {"kubernetes_version": "v1.31.4"}, {"run_id": "../bad"}, {"run_id": "--namespace"}, {"run_id": "A" * 12}):
            with self.subTest(changes=changes), self.assertRaises(c.Rejected):
                target(**changes)

    def test_plan_exact_bounded_scope(self):
        plan = c.plan(RUN_ID)
        self.assertFalse(plan["production_pass"])
        self.assertEqual(len(plan["fixtures"]), 6)
        self.assertEqual([entry["object"]["kind"] for entry in plan["fixtures"]], ["Namespace", "ResourceFlavor", "ClusterQueue", "LocalQueue", "Workload", "Workload"])
        queue = plan["fixtures"][2]["object"]
        resources = queue["spec"]["resourceGroups"][0]["flavors"][0]["resources"]
        self.assertEqual({x["name"]: x["nominalQuota"] for x in resources}, {"cpu": "1", "memory": "64Mi", "nvidia.com/gpu": "0"})
        for fixture in plan["fixtures"][4:]:
            obj = fixture["object"]
            self.assertNotIn("ownerReferences", obj["metadata"])
            self.assertNotIn("kueue.x-k8s.io/job-uid", obj["metadata"]["labels"])
            self.assertEqual(set(obj["spec"]["podSets"][0]["template"]["spec"]["containers"][0]["resources"]["requests"]), {"cpu", "memory"})
        self.assertIn("physical_gpu", plan["unverified"])
        self.assertEqual(plan["bounds"]["run_seconds"], 180)

    def test_actual_generator_outputs_are_used_without_api_rewriting(self):
        pool, policy = c.generator_inputs(RUN_ID)
        expected = reconciler.pool_resources(pool, [target().namespace], "1", "64Mi")
        actual = c.fixtures(RUN_ID)[1:3]
        for (_, left), (_, right) in zip(expected, actual):
            self.assertEqual(left["spec"], right["spec"])
        with patch.object(reconciler, "KUEUE_API", "kueue.x-k8s.io/v1beta1"):
            with self.assertRaisesRegex(c.Rejected, "generator_api_version"):
                c.fixtures(RUN_ID)

    def test_real_reconciler_blocks_gpu_less_selected_node_before_any_writes(self):
        pool, _ = c.generator_inputs(RUN_ID)
        class NoWrites:
            def __getattr__(self, name):
                raise AssertionError("Reconciler reached a write/client method: " + name)
        runner = reconciler.Reconciler(NoWrites(), [target().namespace])
        runner.pools = [pool]
        runner.nodes = [{"metadata": {"labels": pool["spec"]["nodeSelector"]}, "status": {"allocatable": {"cpu": "2", "memory": "4Gi"}}}]
        with self.assertRaisesRegex(ApiError, "verified whole-GPU"):
            runner.reconcile_pool(pool)


class JSONTests(unittest.TestCase):
    def test_reject_ambiguous_unbounded_malformed_and_nonfinite(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":{"x":1,"x":2}}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":1e999}', b'{"a":1.0}', b'[]', b'null', b'not-json', b'\xff', b'{' + b' ' * c.MAX_BYTES, b'{"a":' + b'[' * 40 + b'0' + b']' * 40 + b'}'):
            with self.subTest(raw=raw[:50]), self.assertRaises(c.Rejected):
                c.strict_json(raw)

    def test_conditions_fail_closed_for_stale_bool_duplicate_or_absent(self):
        obj = {"metadata": {"generation": 2}, "status": {"conditions": [condition("Active", generation=1)]}}
        self.assertFalse(c.current_condition(obj, "Active", "True"))
        obj["metadata"]["generation"] = 1
        obj["status"]["conditions"][0]["observedGeneration"] = True
        self.assertFalse(c.current_condition(obj, "Active", "True"))
        obj["status"]["conditions"] = []
        self.assertFalse(c.current_condition(obj, "Active", "True"))
        for conditions in ([condition("Active"), condition("Active")], ["bad"], [condition("Active", "yes")], {}):
            obj["status"]["conditions"] = conditions
            with self.assertRaises(c.Rejected):
                c.current_condition(obj, "Active", "True")

    def test_quantity_units_compare_exactly_and_malformed_refused(self):
        c.quantities({"cpu": "0.1", "memory": "16777216"}, {"cpu": "100m", "memory": "16Mi"})
        for wrong in ({"cpu": True}, {"cpu": "NaN"}, {"cpu": "1e9999999"}, {"cpu": "-1"}, {"cpu": "1", "nvidia.com/gpu": "1"}):
            with self.assertRaises(c.Rejected):
                c.quantities(wrong, {"cpu": "1"})


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "config.json"
        self.write(config_body())

    def write(self, body):
        self.path.write_text(json.dumps(body))
        self.path.chmod(0o600)

    def test_private_snapshot_cannot_be_retargeted_by_input_or_environment(self):
        config = c.PinnedConfig(self.path, target())
        self.addCleanup(config.close)
        self.path.write_text("not original config")
        self.assertEqual(json.loads(Path(config.path).read_text()), config_body())
        self.assertEqual(os.fstat(config.fd).st_mode & 0o777, 0o600)
        with patch.dict(os.environ, {"KUBECONFIG": "/evil", "HTTPS_PROXY": "https://evil", "http_proxy": "http://evil"}):
            raw = c.bounded_process([sys.executable, "-c", "import os; print(os.getenv('KUBECONFIG'),os.getenv('HTTPS_PROXY'),os.getenv('http_proxy'))"], None, 2)
        self.assertEqual(raw, b"None None None\n")

    def test_inherited_readonly_descriptor_works_without_named_file(self):
        config = c.PinnedConfig(self.path, target())
        try:
            raw = c.bounded_process([sys.executable, "-c", "import pathlib,sys; print(pathlib.Path(sys.argv[1]).read_text())", config.path], None, 2, pass_fds=(config.fd,))
            self.assertEqual(json.loads(raw), config_body())
        finally:
            config.close()
        self.assertFalse(Path(config.path).exists())

    def test_duplicate_and_oversized_config_never_load(self):
        for raw in ('{"kind":"Config","kind":"evil"}', ' ' * (c.MAX_INPUT + 1)):
            self.path.write_text(raw)
            with self.assertRaises(c.Rejected):
                c.PinnedConfig(self.path, target())


    def test_permissions_symlink_and_nonregular_input_rejected(self):
        self.path.chmod(0o644)
        with self.assertRaises(c.Rejected):
            c.PinnedConfig(self.path, target())
        self.path.unlink()
        self.path.symlink_to("/dev/null")
        with self.assertRaises(OSError):
            c.PinnedConfig(self.path, target())
        self.path.unlink()
        os.mkfifo(self.path, 0o600)
        with self.assertRaises(c.Rejected):
            c.PinnedConfig(self.path, target())

    def test_credential_execution_proxy_insecure_impersonation_and_extra_context_refused(self):
        mutations = [lambda b: b["users"][0]["user"].update(exec={"command": "evil"}),
                     lambda b: b["users"][0]["user"].update(token="secret-token"),
                     lambda b: b["users"][0]["user"].update(**{"auth-provider": {}, "as": "system:admin"}),
                     lambda b: b["clusters"][0]["cluster"].update(**{"proxy-url": "http://evil"}),
                     lambda b: b["clusters"][0]["cluster"].update(**{"insecure-skip-tls-verify": True}),
                     lambda b: b["clusters"][0]["cluster"].update(**{"certificate-authority": "/secret"}),
                     lambda b: b["clusters"][0]["cluster"].update(server="https://127.0.0.1:7443"),
                     lambda b: b["contexts"].append(deepcopy(b["contexts"][0])),
                     lambda b: b["contexts"][0]["context"].update(namespace="default"),
                     lambda b: b["users"][0]["user"].update(**{"client-key-data": "invalid-base64"})]
        for mutate in mutations:
            body = config_body()
            mutate(body)
            self.write(body)
            with self.subTest(body=body.keys()), self.assertRaises(c.Rejected):
                c.PinnedConfig(self.path, target())


class ProcessTests(unittest.TestCase):
    def test_stderr_and_failures_do_not_leak(self):
        with self.assertRaises(c.Rejected) as caught:
            c.bounded_process([sys.executable, "-c", "import sys; print('secret-token',file=sys.stderr); sys.exit(2)"], None, 2)
        self.assertEqual(str(caught.exception), "kubectl_failed")

    def test_timeout_size_and_stdin_block_are_bounded(self):
        for code, payload, timeout, reason in (("import time; time.sleep(2)", None, .1, "request_timeout"), ("import sys; sys.stdout.write('x'*3000000)", None, 2, "response_size"), ("import time; time.sleep(2)", b"x" * c.MAX_INPUT, .1, "request_timeout")):
            with self.subTest(reason=reason), self.assertRaisesRegex(c.Rejected, reason):
                c.bounded_process([sys.executable, "-c", code], payload, timeout)

    @unittest.skipUnless(hasattr(os, "fork"), "POSIX child-process regression")
    def test_exited_parent_cannot_leave_child_holding_stdout(self):
        with tempfile.TemporaryDirectory() as directory:
            pid_path = Path(directory) / "pid"
            code = "import os,sys,time; pid=os.fork(); (open(sys.argv[1],'w').write(str(os.getpid())),time.sleep(30)) if pid==0 else os._exit(0)"
            try:
                with self.assertRaisesRegex(c.Rejected, "request_timeout"):
                    c.bounded_process([sys.executable, "-c", code, str(pid_path)], None, .3)
                pid = int(pid_path.read_text())
                for _ in range(30):
                    proc = Path(f"/proc/{pid}/stat")
                    if not proc.exists() or proc.read_text().split()[2] == "Z":
                        break
                    time.sleep(.02)
                else:
                    self.fail("Child process survived timeout")
            finally:
                if pid_path.exists():
                    try:
                        os.kill(int(pid_path.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_call_and_wall_time_budgets(self):
        clock = Clock()
        budget = c.Budget(20, clock)
        for _ in range(c.MAX_CALLS):
            budget.call_timeout()
        with self.assertRaisesRegex(c.Rejected, "call_budget"):
            budget.call_timeout()
        clock.now = 21
        with self.assertRaisesRegex(c.Rejected, "time_budget"):
            budget.remaining()


class TransportTests(IsolatedGenerators):
    def setUp(self):
        super().setUp()
        self.calls = []
        self.output = b"{}"
        self.config = type("Config", (), {"path": "/proc/self/fd/99", "fd": 99})()
        def process(argv, payload, timeout, **kwargs):
            self.calls.append((argv, payload, timeout, kwargs))
            return self.output
        self.transport = c.Kubectl(target(), self.config, process)
        self.budget = c.Budget(100)

    def test_no_raw_config_read_and_every_call_binds_context_and_fd(self):
        self.output = SERVER.encode()
        self.assertEqual(self.transport.server(self.budget), SERVER)
        self.assertNotIn("--raw", self.calls[-1][0])
        self.output = b"{}"
        self.transport.get("/version", self.budget)
        for argv, _, timeout, kwargs in self.calls:
            self.assertEqual(argv[:3], ["kubectl", "--kubeconfig=/proc/self/fd/99", "--context=" + c.CONTEXT])
            self.assertEqual(kwargs, {"pass_fds": (99,)})
            self.assertLessEqual(timeout, c.REQUEST_SECONDS)

    def test_no_arbitrary_get_or_write_paths(self):
        for path in ("/api/v1/secrets", "https://evil", "/api/v1/nodes/worker", "/apis/rbac.authorization.k8s.io/v1/clusterroles"):
            with self.subTest(path=path), self.assertRaises(c.Rejected):
                self.transport.get(path, self.budget)
        with self.assertRaises(c.Rejected):
            self.transport.delete("/api/v1/namespaces/default", SYSTEM_UID, "1", self.budget)
        self.assertFalse(self.calls)

    def test_create_payload_cannot_become_deployer(self):
        collection, body = c.fixtures(RUN_ID)[0]
        self.transport.create(collection, body, self.budget)
        for field, value in (("kind", "Pod"), ("spec", {"containers": []})):
            wrong = deepcopy(body)
            wrong[field] = value
            with self.assertRaises(c.Rejected):
                self.transport.create(collection, wrong, self.budget)
        collection, workload = c.fixtures(RUN_ID)[4]
        workload["metadata"]["ownerReferences"] = [{"apiVersion": "v1", "kind": "Namespace", "name": target().namespace, "uid": SYSTEM_UID}]
        with self.assertRaises(c.Rejected):
            self.transport.create(collection, workload, self.budget)

    def test_uid_and_resource_version_precondition_is_delete_body(self):
        collection, body = c.fixtures(RUN_ID)[0]
        path = c.object_path(collection, body)
        self.transport.delete(path, SYSTEM_UID, "42", self.budget)
        argv, payload, _, _ = self.calls[-1]
        self.assertEqual(argv[-3:], ["delete", "--raw=" + path, "--filename=-"])
        self.assertEqual(json.loads(payload)["preconditions"], {"uid": SYSTEM_UID, "resourceVersion": "42"})
        self.assertNotIn("--force", argv)

    def test_only_successful_empty_named_get_means_not_found(self):
        self.output = b""
        path = next(iter(self.transport.paths))
        self.assertIsNone(self.transport.get(path, self.budget))
        self.assertIn("--ignore-not-found=true", self.calls[-1][0])
        self.output = b"{bad json"
        with self.assertRaises(c.Rejected):
            self.transport.get(path, self.budget)


class RunnerTests(IsolatedGenerators):
    def test_synthetic_happy_path_records_only_bounded_claims_and_cleans_everything(self):
        report, api = self.run_fake()
        self.assertEqual(report["status"], "passed_bounded_cpu_integration", report)
        self.assertEqual(report["cleanup"], {"status": "complete", "deleted": 6})
        self.assertFalse(api.objects)
        self.assertFalse(report["production_pass"])
        self.assertEqual(report["unverified"], c.UNVERIFIED)
        self.assertEqual(report["source"], SOURCE)
        cases = {entry["case"] for entry in report["observations"]}
        self.assertTrue({"clusterqueue_active", "localqueue_active", "cpu_quota_admitted", "cpu_quota_pending", "quota_release_and_readmission", "no_pods_or_jobs"} <= cases)
        self.assertNotIn(SYSTEM_UID, json.dumps(report))
        self.assertNotIn(SERVER, json.dumps(report))
        posts = [body for method, _, body in api.calls if method == "POST"]
        self.assertEqual(len(posts), 6)
        for obj in posts:
            if obj["kind"] == "Workload":
                self.assertNotIn("ownerReferences", obj["metadata"])
        self.assertFalse(any(method in ("PATCH", "PUT") for method, _, _ in api.calls))

    def test_every_guard_failure_prevents_all_mutation(self):
        mutations = [lambda a: setattr(a, "server_value", "https://127.0.0.1:7443"),
                     lambda a: a.fixed["/version"].update(gitVersion="v1.31.4"),
                     lambda a: a.fixed["/api/v1/namespaces/kube-system"]["metadata"].update(uid=meta("wrong", 999)["uid"]),
                     lambda a: a.fixed["/api/v1/namespaces/kube-system"]["metadata"].update(labels={}),
                     lambda a: a.fixed["/api/v1/nodes"]["metadata"].update(**{"continue": "secret-next-page"}),
                     lambda a: a.fixed["/api/v1/nodes"]["items"].append(deepcopy(a.fixed["/api/v1/nodes"]["items"][0])),
                     lambda a: a.fixed["/api/v1/nodes"]["items"][0]["status"]["allocatable"].update(**{"nvidia.com/gpu": "0"}),
                     lambda a: a.fixed[c.ROOT].update(resources=[]),
                     lambda a: a.fixed[c.ROOT]["resources"].append({"name": "workloads"}),
                     lambda a: a.fixed["/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager"]["status"].update(observedGeneration=0),
                     lambda a: a.fixed["/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager"]["status"].update(observedGeneration=True),
                     lambda a: a.fixed["/apis/apps/v1/namespaces/kueue-system/deployments/kueue-controller-manager"]["spec"]["template"]["spec"]["containers"][0].update(image="unpinned:latest")]
        for mutate in mutations:
            api = FakeAPI()
            mutate(api)
            report, api = self.run_fake(api)
            with self.subTest(failure=report.get("failure")):
                self.assertEqual(report["status"], "failed")
                self.assertFalse(any(method in ("POST", "DELETE") for method, _, _ in api.calls))

    def test_existing_targets_are_never_adopted_overwritten_or_deleted(self):
        for collection, desired in c.fixtures(RUN_ID):
            api = FakeAPI()
            api.objects[c.object_path(collection, desired)] = deepcopy(desired)
            report, api = self.run_fake(api)
            self.assertEqual(report.get("failure"), "target_already_exists")
            self.assertFalse(any(method in ("POST", "DELETE") for method, _, _ in api.calls))

    def test_stale_active_condition_never_passes(self):
        api = FakeAPI()
        def get(path, actual):
            if actual and actual.get("kind") == "ClusterQueue":
                actual["status"]["conditions"][0]["observedGeneration"] = 0
            return actual
        api.on_get = get
        report, _ = self.run_fake(api)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["cleanup"]["status"], "complete")
        self.assertNotIn("clusterqueue_active", [entry["case"] for entry in report["observations"]])

    def test_duplicate_condition_and_changed_generation_never_pass(self):
        for change in (lambda o: o["status"]["conditions"].append(condition("Active")), lambda o: o["metadata"].update(generation=2)):
            api = FakeAPI()
            def get(path, actual):
                if actual and actual.get("kind") == "ClusterQueue":
                    change(actual)
                return actual
            api.on_get = get
            report, _ = self.run_fake(api)
            self.assertEqual(report["status"], "failed")

    def test_reserved_without_admitted_and_wrong_reservation_cannot_pass(self):
        changes = [lambda o: o["status"].update(conditions=[condition("QuotaReserved")]),
                   lambda o: o["status"]["admission"].update(clusterQueue="foreign"),
                   lambda o: o["status"]["admission"]["podSetAssignments"][0]["flavors"].update(cpu="foreign"),
                   lambda o: o["status"]["admission"]["podSetAssignments"][0].update(resourceUsage={"cpu": "2", "memory": "16Mi"})]
        for change in changes:
            api = FakeAPI()
            def get(path, actual):
                if actual and actual.get("kind") == "Workload" and actual["metadata"]["name"] == "cpu-admitted":
                    change(actual)
                return actual
            api.on_get = get
            report, _ = self.run_fake(api)
            self.assertEqual(report["status"], "failed")
            self.assertNotIn("cpu_quota_admitted", [entry["case"] for entry in report["observations"]])

    def test_missing_queue_accounting_and_unexpected_workload_execution_fail(self):
        for pods in (False, True):
            api = FakeAPI()
            if pods:
                api.fixed["/api/v1/namespaces/" + target().namespace + "/pods"]["items"] = [{"kind": "Pod"}]
            else:
                def get(path, actual):
                    if actual and actual.get("kind") == "ClusterQueue":
                        actual["status"].pop("flavorsReservation", None)
                    return actual
                api.on_get = get
            report, _ = self.run_fake(api)
            self.assertEqual(report["status"], "failed")

    def test_cleanup_failure_never_becomes_success_or_deletes_namespace(self):
        api = FakeAPI()
        def fail(path, actual):
            if actual["kind"] == "LocalQueue":
                raise c.Rejected("kubectl_failed")
        api.on_delete = fail
        report, api = self.run_fake(api)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["cleanup"]["status"], "incomplete")
        self.assertIn("/api/v1/namespaces/" + target().namespace, api.objects)

    def test_accepted_but_stuck_delete_is_not_completion(self):
        api = FakeAPI()
        api.hold_deletions = True
        report, _ = self.run_fake(api)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(report["cleanup"]["status"], "incomplete")
        self.assertFalse(any(method == "DELETE" and path == "/api/v1/namespaces/" + target().namespace for method, path, _ in api.calls))

    def test_foreign_replacement_refuses_delete_and_cascade(self):
        api = FakeAPI()
        def get(path, actual):
            if actual and actual.get("kind") == "LocalQueue":
                actual["metadata"]["uid"] = meta("foreign", 999)["uid"]
            return actual
        api.on_get = get
        report, api = self.run_fake(api)
        self.assertEqual(report.get("failure"), "ownership_changed")
        self.assertEqual(report["cleanup"]["status"], "incomplete")
        self.assertFalse(any(method == "DELETE" and "localqueues" in path for method, path, _ in api.calls))

    def test_unexpected_pod_prevents_namespace_cascade(self):
        api = FakeAPI()
        api.fixed["/api/v1/namespaces/" + target().namespace + "/pods"]["items"] = [{"kind": "Pod"}]
        report, api = self.run_fake(api)
        self.assertEqual(report["cleanup"]["reason"], "unexpected_pod_or_job")
        self.assertFalse(any(method == "DELETE" and path == "/api/v1/namespaces/" + target().namespace for method, path, _ in api.calls))

    def test_delayed_delete_is_polled_until_confirmed_absent(self):
        api = FakeAPI()
        api.hold_deletions = True
        polls = {}
        def get(path, actual):
            if actual and actual.get("metadata", {}).get("deletionTimestamp"):
                polls[path] = polls.get(path, 0) + 1
                if polls[path] >= 2:
                    api.objects.pop(path, None)
                    return None
            return actual
        api.on_get = get
        report, _ = self.run_fake(api)
        self.assertEqual(report["status"], "passed_bounded_cpu_integration", report)
        self.assertEqual(len(polls), 6)

    def test_pending_workload_cannot_claim_admission(self):
        api = FakeAPI()
        def get(path, actual):
            if actual and actual.get("kind") == "Workload" and actual["metadata"]["name"] == "cpu-pending":
                actual.setdefault("status", {})["admission"] = {"clusterQueue": "foreign"}
            return actual
        api.on_get = get
        report, _ = self.run_fake(api)
        self.assertEqual(report.get("failure"), "over_quota_admitted")

    def test_post_mutation_and_foreign_label_cannot_be_accepted(self):
        for change in (lambda o: o["spec"].update(queueingStrategy="BestEffortFIFO"), lambda o: o["metadata"]["labels"].update({c.RUN_LABEL: "foreign"})):
            api = FakeAPI()
            def created(collection, actual):
                if actual["kind"] == "ClusterQueue":
                    change(actual)
                return actual
            api.on_create = created
            report, _ = self.run_fake(api)
            self.assertEqual(report["status"], "failed")
            self.assertEqual(report["cleanup"]["status"], "incomplete")
            self.assertNotIn("clusterqueue_active", [item["case"] for item in report["observations"]])


    def test_uncertain_create_is_not_retried_or_cascade_cleaned(self):
        api = FakeAPI()
        def uncertain(collection, actual):
            if actual["kind"] == "ResourceFlavor":
                raise c.Rejected("request_timeout")
            return actual
        api.on_create = uncertain
        report, api = self.run_fake(api)
        self.assertEqual(report["cleanup"]["reason"], "create_outcome_uncertain")
        self.assertEqual(len([call for call in api.calls if call[0] == "POST"]), 2)
        self.assertFalse(any(method == "DELETE" for method, _, _ in api.calls))

    def test_untrusted_response_exception_content_is_not_evidence(self):
        api = FakeAPI()
        api.on_get = lambda *_: (_ for _ in ()).throw(ValueError("secret-token-do-not-log"))
        report, _ = self.run_fake(api)
        self.assertNotIn("secret-token", json.dumps(report))
        self.assertEqual(report.get("failure"), "response_or_runner_invalid")


class CLITests(IsolatedGenerators):
    def test_real_offline_plan_needs_no_kubectl_or_credentials(self):
        env = dict(os.environ)
        env.pop("DAVANO_KUEUE_API_VERSION", None)
        env["PATH"] = "/no-programs"
        result = subprocess.run([sys.executable, "-m", "acceptance.cluster", "--plan"], cwd=ROOT, env=env, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        plan = json.loads(result.stdout)
        self.assertTrue(all(item["object"]["apiVersion"] == c.API for item in plan["fixtures"][1:]))
        env["DAVANO_KUEUE_API_VERSION"] = "v1beta1"
        result = subprocess.run([sys.executable, "-m", "acceptance.cluster", "--plan"], cwd=ROOT, env=env, capture_output=True, timeout=5)
        self.assertEqual(result.returncode, 2)
        self.assertEqual(json.loads(result.stdout)["reason"], "generator_api_version")

    def test_execute_requires_every_target_input_before_subprocess(self):
        with patch.object(c, "bounded_process", side_effect=AssertionError("No process allowed")), redirect_stdout(io.StringIO()) as out:
            self.assertEqual(c.main(["--execute"]), 2)
        self.assertEqual(json.loads(out.getvalue())["reason"], "explicit_execution_arguments_required")

    def test_existing_evidence_never_overwritten_and_no_cluster_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text(json.dumps(config_body()))
            config_path.chmod(0o600)
            evidence = Path(directory) / "evidence.json"
            evidence.write_text("preserved")
            args = ["--execute", "--context", c.CONTEXT, "--kubeconfig-json", str(config_path), "--expected-server", SERVER, "--expected-kube-system-uid", SYSTEM_UID, "--expected-kubernetes-version", c.KUBERNETES, "--run-id", RUN_ID, "--evidence", str(evidence)]
            with patch.object(c.Runner, "run", side_effect=AssertionError("Must not run")), redirect_stdout(io.StringIO()):
                self.assertEqual(c.main(args), 2)
            self.assertEqual(evidence.read_text(), "preserved")

    def test_cli_writes_private_sanitized_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "config.json"
            config_path.write_text(json.dumps(config_body()))
            config_path.chmod(0o600)
            evidence = Path(directory) / "evidence.json"
            args = ["--execute", "--context", c.CONTEXT, "--kubeconfig-json", str(config_path), "--expected-server", SERVER, "--expected-kube-system-uid", SYSTEM_UID, "--expected-kubernetes-version", c.KUBERNETES, "--run-id", RUN_ID, "--evidence", str(evidence)]
            report, _ = self.run_fake()
            with patch.object(c.Runner, "run", return_value=report), redirect_stdout(io.StringIO()) as out:
                self.assertEqual(c.main(args), 0)
            self.assertEqual(evidence.stat().st_mode & 0o777, 0o600)
            self.assertEqual(json.loads(evidence.read_text()), report)
            self.assertNotIn(SYSTEM_UID, out.getvalue())
            self.assertNotIn("BEGIN PRIVATE KEY", evidence.read_text())


if __name__ == "__main__":
    unittest.main()
