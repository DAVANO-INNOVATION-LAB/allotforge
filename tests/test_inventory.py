import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError
from davano.inventory import (quantity, allocation_mode, normalize, pod_requests, KubernetesInventory, InventoryUnavailable, NoRedirectHandler, open_without_redirect)


def node(name="one", count=8, labels=None, ready=True, taints=None):
    return {"metadata": {"name": name, "labels": {"davano.dev/pool": "test", "davano.dev/allocation-mode": "whole-gpu", **(labels or {})}}, "spec": {"taints": taints or []}, "status": {"conditions": [{"type": "Ready", "status": "True" if ready else "False"}], "capacity": {"nvidia.com/gpu": str(count)}, "allocatable": {"nvidia.com/gpu": str(count)}}}


def pod(count, node_name="one", phase="Running", namespace="research"):
    return {"metadata": {"name": "job", "namespace": namespace}, "spec": {"nodeName": node_name, "containers": [{"resources": {"limits": {"nvidia.com/gpu": str(count)}}}]}, "status": {"phase": phase}}


class InventoryTests(unittest.TestCase):
    def test_quantities(self):
        for raw, expected in [("8",8),("1000m",1),("1k",1000),("1Ki",1024),("1e2",100),("1.5",None),("-1",None),(True,None),("wat",None),("2Mi",None),("1e9999999",None),("1e-9999999",None)]:
            with self.subTest(raw=raw): self.assertEqual(quantity(raw), expected)

    def test_unknown_is_not_whole(self):
        self.assertEqual(allocation_mode("nvidia.com/gpu", {}), "unknown")
        self.assertEqual(allocation_mode("amd.com/gpu", {}), "unknown")

    def test_mode_evidence_and_conflicts(self):
        explicit = {"davano.dev/allocation-mode":"whole-gpu"}
        self.assertEqual(allocation_mode("amd.com/gpu",explicit),"whole-gpu")
        for key, labels, result in [("nvidia.com/mig-1g.10gb",explicit,"mig"),("nvidia.com/gpu.shared",explicit,"shared"),("nvidia.com/gpu",{**explicit,"nvidia.com/mig.strategy":"single"},"mig"),("nvidia.com/gpu",{**explicit,"nvidia.com/gpu.product":"A100-MIG-1g.5gb"},"mig"),("nvidia.com/gpu",{**explicit,"nvidia.com/gpu.replicas":"4"},"shared"),("nvidia.com/gpu",{**explicit,"nvidia.com/gpu.sharing-strategy":"mps"},"shared")]:
            with self.subTest(key=key,labels=labels): self.assertEqual(allocation_mode(key,labels),result)

    def test_terminal_pods_do_not_consume(self):
        nodes, jobs = normalize([node()], [pod(4),pod(8,phase="Succeeded"),pod(8,phase="Failed")])
        self.assertEqual(nodes[0]["gpu_requested"],4)
        self.assertEqual(nodes[0]["gpu_available"],4)
        self.assertEqual(len(jobs),1)

    def test_bound_pending_consumes_unbound_does_not(self):
        n,_=normalize([node()], [pod(2,phase="Pending"),pod(4,node_name="",phase="Pending")])
        self.assertEqual(n[0]["gpu_requested"],2)

    def test_unknown_counts_fail_closed(self):
        n,_=normalize([node()], [pod("garbage")])
        self.assertIsNone(n[0]["gpu_available"])
        self.assertTrue(n[0]["warnings"])

    def test_overrequests_never_negative(self):
        n,_=normalize([node()], [pod(9)])
        self.assertEqual(n[0]["gpu_available"],0)
        self.assertTrue(n[0]["warnings"])

    def test_notready_cordoned_and_tainted(self):
        n=node(); n["spec"]["unschedulable"]=True
        ns,_=normalize([n,node("two",ready=False),node("three",taints=[{"key":"dedicated","effect":"NoSchedule"}])],[])
        self.assertTrue(all(not n["schedulable"] for n in ns))

    def test_mig_not_added_to_whole_capacity(self):
        n=node(); n["status"]["capacity"]["nvidia.com/mig-1g.10gb"]="7";n["status"]["allocatable"]["nvidia.com/mig-1g.10gb"]="7"
        ns,_=normalize([n],[])
        self.assertEqual(ns[0]["gpu_capacity"],8)
        self.assertEqual(ns[0]["mode"],"mixed")

    def test_sidecar_init_effective_request(self):
        p=pod(2)
        def container(count,sidecar=False):
            return {"resources":{"requests":{"nvidia.com/gpu":str(count)}},**({"restartPolicy":"Always"} if sidecar else {})}
        p["spec"]["initContainers"]=[container(3,True),container(4),container(2,True)]
        p["spec"]["overhead"]={"nvidia.com/gpu":"1"}
        # Peak=max(3+4, app2+sidecars5)+overhead1=8, not sum all containers.
        self.assertEqual(pod_requests(p),{"nvidia.com/gpu":8})

    def test_init_peak(self):
        p=pod(2);p["spec"]["initContainers"]=[{"resources":{"limits":{"nvidia.com/gpu":"6"}}}]
        self.assertEqual(pod_requests(p)["nvidia.com/gpu"],6)


class LiveAdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.token=Path(self.temp.name)/"token"; self.token.write_text("test-only")
    def tearDown(self): self.temp.cleanup()
    def adapter(self,opener): return KubernetesInventory(api="https://kubernetes.example",token_path=str(self.token),ca_path="test-ca",opener=opener)

    def test_requires_https_without_credentials_or_path(self):
        for api in ("http://localhost:8001","https://name:secret@example.test","https://example.test/prefix","https://example.test?token=x"):
            with self.subTest(api=api),self.assertRaises(InventoryUnavailable): KubernetesInventory(api=api)

    @patch("davano.inventory.ssl.create_default_context")
    def test_pagination_and_get_only(self,ssl_context):
        seen=[]
        def opener(req,**kwargs):
            seen.append(req)
            data={"items":[node()],"metadata":{"continue":"next+token"}} if len(seen)==1 else {"items":[],"metadata":{}}
            return io.BytesIO(json.dumps(data).encode())
        items=self.adapter(opener)._list("/api/v1/nodes")
        self.assertEqual(len(items),1);self.assertEqual(len(seen),2)
        self.assertTrue(all(r.method=="GET" for r in seen))
        self.assertIn("continue=next%2Btoken",seen[1].full_url)
        self.assertEqual(seen[0].headers["Authorization"],"Bearer test-only")
        ssl_context.assert_called_with(cafile="test-ca")

    @patch("davano.inventory.ssl.create_default_context")
    def test_rotation_reads_current_token(self,_):
        headers=[]
        def opener(req,**kwargs): headers.append(req.headers["Authorization"]);return io.BytesIO(b'{"items":[],"metadata":{}}')
        adapter=self.adapter(opener);adapter._list('/api/v1/nodes');self.token.write_text('rotated-test');adapter._list('/api/v1/nodes')
        self.assertEqual(headers,['Bearer test-only','Bearer rotated-test'])

    @patch("davano.inventory.ssl.create_default_context")
    def test_http_error_sanitizes_secret(self,_):
        def opener(req,**kwargs): raise HTTPError(req.full_url,403,"test-only",{},None)
        with self.assertRaises(InventoryUnavailable) as ctx:self.adapter(opener).snapshot()
        self.assertIn("403",str(ctx.exception));self.assertNotIn("test-only",str(ctx.exception))

    @patch("davano.inventory.ssl.create_default_context")
    def test_no_partial_inventory_on_failure(self,_):
        def opener(req,**kwargs):
            if '/pods?' in req.full_url: raise HTTPError(req.full_url,403,"forbidden",{},None)
            return io.BytesIO(json.dumps({"items":[node()],"metadata":{}}).encode())
        with self.assertRaises(InventoryUnavailable):self.adapter(opener).snapshot()


class RedirectSafetyTests(unittest.TestCase):
    def test_every_redirect_is_rejected(self):
        from urllib.request import Request
        handler=NoRedirectHandler()
        for target in ('https://other.example/', 'http://kubernetes.example/', 'https://kubernetes.example/next'):
            self.assertIsNone(handler.redirect_request(Request('https://kubernetes.example',headers={'Authorization':'Bearer secret'}),None,302,'Found',{},target))

    @patch('davano.inventory.build_opener')
    def test_default_transport_installs_no_redirect_handler(self, builder):
        from urllib.request import Request
        request=Request('https://kubernetes.example')
        open_without_redirect(request,context=None,timeout=10)
        self.assertTrue(any(isinstance(arg,NoRedirectHandler) for arg in builder.call_args.args))
        builder.return_value.open.assert_called_once_with(request,timeout=10)
