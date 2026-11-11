import json
import tempfile
import unittest
from pathlib import Path
from fastapi.testclient import TestClient
from davano.app import create_app
from davano.inventory import InventoryUnavailable, normalize
from davano.store import Store
from test_inventory import node, pod

POOL={"id":"test","name":"Test pool","selector":{"davano.dev/pool":"test"},"vendor":"NVIDIA","mode":"whole-gpu"}
POLICY={"id":"test-policy","name":"Test policy","pool_id":"test","namespace":"research","max_gpus_per_workload":8,"priority":"normal"}

class MutableInventory:
    source="kubernetes"
    def __init__(self,nodes=None,pods=None): self.nodes=nodes if nodes is not None else [node()]; self.pods=pods or [];self.failed=False
    def snapshot(self):
        if self.failed: raise InventoryUnavailable("Inventory test unavailable")
        nodes,workloads=normalize(self.nodes,self.pods)
        return {"nodes":nodes,"workloads":workloads,"observed_at":"2026-11-11T00:00:00Z","warnings":[]}

class APITests(unittest.TestCase):
    def setUp(self):
        self.inventory=MutableInventory()
        self.client=TestClient(create_app(source="kubernetes",db_path=":memory:",inventory=self.inventory));self.client.__enter__()
        self.assertEqual(self.client.post('/api/v1/pools',json=POOL).status_code,201)
        self.assertEqual(self.client.post('/api/v1/policies',json=POLICY).status_code,201)
    def tearDown(self):self.client.__exit__(None,None,None)
    def evaluate(self,**kwargs):return self.client.post('/api/v1/evaluate',json={"pool_id":"test","namespace":"research","gpu_count":4,"vendor":"NVIDIA",**kwargs})

    def test_health_and_ready(self):
        self.assertEqual(self.client.get('/healthz').status_code,200)
        self.assertEqual(self.client.get('/readyz').json()['source'],'kubernetes')

    def test_state_is_truthful(self):
        s=self.client.get('/api/v1/state').json()
        self.assertEqual(s['meta']['source'],'kubernetes');self.assertTrue(s['meta']['read_only_cluster'])
        self.assertEqual(s['meta']['enforcement'],'advisory-only')
        self.assertEqual(s['summary']['whole_gpu_capacity'],8)
        self.assertEqual(s['pools'][0]['node_ids'],['one'])
        self.assertEqual(s['integrations'][1]['status'],'available-disabled')

    def test_fit_never_allocates_or_submits(self):
        before=self.client.get('/api/v1/state').json()
        result=self.evaluate().json()
        self.assertEqual(result['verdict'],'fits');self.assertFalse(result['allocated']);self.assertFalse(result['submitted']);self.assertTrue(result['advisory_only'])
        self.assertEqual(before['summary'],self.client.get('/api/v1/state').json()['summary'])

    def test_no_cross_node_aggregation(self):
        self.inventory.nodes=[node('one',count=2),node('two',count=2)]
        self.assertEqual(self.evaluate(gpu_count=4).json()['verdict'],'blocked')

    def test_cap_and_missing_policy_block(self):
        self.assertEqual(self.evaluate(gpu_count=9).json()['verdict'],'blocked')
        self.assertEqual(self.evaluate(namespace='other').json()['verdict'],'blocked')

    def test_strictest_policy_wins(self):
        self.client.post('/api/v1/policies',json={**POLICY,'id':'stricter','max_gpus_per_workload':2})
        self.assertEqual(self.evaluate().json()['verdict'],'blocked')

    def test_mismatching_vendor_blocked(self):
        self.assertEqual(self.evaluate(vendor='AMD').json()['verdict'],'blocked')

    def test_notready_not_candidate(self):
        self.inventory.nodes=[node(ready=False)]
        self.assertEqual(self.evaluate().json()['candidate_nodes'],[])

    def test_shared_and_unknown_excluded(self):
        for labels in ({'nvidia.com/gpu.replicas':'4'},{'davano.dev/allocation-mode':'unknown'}):
            self.inventory.nodes=[node(labels=labels)]
            self.assertEqual(self.evaluate().json()['verdict'],'blocked')
            self.assertEqual(self.client.get('/api/v1/state').json()['summary']['whole_gpu_capacity'],0)

    def test_unknown_counts_return_unknown(self):
        self.inventory.pods=[pod('oops')]
        self.assertEqual(self.evaluate().json()['verdict'],'unknown')

    def test_live_failure_has_no_demo_fallback(self):
        self.inventory.failed=True
        for url in ('/readyz','/api/v1/state'):
            r=self.client.get(url);self.assertEqual(r.status_code,503);self.assertFalse(r.json()['fallback']);self.assertEqual(r.json()['source'],'kubernetes')
        self.assertEqual(self.client.get('/healthz').status_code,200)
        self.assertEqual(self.evaluate().status_code,503)

    def test_integer_validation(self):
        for count in (0,-1,1.5,True,'4',1025):
            with self.subTest(count=count):self.assertEqual(self.evaluate(gpu_count=count).status_code,422)

    def test_duplicate_and_reference(self):
        self.assertEqual(self.client.post('/api/v1/pools',json=POOL).status_code,409)
        self.assertEqual(self.client.post('/api/v1/policies',json={**POLICY,'id':'bad','pool_id':'missing'}).status_code,404)
        self.assertEqual(self.client.delete('/api/v1/pools/test').status_code,409)

    def test_delete_local_config(self):
        self.assertFalse(self.client.delete('/api/v1/policies/test-policy').json()['cluster_changed'])
        self.assertEqual(self.client.delete('/api/v1/pools/test').status_code,200)
        self.assertEqual(self.client.delete('/api/v1/pools/test').status_code,404)

    def test_extra_fields_and_malformed_selectors(self):
        for payload in ({**POOL,'id':'other','allocate':True},{**POOL,'id':'other','selector':{}},{**POOL,'id':'other','selector':{'bad key':'value'}},{**POOL,'id':'other','mode':'mig'}):
            self.assertEqual(self.client.post('/api/v1/pools',json=payload).status_code,422)

    def test_request_body_size_is_bounded(self):
        r=self.client.post('/api/v1/pools',content=b' ' * 70000,headers={'Content-Type':'application/json'})
        self.assertEqual(r.status_code,413)
        def chunks():
            yield b' ' * 40000
            yield b' ' * 40000
        r=self.client.post('/api/v1/pools',content=chunks(),headers={'Content-Type':'application/json'})
        self.assertEqual(r.status_code,413)

    def test_cross_origin_write_rejected(self):
        self.assertEqual(self.client.post('/api/v1/pools',json={**POOL,'id':'other'},headers={'Origin':'https://evil.example'}).status_code,403)
        self.assertEqual(self.client.post('/api/v1/pools',content=json.dumps({**POOL,'id':'other'}),headers={'Content-Type':'text/plain'}).status_code,415)

    def test_pool_overlap_does_not_double_fleet_capacity(self):
        self.client.post('/api/v1/pools',json={**POOL,'id':'overlap'})
        state=self.client.get('/api/v1/state').json()
        self.assertEqual(state['summary']['whole_gpu_capacity'],8)
        self.assertTrue(all(any('Overlapping' in w for w in p['warnings']) for p in state['pools']))

    def test_list_routes_cannot_override_collection(self):
        self.assertEqual(self.client.get('/api/v1/nodes?collection=policies').json()[0]['name'],'one')

class PersistenceTests(unittest.TestCase):
    def test_restart_keeps_configuration_and_audit(self):
        with tempfile.TemporaryDirectory() as td:
            path=str(Path(td)/'state.db')
            with TestClient(create_app(source='kubernetes',db_path=path,inventory=MutableInventory())) as c:c.post('/api/v1/pools',json=POOL)
            with TestClient(create_app(source='kubernetes',db_path=path,inventory=MutableInventory())) as c:
                s=c.get('/api/v1/state').json();self.assertEqual(s['pools'][0]['id'],'test');self.assertEqual(s['events'][0]['type'],'pool.created')
    def test_source_switch_rejected_without_seed_pollution(self):
        with tempfile.TemporaryDirectory() as td:
            path=str(Path(td)/'state.db')
            with TestClient(create_app(source='kubernetes',db_path=path,inventory=MutableInventory())):pass
            with self.assertRaisesRegex(ValueError,'source mismatch'):create_app(source='demo',db_path=path)
            db=Store(path,source='kubernetes');self.assertEqual(db.list('pools'),[]);db.close()
    def test_demo_seeding_idempotent(self):
        with tempfile.TemporaryDirectory() as td:
            path=str(Path(td)/'demo.db')
            for _ in range(2):
                with TestClient(create_app(db_path=path)) as c:
                    s=c.get('/api/v1/state').json();self.assertEqual(s['summary']['whole_gpu_capacity'],36);self.assertEqual(len(s['pools']),3);self.assertEqual(s['meta']['source'],'demo')
