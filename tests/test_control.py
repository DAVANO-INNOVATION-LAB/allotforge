"""Control-plane unit/integration tests with an in-memory Kubernetes API double.

These verify reconciliation and HTTP contracts, not Kubernetes CEL admission,
Kueue behavior, actual GPU scheduling or production compatibility.
"""
import copy
import json
import os
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.parse import urlsplit
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import ValidationError
from davano.control_models import ManagedPoolCreate, ManagedPolicyCreate
from davano.control_api import install_control_api
from davano.kube import KubeClient, ApiError, OwnershipConflict, OWNER_LABEL, ROOT
from davano.reconciler import Reconciler, FINALIZER, KUEUE, ADMISSION, pool_name, policy_name, cr_path, native_policy, pool_resources
from davano.operator import Lease, LeaderClient

SELECTOR = {'davano.io.node-restriction.kubernetes.io/pool': 'a'}
def pool(name='a', uid='11111111-1111-1111-1111-111111111111'):
    return {'apiVersion':'fleet.davano.io/v1alpha1','kind':'GPUPool','metadata':{'name':name,'uid':uid,'resourceVersion':'1','generation':1},'spec':{'nodeSelector':dict(SELECTOR),'resourceName':'nvidia.com/gpu','nominalQuota':8,'mode':'whole-gpu'}}
def policy():
    return {'apiVersion':'fleet.davano.io/v1alpha1','kind':'GPUAccessPolicy','metadata':{'name':'research','namespace':'team','uid':'22222222-2222-2222-2222-222222222222','resourceVersion':'1','generation':1},'spec':{'poolRef':'a','gpuQuota':4,'maxGPUsPerPod':2,'subjects':[{'kind':'Group','name':'research'}]}}
def node():
    return {'apiVersion':'v1','kind':'Node','metadata':{'name':'worker','labels':{**SELECTOR,'nvidia.com/gpu.sharing-strategy':'none'}},'status':{'allocatable':{'nvidia.com/gpu':'8','cpu':'16','memory':'64Gi'}}}

class FakeKube(KubeClient):
    def __init__(self):
        self.objects = {}
        self.writes = []
        self.fail = None
        self.native_ready = True
        self.inject_warnings = False
        self.rv = 10
    def seed(self, path, body):
        self.objects[path] = copy.deepcopy(body)
    def request(self, method, path, body=None):
        path = urlsplit(path).path
        if self.fail and (self.fail == path or self.fail == method):
            raise ApiError(503,'unavailable')
        if method == 'GET':
            if path in self.objects:
                return copy.deepcopy(self.objects[path])
            matches = [copy.deepcopy(o) for k,o in self.objects.items() if k.rsplit('/',1)[0] == path]
            if path == ROOT + '/gpuaccesspolicies':
                matches = [copy.deepcopy(o) for o in self.objects.values() if o['kind']=='GPUAccessPolicy']
            if matches or path in ['/api/v1/nodes', ROOT+'/gpupools', ROOT+'/gpuaccesspolicies'] or path.endswith('/pods'):
                return {'items':matches}
            raise ApiError(404,'missing')
        self.writes.append((method,path,copy.deepcopy(body)))
        if method == 'DELETE':
            if path not in self.objects:
                raise ApiError(404,'missing')
            current = self.objects[path]
            for k,v in body['preconditions'].items():
                if current['metadata'][k] != v:
                    raise ApiError(409,'conflict')
            del self.objects[path]
            return {'status':'Success'}
        status_update = path.endswith('/status')
        actual_path = path.removesuffix('/status') if status_update else path
        if method == 'POST':
            actual_path += '/' + body['metadata']['name']
            if actual_path in self.objects:
                raise ApiError(409,'exists')
        elif actual_path not in self.objects:
            raise ApiError(404,'missing')
        current = self.objects.get(actual_path)
        if method == 'PUT' and body['metadata'].get('resourceVersion') != current['metadata'].get('resourceVersion'):
            raise ApiError(409,'conflict')
        value = copy.deepcopy(body)
        self.rv += 1
        value['metadata'].setdefault('uid', f'generated-{self.rv}')
        value['metadata']['resourceVersion'] = str(self.rv)
        value['metadata'].setdefault('generation',1)
        if current and not status_update and current.get('spec') != value.get('spec'):
            value['metadata']['generation'] = current['metadata'].get('generation',1)+1
        if self.native_ready:
            if value['kind'] in ('ClusterQueue','LocalQueue'):
                value['status']={'conditions':[{'type':'Active','status':'True','observedGeneration':value['metadata']['generation']}]}
            if value['kind']=='ResourceQuota':
                value['status']={'hard':value['spec']['hard']}
            if value['kind']=='ValidatingAdmissionPolicy':
                value['status']={'observedGeneration':value['metadata']['generation'],'typeChecking':{'expressionWarnings':[{'warning':'bad CEL'}] if self.inject_warnings else []}}
        self.objects[actual_path] = value
        return copy.deepcopy(value)

def fixture():
    c=FakeKube();p=pool();a=policy()
    c.seed(cr_path(p),p);c.seed(cr_path(a),a);c.seed('/api/v1/nodes/worker',node());c.seed('/api/v1/namespaces/team',{'apiVersion':'v1','kind':'Namespace','metadata':{'name':'team'}})
    return c,p,a

class ReconcileTests(unittest.TestCase):
    def test_full_reconciliation_persists_real_native_objects(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle()
        kinds={o['kind'] for o in c.objects.values()}
        self.assertTrue({'ResourceFlavor','ClusterQueue','LocalQueue','ResourceQuota','ValidatingAdmissionPolicy','ValidatingAdmissionPolicyBinding'} <= kinds)
        for cr in (p,a):
            actual=c.get(cr_path(cr));self.assertIn(FINALIZER,actual['metadata']['finalizers'])
        self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['status'],'True')
        self.assertEqual(c.get(cr_path(a))['status']['conditions'][0]['reason'],'WorkloadGrantsDisabled')
        self.assertNotIn('RoleBinding',kinds)
        self.assertFalse(any('nodes/' in path for method,path,_ in c.writes))
    def test_cpu_memory_quotas_are_observed_not_fabricated(self):
        c,p,a=fixture();Reconciler(c,['team']).cycle();q=c.get(KUEUE+'/clusterqueues/'+pool_name(p));resources=q['spec']['resourceGroups'][0]['flavors'][0]['resources'];quotas={r['name']:r['nominalQuota'] for r in resources};self.assertEqual(quotas,{'cpu':'16','memory':'68719476736','nvidia.com/gpu':'8'})
    def test_missing_cpu_memory_blocks_readiness(self):
        c,p,a=fixture();del c.objects['/api/v1/nodes/worker']['status']['allocatable']['cpu'];Reconciler(c,['team']).cycle();self.assertFalse(any(o['kind']=='ClusterQueue' for o in c.objects.values()))
    def test_idempotent_no_native_spec_writes(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();c.writes=[];r.cycle()
        self.assertTrue(c.writes)
        self.assertTrue(all(path.endswith('/status') for _,path,_ in c.writes))
    def test_foreign_resource_cannot_be_adopted(self):
        c,p,a=fixture();c.seed(KUEUE+'/resourceflavors/'+pool_name(p),{'apiVersion':'kueue.x-k8s.io/v1beta1','kind':'ResourceFlavor','metadata':{'name':pool_name(p),'uid':'foreign','resourceVersion':'1'},'spec':{}})
        Reconciler(c,['team']).cycle()
        self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['reason'],'OwnershipConflict')
        self.assertFalse(any(o['kind']=='RoleBinding' for o in c.objects.values()))
    def test_existing_finalizers_are_preserved(self):
        c,p,a=fixture();Reconciler(c,['team']).cycle();path=KUEUE+'/clusterqueues/'+pool_name(p);c.objects[path]['metadata']['finalizers']=['kueue.x-k8s.io/resource-in-use'];c.objects[path]['spec']['queueingStrategy']='BestEffortFIFO';Reconciler(c,['team']).cycle();self.assertEqual(c.get(path)['metadata']['finalizers'],['kueue.x-k8s.io/resource-in-use'])
    def test_failed_full_list_never_mutates(self):
        c,_,_=fixture();c.fail='/api/v1/nodes'
        with self.assertRaises(ApiError): Reconciler(c,['team']).cycle()
        self.assertTrue(all(path.endswith('/status') for _,path,_ in c.writes))
    def test_shared_selected_node_blocks_pool(self):
        c,p,a=fixture();c.objects['/api/v1/nodes/worker']['metadata']['labels']['nvidia.com/gpu.sharing-strategy']='time-slicing';Reconciler(c,['team']).cycle();self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['status'],'False');self.assertFalse(any(o['kind']=='ClusterQueue' for o in c.objects.values()))
    def test_overlapping_selector_blocks_both(self):
        c,p,a=fixture();other=pool('b','33333333-3333');c.seed(cr_path(other),other);Reconciler(c,['team']).cycle();self.assertFalse(any(o['kind']=='ResourceFlavor' for o in c.objects.values()))
    def test_overcommit_blocks_pool(self):
        c,p,a=fixture();c.objects[cr_path(p)]['spec']['nominalQuota']=9;Reconciler(c,['team']).cycle();self.assertIn('exceeds',c.get(cr_path(p))['status']['conditions'][0]['message'])
    def test_unmanaged_namespace_no_writes(self):
        c,p,a=fixture();Reconciler(c,['other']).cycle();self.assertNotIn('status',c.get(cr_path(a)));self.assertFalse(any(o['kind']=='ResourceQuota' for o in c.objects.values()))
    def test_duplicate_namespace_policy_blocks_grants(self):
        c,p,a=fixture();other=copy.deepcopy(a);other['metadata'].update(name='second',uid='unique');c.seed(cr_path(other),other);Reconciler(c,['team']).cycle();self.assertFalse(any(o['kind']=='RoleBinding' for o in c.objects.values()))
    def test_admission_type_warning_blocks_grants(self):
        c,p,a=fixture();c.inject_warnings=True;Reconciler(c,['team']).cycle();self.assertEqual(c.get(cr_path(a))['status']['conditions'][0]['reason'],'WaitingForAdmission');self.assertFalse(any(o['kind']=='RoleBinding' for o in c.objects.values()))
    def test_native_readiness_not_fabricated(self):
        c,p,a=fixture();c.native_ready=False;Reconciler(c,['team']).cycle();self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['reason'],'WaitingForKueue');self.assertEqual(c.get(cr_path(a))['status']['conditions'][0]['status'],'False')
    def test_pool_deletion_blocked_by_policy(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();c.objects[cr_path(p)]['metadata']['deletionTimestamp']='2026-01-01T00:00:00Z';r.cycle();self.assertIn(FINALIZER,c.get(cr_path(p))['metadata']['finalizers']);self.assertIn(KUEUE+'/clusterqueues/'+pool_name(p),c.objects)
    def test_policy_deletion_retains_guardrails_while_pods_live(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();c.objects[cr_path(a)]['metadata']['deletionTimestamp']='2026-01-01T00:00:00Z';c.seed('/api/v1/namespaces/team/pods/running',{'kind':'Pod','metadata':{'name':'running'},'status':{'phase':'Running'}});r.cycle();self.assertIn(FINALIZER,c.get(cr_path(a))['metadata']['finalizers']);self.assertTrue(any(o['kind']=='ResourceQuota' for o in c.objects.values()))
    def test_policy_deletion_retains_queued_workloads(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();path=KUEUE+'/namespaces/team/localqueues/'+policy_name(a);c.objects[path]['status']['pendingWorkloads']=1;c.objects[cr_path(a)]['metadata']['deletionTimestamp']='2026-01-01T00:00:00Z';r.cycle();self.assertIn(FINALIZER,c.get(cr_path(a))['metadata']['finalizers']);self.assertIn(path,c.objects)
    def test_deletion_cleans_owned_native_resources(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();c.objects[cr_path(a)]['metadata']['deletionTimestamp']='2026-01-01T00:00:00Z';r.cycle();self.assertNotIn(FINALIZER,c.get(cr_path(a))['metadata']['finalizers']);self.assertFalse(any(o['kind']=='ResourceQuota' for o in c.objects.values()))
    def test_foreign_replacement_never_deleted(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();path='/api/v1/namespaces/team/resourcequotas/'+policy_name(a);c.objects[path]['metadata']['labels'][OWNER_LABEL]='foreign';c.objects[cr_path(a)]['metadata']['deletionTimestamp']='2026-01-01T00:00:00Z';r.cycle();self.assertIn(path,c.objects);self.assertIn(FINALIZER,c.get(cr_path(a))['metadata']['finalizers'])
    def test_namespace_selector_never_empty_all_namespaces(self):
        q=pool_resources(pool(),[])[-1][1];self.assertNotEqual(q['spec']['namespaceSelector'],{});self.assertEqual(q['spec']['namespaceSelector']['matchExpressions'][0]['operator'],'In')
    def test_native_policy_scopes_namespace_and_fail_closed(self):
        vap,binding=native_policy(policy(),pool());self.assertEqual(vap['spec']['failurePolicy'],'Fail');self.assertEqual(binding['spec']['validationActions'],['Deny']);self.assertEqual(vap['spec']['matchConditions'][0]['expression'],'request.namespace == "team"');self.assertIn('!has(object.spec.nodeName)',vap['spec']['validations'][3]['expression']);self.assertEqual(vap['spec']['matchConstraints']['resourceRules'][0]['operations'],['CREATE'])

    def test_native_policy_denies_runtimeclass_even_without_container_gpu(self):
        vap,_=native_policy(policy(),pool())
        guard=next(v for v in vap['spec']['validations'] if v['message']=='RuntimeClass is unsupported in managed GPU namespaces')
        self.assertEqual(guard['expression'],'!has(object.spec.runtimeClassName)')
        # Do not condition this on usesGPU: RuntimeClass may inject overhead into
        # an otherwise CPU-only Pod. Nor hide an unsupported field behind dyn().
        self.assertTrue(all('object.spec.overhead' not in v['expression'] for v in vap['spec']['validations']))

    def test_subjects_never_grant_workload_permissions(self):
        c,p,a=fixture();Reconciler(c,['team']).cycle();status=c.get(cr_path(a))['status'];self.assertEqual(status['workloadGrants'],'disabled');self.assertEqual(status['conditions'][0]['status'],'False');self.assertTrue(any(x['type']=='WorkloadAccess' and x['status']=='False' for x in status['conditions']));self.assertTrue(any(x['type']=='GuardrailsReady' and x['status']=='True' for x in status['conditions']));self.assertFalse(any(o['kind']=='RoleBinding' for o in c.objects.values()))
    def test_legacy_owned_grant_revoked_even_if_nodes_unavailable(self):
        c,p,a=fixture();path='/apis/rbac.authorization.k8s.io/v1/namespaces/team/rolebindings/'+policy_name(a);c.seed(path,{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':{'name':policy_name(a),'namespace':'team','uid':'legacy-grant','resourceVersion':'1','labels':{OWNER_LABEL:a['metadata']['uid'],'app.kubernetes.io/managed-by':'davano-controller'}},'subjects':a['spec']['subjects'],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':'davano-workload-submitter'}});c.fail='/api/v1/nodes'
        with self.assertRaises(ApiError):Reconciler(c,['team']).cycle()
        self.assertNotIn(path,c.objects);self.assertFalse(any(method in ('POST','PUT') and not target.endswith('/status') for method,target,_ in c.writes))
    def test_same_name_replacement_is_not_finalized_or_status_updated(self):
        c,p,a=fixture();base=c.request;replaced=[False]
        def replace(method,path,body=None):
            if method=='GET' and path==cr_path(a) and not replaced[0]:
                changed=copy.deepcopy(a);changed['metadata'].update(uid='new-owner',resourceVersion='2');changed['spec']['subjects']=[{'kind':'Group','name':'new-team'}];c.seed(path,changed);replaced[0]=True
            return base(method,path,body)
        c.request=replace;Reconciler(c,['team']).cycle();actual=c.get(cr_path(a));self.assertNotIn('status',actual);self.assertNotIn('finalizers',actual['metadata']);self.assertFalse(any(o['kind']=='ResourceQuota' for o in c.objects.values()))
    def test_changed_generation_is_not_status_updated(self):
        c,p,a=fixture();r=Reconciler(c,['team']);c.objects[cr_path(a)]['metadata']['generation']=2
        with self.assertRaises(ApiError):r.save_status(a,True,'Reconciled','stale')
        self.assertNotIn('status',c.get(cr_path(a)))
    def test_accepted_child_delete_retains_parent_finalizer_and_remaining_guards(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();base=c.request
        def delayed(method,path,body=None):
            if method=='DELETE' and '/localqueues/' in path:
                c.objects[path]['metadata'].update(deletionTimestamp='2026-11-11T00:00:00Z',finalizers=['kueue.x-k8s.io/resource-in-use']);return copy.deepcopy(c.objects[path])
            return base(method,path,body)
        c.request=delayed;c.objects[cr_path(a)]['metadata']['deletionTimestamp']='2026-11-11T00:00:00Z';r.cycle();self.assertIn(FINALIZER,c.get(cr_path(a))['metadata']['finalizers']);self.assertTrue(any(o['kind']=='ResourceQuota' for o in c.objects.values()));self.assertTrue(any(o['kind']=='ValidatingAdmissionPolicyBinding' for o in c.objects.values()))

    def test_legacy_binding_pending_deletion_is_not_reported_revoked(self):
        c,p,a=fixture();r=Reconciler(c,['team']);path='/apis/rbac.authorization.k8s.io/v1/namespaces/team/rolebindings/'+policy_name(a);c.seed(path,{'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':{'name':policy_name(a),'namespace':'team','uid':'legacy','resourceVersion':'1','labels':{OWNER_LABEL:a['metadata']['uid'],'app.kubernetes.io/managed-by':'davano-controller'}},'subjects':a['spec']['subjects'],'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':'davano-workload-submitter'}});base=c.request
        def delayed(method,target,body=None):
            if method=='DELETE' and target==path:
                c.objects[path]['metadata'].update(deletionTimestamp='2026-11-11T00:00:00Z',finalizers=['test/hold']);return copy.deepcopy(c.objects[path])
            return base(method,target,body)
        c.request=delayed;r.cycle();status=c.get(cr_path(a))['status'];self.assertEqual(status['workloadGrants'],'revocation-pending');self.assertEqual(status['conditions'][0]['reason'],'LegacyGrantRevocationPending');self.assertTrue(any(x['type']=='WorkloadAccess' and x['status']=='Unknown' for x in status['conditions']));self.assertIn(path,c.objects)
    def test_invalid_mode_holds_existing_queue_and_valid_mode_releases(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();path=KUEUE+'/clusterqueues/'+pool_name(p);c.objects['/api/v1/nodes/worker']['metadata']['labels']['nvidia.com/gpu.sharing-strategy']='time-slicing';r.cycle();self.assertEqual(c.get(path)['spec']['stopPolicy'],'Hold');self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['status'],'False');c.objects['/api/v1/nodes/worker']['metadata']['labels']['nvidia.com/gpu.sharing-strategy']='none';r.cycle();self.assertEqual(c.get(path)['spec']['stopPolicy'],'None')
    def test_unavailable_nodes_requests_hold_on_existing_owned_queue(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();c.fail='/api/v1/nodes'
        with self.assertRaises(ApiError):r.cycle()
        self.assertEqual(c.get(KUEUE+'/clusterqueues/'+pool_name(p))['spec']['stopPolicy'],'Hold');self.assertEqual(c.get(cr_path(p))['status']['conditions'][0]['reason'],'InventoryUnavailable')
    def test_hold_never_changes_foreign_queue(self):
        c,p,a=fixture();r=Reconciler(c,['team']);r.cycle();path=KUEUE+'/clusterqueues/'+pool_name(p);c.objects[path]['metadata']['labels'][OWNER_LABEL]='foreign';c.objects['/api/v1/nodes/worker']['metadata']['labels']['nvidia.com/gpu.sharing-strategy']='time-slicing';r.cycle();self.assertEqual(c.get(path)['spec']['stopPolicy'],'None')
    def test_empty_namespace_selector_is_logically_impossible(self):
        selector=pool_resources(pool(),[])[-1][1]['spec']['namespaceSelector']['matchExpressions'];self.assertEqual(len(selector),2);self.assertEqual(selector[0]['key'],selector[1]['key']);self.assertEqual(selector[0]['values'],selector[1]['values']);self.assertEqual([s['operator'] for s in selector],['In','NotIn'])

class InputTests(unittest.TestCase):
    def test_protected_label_required(self):
        body={'name':'a','spec':pool()['spec']};body['spec']['nodeSelector']={'gpu':'yes'}
        with self.assertRaises(ValidationError): ManagedPoolCreate.model_validate(body)
    def test_managed_selector_maximum_matches_resourceflavor(self):
        body={'name':'a','spec':pool()['spec']};body['spec']['nodeSelector'].update({f'label-{i}':'yes' for i in range(7)});ManagedPoolCreate.model_validate(body);body['spec']['nodeSelector']['ninth']='no'
        with self.assertRaises(ValidationError):ManagedPoolCreate.model_validate(body)
    def test_unsafe_subject_and_namespace_rejected(self):
        for ns,subject in [('default',{'kind':'Group','name':'research'}),('team',{'kind':'Group','name':'system:authenticated'}),('team',{'kind':'ServiceAccount','name':'submitter','namespace':'other'})]:
            a=policy();a['spec']['subjects']=[subject]
            with self.assertRaises(ValidationError): ManagedPolicyCreate.model_validate({'name':'a','namespace':ns,'spec':a['spec']})
    def test_strict_counts_and_extra_fields(self):
        for value in [True,1.5,'4',-1]:
            p=pool();p['spec']['nominalQuota']=value
            with self.assertRaises(ValidationError): ManagedPoolCreate.model_validate({'name':'a','spec':p['spec']})

class ControlApiTests(unittest.TestCase):
    def app(self,enabled=False,source='kubernetes'):
        c=FakeKube();app=FastAPI()
        with patch.dict(os.environ,{'DAVANO_CONTROL_ENABLED':str(enabled).lower(),'DAVANO_API_TOKEN':'x'*32}): install_control_api(app,source,c)
        return TestClient(app),c
    def test_disabled_default_no_client_access(self):
        client,c=self.app();self.assertFalse(client.get('/api/v1/control/status').json()['enabled']);self.assertEqual(client.get('/api/v1/control/pools').status_code,409);self.assertEqual(c.writes,[])
    def test_enabled_requires_strong_token_and_live_source(self):
        for source,token in [('demo','x'*32),('kubernetes','short')]:
            with patch.dict(os.environ,{'DAVANO_CONTROL_ENABLED':'true','DAVANO_API_TOKEN':token}):
                with self.assertRaises(ValueError): install_control_api(FastAPI(),source,FakeKube())
    def test_reads_and_writes_require_exact_bearer(self):
        client,c=self.app(True)
        for header in ['', 'Bearer wrong', 'Basic '+ 'x'*32]:
            self.assertEqual(client.get('/api/v1/control/pools',headers={'Authorization':header}).status_code,401)
        self.assertEqual(client.get('/api/v1/control/pools',headers={'Authorization':'Bearer '+'x'*32}).status_code,200)
    def test_create_is_crd_not_local_metadata(self):
        client,c=self.app(True);result=client.post('/api/v1/control/pools',json={'name':'a','spec':pool()['spec']},headers={'Authorization':'Bearer '+'x'*32});self.assertEqual(result.status_code,201);self.assertEqual(result.json()['kind'],'GPUPool');self.assertNotIn('status',result.json());self.assertEqual(c.writes[0][1],ROOT+'/gpupools')
    def test_delete_requires_current_uid_and_resourceversion(self):
        client,c=self.app(True);p=pool();c.seed(cr_path(p),p);headers={'Authorization':'Bearer '+'x'*32}
        self.assertEqual(client.request('DELETE','/api/v1/control/pools/a',headers=headers,json={'uid':'wrong','resourceVersion':'1'}).status_code,409)
        self.assertIn(cr_path(p),c.objects)
        self.assertEqual(client.request('DELETE','/api/v1/control/pools/a',headers=headers,json={'uid':p['metadata']['uid'],'resourceVersion':'1'}).status_code,200)

class LeaderTests(unittest.TestCase):
    def test_lease_create_renew_and_contender(self):
        c=FakeKube();a=Lease(c,'davano');b=Lease(c,'davano');self.assertTrue(a.renew());self.assertFalse(b.renew());self.assertTrue(a.renew())
    def test_contender_waits_local_duration_despite_stale_wall_time(self):
        c=FakeKube();clock=[0];a=Lease(c,'davano',clock=lambda:clock[0]);b=Lease(c,'davano',clock=lambda:clock[0]);a.renew();c.objects[a.path]['spec']['renewTime']='1970-01-01T00:00:00Z';self.assertFalse(b.renew());clock[0]=29;self.assertFalse(b.renew());clock[0]=31;self.assertTrue(b.renew());self.assertFalse(a.renew())
    def test_expired_leader_stops_mutation(self):
        c=FakeKube();clock=[0];lease=Lease(c,'davano',clock=lambda:clock[0]);self.assertTrue(lease.renew());client=LeaderClient(c,lease);clock[0]=100
        with self.assertRaises(ApiError): client.request('POST','/api/v1/namespaces',{})
    def test_failed_renewal_immediately_fences(self):
        c=FakeKube();lease=Lease(c,'davano');lease.renew();c.fail='GET';self.assertFalse(lease.renew());self.assertFalse(lease.held)

if __name__=='__main__': unittest.main()
