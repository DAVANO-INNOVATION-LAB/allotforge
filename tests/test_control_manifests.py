"""Static operator security contracts. Requires real cluster acceptance separately."""
import unittest
from pathlib import Path
import yaml
import jsonschema

DEPLOY=Path(__file__).resolve().parents[1]/'deploy'
def read(path): return yaml.safe_load((DEPLOY/path).read_text())
class ControlManifestTests(unittest.TestCase):
    def test_managed_crds_are_separate_from_legacy_drafts(self):
        for name,scope in [('gpupools','Cluster'),('gpuaccesspolicies','Namespaced')]:
            obj=read('operator/'+name+'.yaml')
            self.assertEqual(obj['spec']['group'],'fleet.davano.io')
            self.assertEqual(obj['spec']['scope'],scope)
            version=obj['spec']['versions'][0]
            self.assertEqual(version['name'],'v1alpha1')
            self.assertEqual(version['subresources'],{'status':{}})
    def test_examples_validate_json_schema(self):
        names={'GPUPool':'gpupools','GPUAccessPolicy':'gpuaccesspolicies'}
        for body in yaml.safe_load_all((DEPLOY/'examples/managed-resources.yaml').read_text()):
            schema=read('operator/'+names[body['kind']]+'.yaml')['spec']['versions'][0]['schema']['openAPIV3Schema']
            jsonschema.validate(body,schema)
    def test_immutable_placement_and_pool_reference(self):
        for name,fields in [('gpupools',['nodeSelector','resourceName']),('gpuaccesspolicies',['poolRef'])]:
            schema=read('operator/'+name+'.yaml')['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']
            for field in fields:
                self.assertIn('self == oldSelf',[r['rule'] for r in schema[field]['x-kubernetes-validations']])
    def test_controller_has_no_node_writes_secrets_or_escalation(self):
        rules=read('operator/clusterrole.yaml')['rules']
        for rule in rules:
            self.assertNotIn('*',rule['resources']);self.assertNotIn('*',rule['verbs'])
            self.assertNotIn('secrets',rule['resources']);self.assertNotIn('escalate',rule['verbs']);self.assertNotIn('impersonate',rule['verbs'])
            if 'nodes' in rule['resources']:
                self.assertEqual(set(rule['verbs']),{'get','list'})
            self.assertNotIn('bind',rule['verbs'])
    def test_namespaced_mutations_require_separate_tenant_role(self):
        for rule in read('operator/clusterrole.yaml')['rules']:
            self.assertFalse(set(rule['resources'])&{'resourcequotas','rolebindings','localqueues','gpuaccesspolicies/status'})
        docs=list(yaml.safe_load_all((DEPLOY/'examples/operator-tenant-rbac.yaml').read_text()))
        self.assertEqual([d['kind'] for d in docs],['Role','RoleBinding'])
        self.assertTrue(all(d['metadata']['namespace']=='tenant-example' for d in docs))
    def test_controller_unprivileged_openshift_uid_compatible(self):
        pod=read('operator/deployment.yaml')['spec']['template']['spec'];c=pod['containers'][0]
        self.assertTrue(pod['securityContext']['runAsNonRoot']);self.assertNotIn('runAsUser',pod['securityContext']);self.assertNotIn('fsGroup',pod['securityContext']);self.assertNotIn('runAsUser',c['securityContext']);self.assertEqual(c['securityContext']['capabilities']['drop'],['ALL']);self.assertFalse(c['securityContext']['allowPrivilegeEscalation']);self.assertTrue(c['securityContext']['readOnlyRootFilesystem']);self.assertTrue(all('hostPath' not in v for v in pod['volumes']))
    def test_workload_grants_are_disabled_in_manifests(self):
        self.assertEqual(read('operator/submitter-clusterrole.yaml')['rules'],[])
        role=list(yaml.safe_load_all((DEPLOY/'examples/operator-tenant-rbac.yaml').read_text()))[0]
        rule=next(r for r in role['rules'] if r['resources']==['rolebindings']);self.assertEqual(set(rule['verbs']),{'get','delete'})
    def test_api_identity_cannot_change_native_enforcement(self):
        rules=read('control-api/clusterrole.yaml')['rules']
        for rule in rules:
            self.assertFalse(set(rule['resources'])&{'clusterqueues','resourceflavors','validatingadmissionpolicies','resourcequotas','rolebindings'})
        patch=read('control-api/deployment-patch.yaml')['spec']['template']['spec']
        env={v['name']:v for v in patch['containers'][0]['env']}
        self.assertEqual(patch['serviceAccountName'],'davano-control-api')
        self.assertEqual(env['DAVANO_CONTROL_ENABLED']['value'],'true')
        self.assertIn('secretKeyRef',env['DAVANO_API_TOKEN']['valueFrom'])
    def test_operator_installs_no_route_ingress_or_scc(self):
        for filename in read('operator/kustomization.yaml')['resources']:
            self.assertNotIn(read('operator/'+filename)['kind'],{'Ingress','Route','SecurityContextConstraints','Secret'})
        self.assertEqual(read('operator/networkpolicy.yaml')['spec']['ingress'],[])
    def test_crd_cel_selector_and_subject_guards_present(self):
        fields=read('operator/gpupools.yaml')['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']
        self.assertIn('node-restriction.kubernetes.io',fields['nodeSelector']['x-kubernetes-validations'][0]['rule'])
        subject=read('operator/gpuaccesspolicies.yaml')['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']['subjects']['items']
        self.assertIn('system:authenticated',subject['x-kubernetes-validations'][0]['rule'])
    def test_subject_namespace_guard_uses_crd_cel_reserved_name_escape(self):
        subject=read('operator/gpuaccesspolicies.yaml')['spec']['versions'][0]['schema']['openAPIV3Schema']['properties']['spec']['properties']['subjects']['items']
        # Kubernetes maps reserved schema properties to __{keyword}__ in CEL.
        # Keep the public JSON property unchanged and enforce the same boundary.
        self.assertIn('namespace',subject['properties'])
        self.assertNotIn('__namespace__',subject['properties'])
        rules=[r for r in subject['x-kubernetes-validations'] if r['message']=='Only ServiceAccounts have namespaces']
        self.assertEqual(len(rules),1)
        self.assertEqual(rules[0]['rule'],"self.kind == 'ServiceAccount' || !has(self.__namespace__)")
if __name__=='__main__': unittest.main()
