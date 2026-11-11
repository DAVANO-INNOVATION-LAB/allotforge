#!/usr/bin/env python3
"""Real Kubernetes admission smoke test, restricted to disposable kind-davano-ci.

Creates/deletes only fixed test namespace and two test-named admission policies.
Does not test Kueue controllers, GPU devices, RBAC or workload execution.
Run with DAVANO_TEST_CONTEXT=kind-davano-ci. Never run against production.
"""
import json
import os
import subprocess
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from davano.reconciler import native_policy, job_policy, policy_name

CONTEXT=os.environ.get('DAVANO_TEST_CONTEXT')
if CONTEXT!='kind-davano-ci':
    raise SystemExit('Refusing mutations: DAVANO_TEST_CONTEXT must be kind-davano-ci')
NS='davano-admission-test'

def kubectl(*args, body=None, check=True):
    run=subprocess.run(['kubectl','--context',CONTEXT,*args],input=json.dumps(body) if body else None,text=True,capture_output=True)
    if check and run.returncode:
        raise RuntimeError(run.stderr)
    return run

pool={'metadata':{'name':'a','uid':'admission-test-pool'},'spec':{'nodeSelector':{'davano.io.node-restriction.kubernetes.io/pool':'test'},'resourceName':'nvidia.com/gpu','nominalQuota':8,'mode':'whole-gpu'}}
policy={'metadata':{'name':'a','namespace':NS,'uid':'admission-test-policy'},'spec':{'poolRef':'a','gpuQuota':4,'maxGPUsPerPod':2,'subjects':[{'kind':'Group','name':'test'}]}}
resources=[*native_policy(policy,pool),*job_policy(policy,pool)]

def pod(name,limits=None,selector=True,**spec):
    c={'name':'worker','image':'registry.k8s.io/pause:3.10'}
    if limits is not None:c['resources']={'limits':limits}
    return {'apiVersion':'v1','kind':'Pod','metadata':{'name':name,'namespace':NS},'spec':{'containers':[c],**({'nodeSelector':pool['spec']['nodeSelector']} if selector else {}),**spec}}

def allowed(body):
    kubectl('create','--dry-run=server','-f','-',body=body)

def denied(body,message):
    result=kubectl('create','--dry-run=server','-f','-',body=body,check=False)
    if result.returncode==0 or message not in result.stderr:
        raise AssertionError(f'Expected admission denial {message!r}: {result.stderr}')

try:
    kubectl('create','namespace',NS)
    # CRD structural/CEL validation against a real API server.
    kubectl('apply','-f',str(Path(__file__).resolve().parents[1]/'deploy/operator/gpupools.yaml'))
    kubectl('apply','-f',str(Path(__file__).resolve().parents[1]/'deploy/operator/gpuaccesspolicies.yaml'))
    for resource in resources:
        kubectl('apply','-f','-',body=resource)
    for policy_body in [r for r in resources if r['kind']=='ValidatingAdmissionPolicy']:
        deadline=time.monotonic()+60
        while True:
            actual=json.loads(kubectl('get','validatingadmissionpolicy',policy_body['metadata']['name'],'-o','json').stdout)
            status=actual.get('status',{})
            if status.get('observedGeneration')==actual['metadata']['generation']:
                warnings=status.get('typeChecking',{}).get('expressionWarnings',[])
                if warnings:raise AssertionError(warnings)
                break
            if time.monotonic()>deadline:raise TimeoutError('Native admission type checking did not complete')
            time.sleep(1)
    # Admission caches propagate asynchronously. Wait for the negative control,
    # then verify both allow/deny cases; no live workloads are created.
    deadline=time.monotonic()+30
    while True:
        result=kubectl('create','--dry-run=server','-f','-',body=pod('missing-selector',{'nvidia.com/gpu':'1'},False),check=False)
        if result.returncode and 'assigned protected pool node selector' in result.stderr:break
        if time.monotonic()>deadline:raise TimeoutError('Admission binding did not become active')
        time.sleep(1)
    allowed(pod('valid',{'nvidia.com/gpu':'2'}))
    allowed(pod('cpu-only',None,False))
    denied(pod('oversized',{'nvidia.com/gpu':'3'}),'maxGPUsPerPod')
    denied(pod('wrong-vendor',{'amd.com/gpu':'1'}),'GPU resource must match')
    denied(pod('missing-selector',{'nvidia.com/gpu':'1'},False),'assigned protected pool node selector')
    denied(pod('direct-node',{'nvidia.com/gpu':'1'},True,nodeName='kind-control-plane'),'assigned protected pool node selector')
    denied(pod('init-overflow',{'nvidia.com/gpu':'1'},True,initContainers=[{'name':'init','image':'registry.k8s.io/pause:3.10','resources':{'limits':{'nvidia.com/gpu':'2'}}}]),'maxGPUsPerPod')
    template=pod('template',{'nvidia.com/gpu':'1'})
    job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'queued','namespace':NS,'labels':{'kueue.x-k8s.io/queue-name':policy_name(policy)}},'spec':{'suspend':True,'template':{'spec':{**template['spec'],'restartPolicy':'Never'}}}}
    allowed(job)
    job['metadata']['labels']={}
    denied(job,'assigned Kueue LocalQueue')
    job['metadata']['labels']={'kueue.x-k8s.io/queue-name':policy_name(policy)}
    job['spec']['suspend']=False
    denied(job,'created suspended')
    print('Real API-server admission smoke passed: CRDs, type checking, valid Pods/Jobs and rejected bypass cases.')
finally:
    for resource in reversed(resources):
        kubectl('delete',resource['kind'],resource['metadata']['name'],'--ignore-not-found',check=False)
    kubectl('delete','namespace',NS,'--ignore-not-found','--wait=false',check=False)
