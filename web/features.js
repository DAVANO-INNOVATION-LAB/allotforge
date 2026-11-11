/* Cluster control and Ray planning. Tokens are kept in memory only. */
'use strict';
let controlGeneration=0, pendingControlDeletion=null;
function control(){
 const enabled=controlState?.enabled===true;
 const active=enabled&&Boolean(apiToken);
 const caps=controlState?.capabilities||[];
 return heading('Cluster control','Inspect desired state, reconciliation, and the exact boundary of enforcement.',button('Refresh status','retry','history')+(apiToken?button('Disconnect session','forget-api','x'):button('Authorize session','connect-api','shield')))+
 `<section class="control-hero"><div><div class="eyebrow">EXPLICIT AUTHORITY · OBSERVED STATUS</div><h2>${enabled?'Control API is enabled.':'Cluster writes are disabled.'}</h2><p>${enabled?'An API write records desired state. Pool readiness, native guardrail reconciliation, and tenant workload access are separate conditions.':'Inventory remains available without write access. Enable the server-side control API and configure its credential before managing cluster resources.'}</p></div><div class="control-mode">${badge(controlState?.mode||'Unavailable',enabled?'amber':'neutral')}<span>${apiToken?'Credential in memory':'No authorized session'}</span></div></section>`+
 `<div class="notice access-boundary" role="note">${icon('shield',19)}<div><strong>No new tenant workload access is granted.</strong> Subjects are declarations only. Native guardrails may reconcile while policy readiness stays blocked. Legacy grants may remain until revocation is verified; external RBAC is unchanged.</div></div>`+
 (controlError?`<div class="notice error" role="alert">${icon('info')}<div><strong>Control status could not be read.</strong><p>${esc(controlError)}</p><p>No enabled state or enforcement is assumed.</p></div></div>`:'')+
 `<div class="readiness-grid"><article class="card readiness-card">${icon('server',20)}<span>API authority</span><strong>${enabled?'Enabled by administrator':'Disabled / unavailable'}</strong><small>Configuration, not cluster health</small></article><article class="card readiness-card">${icon('shield',20)}<span>Session</span><strong>${apiToken?'Credential supplied':'Not authenticated'}</strong><small>Never saved in browser storage</small></article><article class="card readiness-card">${icon('history',20)}<span>Reconciliation</span><strong>${active?'Inspect each resource':'Not observed'}</strong><small>Inspect each current-generation condition</small></article></div>`+
 `<div class="section-title"><h2>Controller capabilities</h2>${badge(controlState?.source||'Unknown source','neutral')}</div><div class="capability-grid">${caps.length?caps.map(c=>`<article class="card capability-card"><div>${icon(c.id==='kueue'?'layers':'shield',18)}${badge(c.level||'Unknown','neutral')}</div><h3>${esc(({'declarative-pools':'Declarative pools','kueue':'Kueue queues','namespace-guardrails':'Namespace guardrails'})[c.id]||c.id||'Capability')}</h3><p>${esc(c.detail||'No detail reported.')}</p></article>`).join(''):empty('No capabilities reported','The configured server has not provided control capability data.')}</div>`+
 `<section class="card managed-section"><div class="card-header"><div><h2>Managed GPU pools</h2><p class="caption">Cluster-scoped GPUPool resources, separate from local advisory views</p></div>${active&&!controlListError&&!controlLoading?button('Create cluster pool','create-control-pool','plus'):badge('Session required','neutral')}</div>${controlResourceList('pools',active)}</section>`+
 `<section class="card managed-section"><div class="card-header"><div><h2>Namespace quota policies</h2><p class="caption">Namespace guardrails only. Declared subjects receive no new permissions.</p></div>${active&&!controlListError&&!controlLoading?button('Create cluster policy','create-control-policy','plus'):badge('Session required','neutral')}</div>${controlResourceList('policies',active)}</section>`+
 `<section class="card detail-panel boundaries"><h2>Keep the guarantees separate</h2><div class="definition"><span>Declared subjects</span><strong>Not access grants</strong></div><div class="definition"><span>Legacy permissions</span><strong>Revocation must be observed</strong></div><div class="definition"><span>Namespace quota</span><strong>Aggregate requested GPU units, not utilization</strong></div><div class="definition"><span>Per-Pod admission</span><strong>Not a multi-Pod job-wide cap</strong></div><div class="definition"><span>Hardware isolation</span><strong>Not proven by an accepted policy</strong></div><div class="definition"><span>Deployment verification</span><strong>Requires target-cluster checks</strong></div>${(controlState?.limitations||[]).map(t=>`<p>${esc(t)}</p>`).join('')}</section>`;
}
function observedCondition(item,type){
 const condition=(item.status?.conditions||[]).find(c=>c.type===type);
 const generation=item.metadata?.generation;
 const observed=condition?.observedGeneration??(type==='Ready'?item.status?.observedGeneration:undefined);
 return {condition,current:Boolean(condition)&&Number.isInteger(generation)&&observed===generation};
}
function conditionState(item,kind){
 if(item.metadata?.deletionTimestamp)return {label:'Draining / deleting',tone:'amber'};
 const {condition:ready,current}=observedCondition(item,'Ready');
 if(!ready)return {label:'Pending reconciliation',tone:'amber'};
 if(!current)return {label:'Status is stale',tone:'amber'};
 if(kind==='policies'||item.kind==='GPUAccessPolicy'){
  if(ready.reason==='LegacyGrantRevocationPending')return {label:'Not ready: revocation pending',tone:'red'};
  if(ready.reason==='WorkloadGrantsDisabled')return {label:'Not ready: workload grants disabled',tone:'amber'};
  if(ready.status==='True')return {label:'Unexpected policy Ready status',tone:'red'};
 }
 if(ready.status==='True')return {label:'Reconciled',tone:''};
 return {label:ready.reason||'Not ready',tone:'red'};
}
function guardrailState(item){
 if(item.metadata?.deletionTimestamp)return {label:'Draining / deleting',tone:'amber'};
 const {condition,current}=observedCondition(item,'GuardrailsReady');
 if(!condition)return {label:'Guardrails not observed',tone:'amber'};
 if(!current)return {label:'Guardrail status stale',tone:'amber'};
 if(condition.status==='True')return {label:'Guardrails reconciled',tone:'blue'};
 return {label:condition.status==='False'?'Guardrails not ready':'Guardrails unknown',tone:'amber'};
}
function workloadAccessState(item){
 const {condition,current}=observedCondition(item,'WorkloadAccess');
 if(!current)return {label:'Access unverified',tone:'amber',detail:'Do not assume prior permissions are revoked.'};
 if(condition.reason==='LegacyGrantRevocationPending'||item.status?.workloadGrants==='revocation-pending')return {label:'Revocation pending',tone:'red',detail:'Legacy permissions may still exist.'};
 if(condition.status==='False')return {label:'No policy access granted',tone:'amber',detail:'External RBAC is unchanged.'};
 return {label:condition.status==='True'?'Unexpected access state':'Access unverified',tone:'red',detail:'Do not treat guardrails as a tenant execution boundary.'};
}
function controlResourceList(kind,active){
 if(!active)return `<div class="control-empty">${icon('shield',23)}<p>${controlState?.enabled?'Authorize a session to read managed resources.':'Enable cluster control before reading managed resources.'}</p><small>No fixture resources appear in this view.</small></div>`;
 if(controlLoading)return '<div class="control-empty" role="status">Reading managed cluster resources…</div>';
 if(controlListError)return `<div class="notice error" role="alert">${esc(controlListError)}</div>`;
 const items=controlItems[kind]||[],policy=kind==='policies';
 return `<div class="table-wrap"><table class="data-table"><caption class="sr-only">${policy?'Managed namespace policies':'Managed GPU pools'}</caption><thead><tr><th>RESOURCE</th><th>${policy?'POOL / NAMESPACE QUOTA':'RESOURCE / DESIRED QUOTA'}</th><th>${policy?'NATIVE GUARDRAILS':'OBSERVED STATE'}</th>${policy?'<th>WORKLOAD ACCESS</th>':''}<th></th></tr></thead><tbody>${items.map((item,index)=>{const c=policy?guardrailState(item):conditionState(item,kind),access=workloadAccessState(item);return `<tr><td><strong>${esc(item.metadata?.name)}</strong><small>${esc(item.metadata?.namespace||'Cluster scoped')} · generation ${esc(item.metadata?.generation??'unknown')}</small>${policy?`<small>${esc(conditionState(item,kind).label)}</small>`:''}</td><td>${esc(policy?item.spec?.poolRef:item.spec?.resourceName)}<small>${count(policy?item.spec?.gpuQuota:item.spec?.nominalQuota)} requested GPU units</small></td><td>${badge(c.label,c.tone)}</td>${policy?`<td>${badge(access.label,access.tone)}<small>${esc(access.detail)}</small></td>`:''}<td><button class="button text" data-control-resource="${index}" data-kind="${kind}" aria-label="Inspect ${esc(item.metadata?.name)}">Inspect ${icon('arrow',13)}</button></td></tr>`;}).join('')||`<tr><td colspan="${policy?5:4}">${empty('No managed resources','Create a desired resource and inspect its reconciliation status here.')}</td></tr>`}</tbody></table></div>`;
}
async function loadControlResources(){
 if(!controlState?.enabled||!apiToken)return;
 const generation=++controlGeneration;controlLoading=true;controlListError=null;if(['control','assignments'].includes(page))render();
 try{const [pools,policies]=await Promise.all([api('/control/pools'),api('/control/policies')]);if(generation!==controlGeneration)return;controlItems={pools:pools.items||[],policies:policies.items||[]};}
 catch(error){if(generation!==controlGeneration)return;controlItems={pools:[],policies:[]};controlListError=error.message;}
 finally{if(generation===controlGeneration){controlLoading=false;if(['control','assignments'].includes(page))render();}}
}
function openControlDetail(index,kind){
 const item=controlItems[kind]?.[Number(index)];if(!item)return;
 const c=conditionState(item,kind),guard=guardrailState(item),access=workloadAccessState(item);
 showDialog(item.metadata?.name||'Managed resource','Desired state and observed status returned by the cluster API.',`<div class="notice">${icon('info')}<div><strong>${esc(c.label)}.</strong> Reconciliation is not a hardware-isolation certification.</div></div><div class="definition"><span>UID</span><strong class="mono">${esc(item.metadata?.uid||'Unknown')}</strong></div><div class="definition"><span>Generation</span><strong>${esc(item.metadata?.generation??'Unknown')}</strong></div>${kind==='policies'?`<div class="policy-access-summary"><div class="definition"><span>Native guardrails</span><strong>${badge(guard.label,guard.tone)}</strong></div><div class="definition"><span>Workload access</span><strong>${badge(access.label,access.tone)}</strong></div><p>${esc(access.detail)} No new workload permissions are granted to declared subjects.</p><h3 class="form-section">Declared subjects, not granted</h3><p>These declarations do not grant permissions.</p>${(item.spec?.subjects||[]).map(subject=>`<div class="definition"><span>${esc(subject.kind)}</span><strong>${esc(subject.namespace?subject.namespace+'/':'')}${esc(subject.name)}</strong></div>`).join('')}</div>`:''}<h3 class="form-section">Observed conditions</h3>${(item.status?.conditions||[]).map(condition=>`<div class="condition"><strong>${esc(condition.type)}: ${esc(condition.status)}</strong><p>${esc(condition.reason||'')} ${esc(condition.message||'')}</p><small>Observed generation ${esc(condition.observedGeneration??item.status?.observedGeneration??'unknown')}</small></div>`).join('')||'<p class="caption">No status conditions have been observed.</p>'}<details class="manifest-details"><summary>Inspect full Kubernetes object</summary><pre class="code-block">${esc(JSON.stringify(item,null,2))}</pre></details>`,`<button class="button" data-action="close-dialog">Close</button><button class="button danger" data-delete-control="${index}" data-kind="${kind}" ${item.metadata?.uid&&item.metadata?.resourceVersion?'':'disabled'}>Delete desired resource</button>`);
}
function safeCredentialOrigin(){return location.protocol==='https:'||['localhost','127.0.0.1','[::1]'].includes(location.hostname);}
function openCredentials(){
 const safe=safeCredentialOrigin();
 showDialog('Authorize this session','Supply the administrator API credential configured for this server.',`<form id="credentials-form"><div class="notice">${icon('shield')}<div>This credential stays in memory and is sent only to this same-origin API. Reloading or disconnecting clears it. Do not enter a Kubernetes service-account token.</div></div>${safe?'':'<div class="notice error">Use HTTPS or a loopback origin before entering a credential.</div>'}<div class="field"><label for="field-token">Administrator API credential</label><input id="field-token" name="token" type="password" autocomplete="off" autocapitalize="off" spellcheck="false" required minlength="32" maxlength="4096" ${safe?'':'disabled'}></div><div id="form-error" class="validation-message" role="alert"></div></form>`,`<button class="button" data-action="close-dialog">Cancel</button><button class="button primary" type="submit" form="credentials-form" ${safe?'':'disabled'}>Use for this session</button>`);
}
function replaceAdministratorCredential(token){
 apiToken=token;invalidateInventorySession();allocationTarget=null;allocationPlan=null;manifestPlan=null;pendingControlDeletion=null;
 controlGeneration++;controlState=null;controlError=null;rayCapabilities=null;controlItems={pools:[],policies:[]};controlListError=null;controlLoading=false;
 diagnosticsCredential=token;resetDiagnostics('Administrator credential changed. Run a new check for this session.');
 const modal=document.getElementById('modal');if(!modal.querySelector('#credentials-form')){if(modal.open)closeDialog();modal.innerHTML='';}
 return inventoryGeneration;
}
function forgetCredentials(){replaceAdministratorCredential('');render();toast('Session credential cleared from memory.');}
function openControlForm(kind){
 if(!controlState?.enabled||!apiToken){openCredentials();return;}
 const pool=kind==='pools';
 showDialog(pool?'Create a managed GPU pool':'Create a namespace quota policy',pool?'Writes a cluster-scoped GPUPool desired resource.':'Writes a GPUAccessPolicy for native guardrails. Subjects are declarations, not access grants.',`<form id="control-${kind}-form"><div class="notice">${icon('info')}<div><strong>Cluster change.</strong> ${pool?'Nominal quota is desired capacity, not a hardware reservation.':'Namespace quota affects matching GPU requests. Per-Pod limits are not job-wide limits. No workload permissions are granted to declared subjects.'} Review the target and limits before creating.</div></div><div class="form-grid">${field('Resource name','name','text','','Kubernetes DNS label.','required pattern="[a-z0-9]([a-z0-9-]*[a-z0-9])?" maxlength="63"')}${pool?field('GPU resource name','resource_name','text','nvidia.com/gpu','Must match the installed device plugin.','required'):field('Namespace','namespace','text','','The namespace must already exist.','required pattern="[a-z0-9]([a-z0-9-]*[a-z0-9])?" maxlength="63"')}${pool?field('Nominal GPU quota','nominal_quota','number','8','Requested resource units. Zero pauses new quota.','required min="0" max="100000" step="1"'):field('Managed pool name','pool_ref','text','','An existing GPUPool resource.','required maxlength="63"')}${pool?selectField('Allocation mode','mode',[['whole-gpu','Whole GPU']]):field('Namespace GPU quota','gpu_quota','number','4','Aggregate GPU requests. Zero denies new GPU requests.','required min="0" max="100000" step="1"')}${pool?'<div class="field full"><label for="field-selector">Node selector JSON</label><textarea id="field-selector" name="selector" rows="3" required spellcheck="false">{"davano.io.node-restriction.kubernetes.io/pool":"training"}</textarea><small>Use 1 to 8 equality labels, including protected membership. Verify actual plugin mode.</small></div>':field('Maximum GPUs per Pod','max_per_pod','number','2','Not a multi-Pod workload cap.','required min="1" max="1024" step="1"')}${pool?'':selectField('Declared subject type','subject_kind',[['Group','Kubernetes group'],['User','Kubernetes user'],['ServiceAccount','Namespace service account']])}${pool?'':'<div class="field full"><label for="field-groups">Declared subjects (not granted)</label><textarea id="field-groups" name="groups" rows="2" required placeholder="One exact identity name per line"></textarea><small>Declaration only: these identities receive no new permissions. ServiceAccounts use the policy namespace.</small></div>'}</div><label class="checkbox-field"><input type="checkbox" name="reviewed" required> I reviewed the cluster resource, scope, and desired limits.</label><div id="form-error" class="validation-message" role="alert"></div></form>`,`<button class="button" data-action="close-dialog">Cancel</button><button class="button primary" type="submit" form="control-${kind}-form">Create desired resource</button>`);
}
function rayCapabilityCard(){return `<section class="card detail-panel ray-capability"><div class="section-title"><h2>Ray manifest planning</h2>${badge('Export only','blue')}</div><p>Build a suspended RayJob for KubeRay ${esc(rayCapabilities.kuberay_version||'version not reported')}. The manifest is not submitted, and no GPU is reserved.</p><div class="definition"><span>Available API resource modes</span><strong>${(rayCapabilities.api_modes||['whole-gpu']).map(esc).join(', ')}</strong></div><div class="definition"><span>Job submission</span><strong>Disabled</strong></div>${(rayCapabilities.limitations||[]).map(t=>`<p>${esc(t)}</p>`).join('')}${button('Plan Ray workload','plan-ray','plus',true)}</section>`;}
function openRayForm(){
 const pool=state.pools[0],policy=state.policies.find(p=>p.pool_id===pool?.id);
 manifestPlan=null;
 showDialog('Plan a Ray workload','Validate against local pool policy, then export a suspended RayJob.',`<div class="workflow-steps"><span class="active">1 · Configure</span><span>2 · Review plan</span><span>3 · Export manifest</span></div><form id="ray-form"><details class="ray-configuration" open><summary>Job configuration</summary><div class="form-grid">${field('Job name','name','text','','','required pattern="[a-z0-9]([a-z0-9-]*[a-z0-9])?" maxlength="63"')}${field('Namespace','namespace','text',policy?.namespace||'','','required pattern="[a-z0-9]([a-z0-9-]*[a-z0-9])?" maxlength="63"')}${selectField('Advisory pool','pool_id',state.pools.map(p=>[p.id,p.name]))}${field('Ray version','ray_version','text','','Exact numeric release, such as 2.55.1.','required pattern="[0-9]+\\.[0-9]+\\.[0-9]+" maxlength="40"')}<div class="field full"><label for="field-image">Pinned container image</label><input id="field-image" name="image" required maxlength="512" placeholder="registry.example.com/team/trainer:version"><small>Must include Ray and your application. Latest and unversioned images are rejected.</small></div><div class="field full"><label for="field-entrypoint">Entrypoint</label><textarea id="field-entrypoint" name="entrypoint" rows="2" required maxlength="4096" placeholder="python /app/train.py"></textarea></div>${field('Worker replicas','workers','number','1','Fixed replicas, no autoscaling.','required min="1" max="1024" step="1"')}${field('GPUs per worker','gpus_per_worker','number','1','Whole-GPU resource units.','required min="1" max="128" step="1"')}${field('Worker CPUs','worker_cpu','number','2','','required min="1" max="1024" step="1"')}${field('Worker memory (GiB)','worker_memory_gib','number','8','','required min="2" max="16384" step="1"')}${field('Head CPUs','head_cpu','number','1','Head requests no GPU.','required min="1" max="128" step="1"')}${field('Head memory (GiB)','head_memory_gib','number','2','','required min="2" max="1024" step="1"')}${selectField('GPU resource name','resource_name',[['nvidia.com/gpu','NVIDIA · nvidia.com/gpu'],['amd.com/gpu','AMD · amd.com/gpu'],['gpu.intel.com/i915','Intel · gpu.intel.com/i915'],['gpu.intel.com/xe','Intel · gpu.intel.com/xe']])}${field('Deadline (seconds)','active_deadline_seconds','number','3600','','required min="60" max="604800" step="1"')}</div></details><div class="info-strip">${icon('info')} Checks local policy and known resource mode. Does not guarantee capacity, image availability, Kueue readiness, or scheduler admission.</div><div id="form-error" class="validation-message" role="alert"></div><div id="manifest-result" aria-live="polite"></div></form>`,`<button class="button" data-action="close-dialog">Cancel</button><button class="button" data-action="download-manifest" disabled>${icon('export',14)}Download JSON</button><button class="button primary" type="submit" form="ray-form" ${state.pools.length?'':'disabled'}>Generate plan</button>`);
}
function clearManifestPreview(event){if(!event.target.closest('#ray-form'))return;manifestPlan=null;const result=document.getElementById('manifest-result');if(result)result.innerHTML='';const download=document.querySelector('[data-action="download-manifest"]');if(download)download.disabled=true;}
function renderManifestPlan(plan){
 const result=document.getElementById('manifest-result');if(!result)return;
 if(plan.submitted!==false||plan.allocated!==false||!plan.manifest||plan.manifest.spec?.suspend!==true)throw new Error('Unexpected plan response. No manifest will be exported.');
 manifestPlan=plan;document.querySelector('.ray-configuration').open=false;
 document.querySelector('.workflow-steps').innerHTML='<span>1 · Configured</span><span class="active">2 · Review plan</span><span>3 · Export manifest</span>';
 result.innerHTML=`<section class="result"><h3>${icon('check',18)}Manifest prepared for review</h3><p><strong>Not submitted. Not allocated.</strong> ${count(plan.total_gpu_units)} requested GPU units across fixed workers.</p><div class="definition"><span>Resource mode</span><strong>${esc(plan.mode)} · ${esc(plan.resource_name)}</strong></div><div class="definition"><span>Matched local policies</span><strong>${(plan.policy_ids||[]).map(esc).join(', ')||'None reported'}</strong></div><div class="plan-caution"><strong>Review before applying</strong><p>The queue name is provisional. Bind an administrator-approved LocalQueue, verify KubeRay and Kueue readiness, and review every prerequisite below.</p></div><details class="manifest-details"><summary>${(plan.prerequisites||[]).length} preflight checks</summary><ul>${(plan.prerequisites||[]).map(t=>`<li>${esc(t)}</li>`).join('')}</ul></details><details class="manifest-details"><summary>${(plan.warnings||[]).length} planning limitations</summary><ul>${(plan.warnings||[]).map(t=>`<li>${esc(t)}</li>`).join('')}</ul></details><details class="manifest-details"><summary>Review suspended RayJob JSON</summary><pre class="code-block">${esc(JSON.stringify(plan.manifest,null,2))}</pre></details></section>`;
 document.querySelector('[data-action="download-manifest"]').disabled=false;
}
function downloadManifest(){if(!manifestPlan?.manifest)return;const blob=new Blob([JSON.stringify(manifestPlan.manifest,null,2)+'\n'],{type:'application/json'});const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download=`allotforge-rayjob-${String(manifestPlan.manifest.metadata?.name||'plan').replace(/[^a-z0-9-]/g,'')}.json`;a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);toast('Suspended RayJob exported. Nothing was submitted.');}
async function submitExtendedForm(event){
 const form=event.target;
 if(!['credentials-form','ray-form','control-pools-form','control-policies-form','control-delete-form'].includes(form.id))return;
 event.preventDefault();const submit=document.querySelector(`[form="${form.id}"]`);if(submit.disabled)return;
 const values=Object.fromEntries(new FormData(form)), generation=dialogGeneration;
 document.getElementById('form-error').textContent='';submit.disabled=true;
 try{
  if(form.id==='credentials-form'){
   if(!safeCredentialOrigin())throw new Error('A secure origin is required for credentials.');
   const token=String(values.token||'').trim();if(token.length<32||/[\r\n]/.test(token))throw new Error('Enter the configured control API credential.');
   form.reset();closeDialog();const session=replaceAdministratorCredential(token);render();await load();if(session!==inventoryGeneration)return;await loadControlResources();if(session!==inventoryGeneration)return;toast(controlListError?'Credential supplied, but cluster access failed. Inspect the error.':'Credential is in memory for this session.');
  }else if(form.id==='ray-form'){
   manifestPlan=null;document.getElementById('manifest-result').innerHTML='';document.querySelector('[data-action="download-manifest"]').disabled=true;
   const payload={...values,mode:'whole-gpu'};for(const key of ['workers','gpus_per_worker','worker_cpu','worker_memory_gib','head_cpu','head_memory_gib','active_deadline_seconds'])payload[key]=Number(payload[key]);
   const result=await api('/ray/plan',{method:'POST',body:JSON.stringify(payload)});
   if(generation!==dialogGeneration||JSON.stringify(values)!==JSON.stringify(Object.fromEntries(new FormData(form))))return;
   renderManifestPlan(result);
  }else if(form.id==='control-delete-form'){
   if(!controlState?.enabled||!apiToken)throw new Error('Cluster control is disabled or the session is not authorized.');
   const pending=pendingControlDeletion;const item=pending?.item;
   if(!item||pending.kind!==values.kind||values.confirmation!==item.metadata?.name)throw new Error('Enter the exact resource name to confirm deletion.');
   const meta=item.metadata;if(!meta.uid||!meta.resourceVersion)throw new Error('Refresh the resource before deleting it.');
   const path=values.kind==='pools'?`/control/pools/${encodeURIComponent(meta.name)}`:`/control/policies/${encodeURIComponent(meta.namespace)}/${encodeURIComponent(meta.name)}`;
   await api(path,{method:'DELETE',body:JSON.stringify({uid:meta.uid,resourceVersion:meta.resourceVersion})});
   if(generation===dialogGeneration)closeDialog();await loadControlResources();toast('Deletion requested. Inspect draining status until the resource disappears.');
  }else{
   if(!controlState?.enabled||!apiToken)throw new Error('Cluster control is disabled or the session is not authorized.');
   if(values.reviewed!=='on')throw new Error('Review the cluster change before creating the resource.');
   const pool=form.id==='control-pools-form';let payload;
   if(pool){const selector=JSON.parse(values.selector);if(!selector||Array.isArray(selector)||typeof selector!=='object'||Object.keys(selector).length<1||Object.keys(selector).length>8||Object.values(selector).some(v=>typeof v!=='string'))throw new Error('Node selector must contain 1 to 8 labels with string values.');payload={name:values.name,spec:{nodeSelector:selector,resourceName:values.resource_name,nominalQuota:Number(values.nominal_quota),mode:values.mode}};}
   else{const groups=values.groups.split('\n').map(x=>x.trim()).filter(Boolean);if(!groups.length)throw new Error('At least one exact declared subject is required.');payload={name:values.name,namespace:values.namespace,spec:{poolRef:values.pool_ref,gpuQuota:Number(values.gpu_quota),maxGPUsPerPod:Number(values.max_per_pod),subjects:[...new Set(groups)].map(name=>({kind:values.subject_kind,name,...(values.subject_kind==='ServiceAccount'?{namespace:values.namespace}:{})}))}};}
   await api(`/control/${pool?'pools':'policies'}`,{method:'POST',body:JSON.stringify(payload)});
   if(generation===dialogGeneration)closeDialog();await loadControlResources();toast('Desired resource created. Inspect its current reconciliation status.');
  }
 }catch(error){if(generation===dialogGeneration){document.getElementById('form-error').textContent=error instanceof SyntaxError?'Enter valid JSON for the node selector.':error.message;}else toast('Request failed: '+error.message);}
 finally{submit.disabled=false;}
}

function openControlDelete(index,kind){
 const item=controlItems[kind]?.[Number(index)];if(!item)return;
 const snapshot=JSON.parse(JSON.stringify(item));closeDialog();pendingControlDeletion={kind,item:snapshot};
 showDialog('Delete desired resource',`${item.metadata?.namespace||'Cluster scoped'} / ${item.metadata?.name}`,`<form id="control-delete-form"><div class="notice error"><div><strong>This changes the cluster.</strong> ${kind==='policies'?'Policy deletion removes GPU guardrails from this namespace after workloads are drained. Existing RBAC outside AllotForge remains unchanged.':'Pool deletion waits for dependent access policies and pending or admitted Kueue workloads to drain.'} Inspect deletion status until the object disappears. A concurrent change will block this request.</div></div><input type="hidden" name="kind" value="${esc(kind)}"><input type="hidden" name="index" value="${Number(index)}">${field('Type the resource name to confirm','confirmation','text','',item.metadata?.name,'required autocomplete="off"')}<div id="form-error" class="validation-message" role="alert"></div></form>`,`<button class="button" data-action="close-dialog">Cancel</button><button class="button danger" type="submit" form="control-delete-form">Delete desired resource</button>`);
}
