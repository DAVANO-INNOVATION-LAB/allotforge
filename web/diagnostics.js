/* On-demand, bounded deployment evidence. Never a readiness certification. */
'use strict';
const diagnosticsMaxAge=5*60*1000;
const diagnosticsCheckLabels={
 'mode-control':'Cluster control mode','mode-execution':'Project execution mode','mode-queue_observability':'Queue observation mode','mode-pending_visibility':'Pending visibility mode','mode-prometheus':'Prometheus configuration',
 'demo-source':'Demo source','kubernetes-configuration':'Kubernetes client configuration','inventory-nodes':'Node inventory read','inventory-pods':'Pod inventory read','managed-resource-api':'Managed resource APIs','admission-api':'Admission API discovery','managed-kueue-api':'Managed Kueue APIs','batch-api':'Job API discovery','network-policy-api':'NetworkPolicy API discovery','queue-observability-api':'Queue observation API','pending-visibility-configuration':'Pending visibility targets','pending-visibility-api':'Pending visibility API','prometheus-query':'Prometheus query availability','write-authorization':'Write authorization','controller-enforcement':'Controller enforcement','tenant-isolation':'Tenant isolation','runtime-hardware':'Runtime and hardware'
};
const diagnosticsStatuses={observed:['Observed','blue'],configured:['Configured','neutral'],disabled:['Disabled','neutral'],unverified:['Unverified','amber'],blocked:['Blocked','red'],unavailable:['Unavailable','amber']};
const diagnosticsEvidence={'configuration':'Configuration only','list-read':'Bounded list read','api-discovery':'API discovery only','read-query':'Fixed read-only query','not-checked':'Not checked'};
let diagnosticsReport=null, diagnosticsError=null, diagnosticsBusy=false, diagnosticsGeneration=0, diagnosticsController=null, diagnosticsCredential='', diagnosticsStale=false, diagnosticsNotice='';
function resetDiagnostics(message=''){
 diagnosticsGeneration++;diagnosticsController?.abort();diagnosticsController=null;diagnosticsBusy=false;diagnosticsReport=null;diagnosticsError=null;diagnosticsStale=false;diagnosticsNotice=message;
}
function syncDiagnosticsCredential(){
 if(diagnosticsCredential===apiToken)return;
 diagnosticsCredential=apiToken;resetDiagnostics('Administrator credential changed. Run a new check for this session.');
}
function leaveDiagnostics(){
 if(page!=='diagnostics')return;
 diagnosticsGeneration++;diagnosticsController?.abort();diagnosticsController=null;diagnosticsBusy=false;diagnosticsError=null;
 if(diagnosticsReport)diagnosticsStale=true;
 diagnosticsNotice='This view was left. Run checks again for a new observation.';
}
function diagnosticText(value,maximum=700){return typeof value==='string'&&value.trim()&&value.length<=maximum?value:null;}
function normalizeDiagnostics(value){
 if(!value||typeof value!=='object'||value.schema_version!==1||!['demo','kubernetes'].includes(value.source)||!Array.isArray(value.checks)||value.checks.length>50||!value.configured_modes||typeof value.configured_modes!=='object')throw new Error('invalid-report');
 const timestamp=typeof value.observed_at==='string'&&value.observed_at.length<=64&&/^\d{4}-\d\d-\d\dT/.test(value.observed_at)?Date.parse(value.observed_at):NaN;
 if(!Number.isFinite(timestamp)||timestamp>Date.now()+60000)throw new Error('invalid-report');
 const seen=new Set();
 const checks=value.checks.map(check=>{
  const label=check&&typeof check.id==='string'&&Object.hasOwn(diagnosticsCheckLabels,check.id)?diagnosticsCheckLabels[check.id]:null;
  const valid=label&&typeof check.status==='string'&&typeof check.evidence==='string'&&Object.hasOwn(diagnosticsStatuses,check.status)&&Object.hasOwn(diagnosticsEvidence,check.evidence)&&diagnosticText(check.detail)&&diagnosticText(check.next_step)&&!seen.has(check.id);
  if(!valid)return {id:null,label:'Unrecognized or malformed check',status:'unverified',evidence:'not-checked',detail:'This result cannot be interpreted by this UI. No successful check is inferred.',next_step:'Use matching UI and server versions, then run checks again.'};
  seen.add(check.id);
  // Positive labels require the evidence type they claim, never a count or summary.
  if(check.status==='observed'&&(value.source==='demo'||!['list-read','api-discovery','read-query'].includes(check.evidence))||check.status==='configured'&&check.evidence!=='configuration')return {id:check.id,label,status:'unverified',evidence:'not-checked',detail:'The reported status and evidence do not agree. This check is unverified.',next_step:'Use matching UI and server versions, then run checks again.'};
  return {id:check.id,label,status:check.status,evidence:check.evidence,detail:check.detail,next_step:check.next_step};
 });
 const modes={};for(const [key,allowed] of Object.entries({control:['enabled','disabled'],execution:['enabled','disabled'],queue_observability:['enabled','disabled'],pending_visibility:['enabled','disabled'],prometheus:['configured','disabled']}))modes[key]=allowed.includes(value.configured_modes[key])?value.configured_modes[key]:'unknown';
 const bounds={};for(const key of ['requests_attempted','max_requests','request_timeout_seconds','report_budget_seconds','max_response_bytes','list_limit']){const n=value.bounds?.[key];bounds[key]=typeof n==='number'&&Number.isFinite(n)&&n>=0&&n<=10000000?n:null;}
 return {source:value.source,observed_at:new Date(timestamp).toISOString(),timestamp,checks,modes,bounds};
}
function diagnosticsCheck(check){
 const [label,tone]=diagnosticsStatuses[check.status];
 return `<article class="diagnostic-check"><div class="diagnostic-check-title"><h3>${esc(check.label)}</h3>${badge(label,tone)}</div><span class="diagnostic-evidence">${esc(diagnosticsEvidence[check.evidence])}</span><p>${esc(check.detail)}</p><div class="diagnostic-next"><strong>Next step</strong><span>${esc(check.next_step)}</span></div></article>`;
}
function diagnosticsGroup(title,subtitle,checks){return `<section class="card diagnostics-section"><div class="card-header"><div><h2>${esc(title)}</h2><p class="caption">${esc(subtitle)}</p></div><span class="diagnostic-count">${checks.length}</span></div><div class="${checks.length?'diagnostic-checks':'diagnostic-no-checks'}">${checks.length?checks.map(diagnosticsCheck).join(''):'<p class="diagnostic-no-evidence">No checks in this category were reported. Nothing is inferred.</p>'}</div></section>`;}
function diagnostics(){
 const report=diagnosticsReport,stale=report&&(diagnosticsStale||Date.now()-report.timestamp>diagnosticsMaxAge),demo=report?.source==='demo';
 const runLabel=diagnosticsBusy?'Running checks…':report?'Run checks again':'Run checks';
 const actions=`<button class="button primary" data-action="run-diagnostics" ${diagnosticsBusy?'disabled':''}>${icon(diagnosticsBusy?'clock':'activity',14)}${runLabel}</button>`+((report||diagnosticsBusy||diagnosticsError)?button('Clear results','clear-diagnostics','x'):'');
 return heading('Deployment diagnostics','Inspect configuration and bounded API evidence. Run only when you choose.',actions)+
 `<section class="diagnostics-hero"><div><div class="eyebrow">EVIDENCE, NOT CERTIFICATION</div><h2>Know what was checked.<br>Keep the unknowns visible.</h2><p>These read-only checks do not deploy workloads or change a cluster. A reachable API and configured modes do not prove a working deployment.</p></div><div class="diagnostic-scope"><span>${icon('shield',19)}Administrator scope</span><p>${apiToken?'Credential supplied for this session. Authentication is verified only by the server.':'Live checks require the existing administrator credential, even when cluster writes are disabled.'}</p><div class="heading-actions">${button(apiToken?'Change credential':'Authorize session','connect-api','shield')}${apiToken?button('Disconnect','forget-api','x'):''}</div><small>Project execution credentials are never used here.</small></div></section>`+
 `<div class="notice access-boundary diagnostics-limit" role="note">${icon('info',20)}<div><strong>Deployment, runtime and hardware remain unverified.</strong> API discovery does not verify controller health, admission enforcement, tenant isolation, CNI behavior, GPU drivers, physical devices or workload execution. There is no overall production-ready verdict.</div></div>`+
 `<div class="diagnostic-run-state" role="status" aria-live="polite" aria-atomic="true">${diagnosticsBusy?'Running one bounded, read-only check. No automatic polling.':diagnosticsNotice||'No automatic checks. Inventory refresh does not rerun diagnostics.'}</div>`+
 (diagnosticsError?`<div class="notice error diagnostic-error" role="alert">${icon('info')}<div><strong>${esc(diagnosticsError.title)}</strong><p>${esc(diagnosticsError.detail)}</p></div></div>`:'')+
 (diagnosticsBusy?`<section class="card diagnostic-wait" aria-busy="true"><span class="signal-orbit">${icon('activity',32)}</span><h2>Collecting bounded evidence</h2><p>Previous results are cleared while this request runs. You can clear the request or leave this view.</p></section>`:report?
 `<section class="diagnostic-provenance ${stale?'is-stale':''}" aria-label="Report provenance"><div><span class="eyebrow">${demo?'DEMO REPORT · NO EXTERNAL PROBES':'KUBERNETES · READ-ONLY REPORT'}</span><strong>${stale?'Stale snapshot. Run again before relying on it.':'Point-in-time evidence, not continuous monitoring.'}</strong><small>Observed <time datetime="${esc(report.observed_at)}">${esc(new Date(report.timestamp).toLocaleString())}</time></small></div>${badge(stale?'Stale snapshot':demo?'Demo only':'Snapshot only',stale?'amber':'neutral')}<p>${demo?'Demo checks never contact Kubernetes or exporters. Configuration is not live evidence.':'Each observation covers only the named operation at that time. Read access does not prove write or controller permissions.'}</p></section>`+
 `<section class="diagnostic-modes" aria-label="Configured modes">${[['control','Cluster control'],['execution','Project execution'],['queue_observability','Queue observation'],['pending_visibility','Pending visibility'],['prometheus','Prometheus']].map(([key,label])=>`<div><span>${label}</span><strong>${esc(({enabled:'Enabled',disabled:'Disabled',configured:'Configured',unknown:'Unknown'})[report.modes[key]])}</strong><small>Configuration only</small></div>`).join('')}</section>`+
 diagnosticsGroup('Configuration evidence','Configured does not mean reachable, enforced or running.',report.checks.filter(c=>c.evidence==='configuration'))+
 diagnosticsGroup('Observed API checks','Bounded reads and discovery only. Inspect each result and its next step.',report.checks.filter(c=>c.evidence!=='configuration'&&c.evidence!=='not-checked'))+
 diagnosticsGroup('Not verified by this check','Disabled, unsupported or missing evidence stays explicit.',report.checks.filter(c=>c.evidence==='not-checked'))+
 `<section class="card diagnostic-bounds"><h2>Request boundaries</h2><dl>${[['requests_attempted','Requests attempted'],['max_requests','Maximum requests'],['request_timeout_seconds','Per-request timeout (s)'],['report_budget_seconds','Report budget (s)'],['list_limit','List item limit'],['max_response_bytes','Response limit (bytes)']].map(([key,label])=>`<div><dt>${label}</dt><dd>${report.bounds[key]===null?'Unknown':count(report.bounds[key])}</dd></div>`).join('')}</dl><p>Reported request limits are not a coverage score. The report budget is checked between operations, not a strict wall-clock deadline; network or DNS work may overrun. Lists are samples, not fleet audits. No secrets, endpoint URLs or raw response bodies are shown.</p></section>`:
 `<section class="card diagnostic-wait"><span class="signal-orbit">${icon('activity',32)}</span><h2>${diagnosticsError?'No diagnostic report available.':'Start with an explicit check.'}</h2><p>Inspect configured modes, limited API reads and missing evidence without waiting for inventory. Demo mode makes no external probe calls.</p><div class="diagnostic-empty-steps"><span><b>01</b> Run checks</span><span><b>02</b> Inspect evidence</span><span><b>03</b> Verify on target</span></div></section>`);
}
function renderDiagnostics(){if(page==='diagnostics'){const action=document.activeElement?.dataset?.action;render();if(action)document.querySelector(`[data-action="${action}"]`)?.focus();}}
async function runDiagnostics(){
 syncDiagnosticsCredential();if(diagnosticsBusy||page!=='diagnostics')return;
 const generation=++diagnosticsGeneration,credential=apiToken,controller=new AbortController();
 diagnosticsController=controller;diagnosticsBusy=true;diagnosticsReport=null;diagnosticsError=null;diagnosticsStale=false;diagnosticsNotice='';renderDiagnostics();
 const timer=setTimeout(()=>controller.abort(),20000);
 try{
  const response=await fetch('/api/v1/diagnostics',{method:'GET',signal:controller.signal,credentials:'same-origin',redirect:'error',cache:'no-store',headers:{Accept:'application/json',...(credential?{Authorization:`Bearer ${credential}`}:{})}});
  if(!response.ok){const error=new Error('http');error.status=response.status;throw error;}
  const report=normalizeDiagnostics(await response.json());
  if(generation!==diagnosticsGeneration||credential!==apiToken||page!=='diagnostics')return;
  diagnosticsReport=report;diagnosticsNotice='Check complete. Inspect individual evidence and the unverified limits below.';
 }catch(error){
  if(generation!==diagnosticsGeneration||credential!==apiToken||page!=='diagnostics')return;
  diagnosticsError=error.status===401||error.status===403?{title:'Administrator authorization required.',detail:'Supply a valid administrator credential for this server and run checks again. Project credentials do not grant diagnostic access.'}:error.status===503?{title:'Diagnostics are unavailable on this server.',detail:'An administrator should verify the server credential configuration and diagnostics availability, then run checks again.'}:error.message==='invalid-report'||error instanceof SyntaxError?{title:'Diagnostic report could not be interpreted.',detail:'Use matching UI and server versions, then run checks again. No successful checks are inferred.'}:error.name==='AbortError'?{title:'Diagnostic request timed out.',detail:'The read-only request did not complete in time. Check server connectivity and run checks again when ready.'}:{title:'Diagnostic request failed.',detail:'Check the same-origin server connection and diagnostics support, then run checks again. Raw server errors are not displayed.'};
 }finally{clearTimeout(timer);if(generation===diagnosticsGeneration){diagnosticsBusy=false;diagnosticsController=null;renderDiagnostics();}}
}
document.addEventListener('click',event=>{const action=event.target.closest('button')?.dataset.action;if(action==='run-diagnostics')runDiagnostics();if(action==='clear-diagnostics'){resetDiagnostics('Results cleared. No automatic checks will run.');renderDiagnostics();document.querySelector('[data-action="run-diagnostics"]')?.focus();}});
