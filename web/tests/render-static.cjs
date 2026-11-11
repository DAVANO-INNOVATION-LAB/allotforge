const fs=require('fs'), vm=require('vm'), path=require('path');
const root=path.resolve(__dirname,'../..');
const elements={};
function element(id){return elements[id]??=( {innerHTML:'',textContent:'',className:'',classList:{add(){},remove(){},toggle(){}},setAttribute(){},addEventListener(){},focus(){}});}
const document={getElementById:element,addEventListener(){},querySelector(){return null},activeElement:null};
const context=vm.createContext({document,window:{addEventListener(){}},location:{hash:'#overview'},console,URL,Blob,setTimeout,clearTimeout,fetch:async()=>({ok:true,json:async()=>JSON.parse(fs.readFileSync(path.join(root,'screenshots/state.json'),'utf8'))})});
vm.runInContext(fs.readFileSync(path.join(root,'web/features.js'),'utf8')+'\n'+fs.readFileSync(path.join(root,'web/assignments.js'),'utf8')+'\n'+fs.readFileSync(path.join(root,'web/operations.js'),'utf8')+'\n'+fs.readFileSync(path.join(root,'web/app.js'),'utf8').replace('navigate();load();',''),context);
vm.runInContext(`state=${fs.readFileSync(path.join(root,'screenshots/state.json'),'utf8')};render();`,context);
let html=fs.readFileSync(path.join(root,'web/index.html'),'utf8');
html=html.replace(/<script[^>]*>[\s\S]*?<\/script>/g,'').replace('<div class="loading" role="status">Loading your resource view…</div>',elements.main.innerHTML).replace('<nav aria-label="Main navigation" id="navigation"></nav>',`<nav aria-label="Main navigation" id="navigation">${elements.navigation.innerHTML}</nav>`);
html=html.replace('<link rel="stylesheet" href="/styles.css">',`<style>${fs.readFileSync(path.join(root,'web/styles.css'),'utf8')}</style>`);
html=html.replace('<body>','<body><div class="render-disclosure">STAGE 1 · STATIC HTML/CSS PREVIEW · DEMO FIXTURE DATA · NOT A BROWSER SCREENSHOT</div>');
html=html.replace('</head>',`<style>@page{size:1440px 1150px;margin:0}html{font-size:14px}body{width:1440px}.render-disclosure{background:#f8e6b7;color:#69501e;font-size:10px;letter-spacing:1px;padding:9px 20px;text-align:center;height:33px}.sidebar{top:33px;height:1117px;width:226px}.app-shell{margin-left:226px}main{padding:32px 35px 20px;min-height:0}.topbar{height:72px}.app-footer{position:absolute;bottom:5px;left:226px;right:0}dialog,.toast,.skip-link{display:none}.sidebar-footer{position:absolute;bottom:27px;left:17px;right:17px}button{appearance:none}.capacity-blocks{grid-template-columns:repeat(18,1fr)}.metrics{grid-template-columns:repeat(4,1fr)}.pool-grid{grid-template-columns:repeat(3,1fr)}.dashboard-grid{grid-template-columns:1.5fr 1fr}.heading-actions{min-width:300px}.nav-item svg{width:16px;height:16px}.button svg{width:14px;height:14px}.button{min-height:35px}svg{display:inline-block}</style></head>`);
fs.writeFileSync(path.join(root,'screenshots/stage-1-static.html'),html);
console.log('Hydrated actual UI source with API demo state.');
