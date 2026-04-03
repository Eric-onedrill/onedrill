/* ══════════════════════════════════════════════
   OneDrill — App Logic
   ══════════════════════════════════════════════
   Segurança: Supabase Auth + Row Level Security
   A anon key abaixo é SEGURA — RLS restringe
   operacoes de escrita a usuarios autenticados.
   ══════════════════════════════════════════════ */

const SUPABASE_URL='https://ofbqtaulvzeltfpqcjhh.supabase.co';
const SUPABASE_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9mYnF0YXVsdnplbHRmcHFjamhoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ0MDMyMjAsImV4cCI6MjA4OTk3OTIyMH0.zPU8SCUAVrTOxp-cuKupXBt0QgRkxnLcpScwnHJKVWE';

let sb,projects=[],tickets=[],parsed=[],parsedProjectTotals={},parsedProjectCoords={};
let isAdmin=false,role='viewer',isSharedView=false,sharedProjectId=null;
let currentDetailId=null,currentPanelId=null,editingTicketId=null,editingProjectId=null,deletingProjectId=null;
let sortCol='ticket',sortAsc=true;
let mf={open:true,damage:true,clear:true,closed:false,cancel:false};
let map,satL,strL,hybL,mkrs=[],lines=[],labels=[],drawn=[];
let shMap,shSatL,shStrL,shHybL,shMkrs=[],shLines=[],shLabels=[];
let clusterGroup=null;
let fieldDrawing=false,fieldPts=[],fieldLine=null,fieldTicketId=null;
let _tt;
let utilCache={},utilCacheLoaded=false;
let dashStateVal='';
let clearProjVal='';
let clearTimeProjVal='';
let progProjVal='';

async function loadUtilCache(){
  try{
    // Busca paginada — Supabase tem limite de 1000 rows por request
    let allData=[];
    let offset=0;
    const pageSize=1000;
    while(true){
      const r=await fetch(`${SUPABASE_URL}/rest/v1/ticket_811_responses?select=ticket_num,utility_name,status,responded_at&order=ticket_num&offset=${offset}&limit=${pageSize}`,{headers:{apikey:SUPABASE_KEY,Authorization:`Bearer ${SUPABASE_KEY}`}});
      const data=await r.json();
      if(!data||!data.length)break;
      allData=allData.concat(data);
      if(data.length<pageSize)break;
      offset+=pageSize;
    }
    utilCache={};
    for(const u of allData){
      const key=String(u.ticket_num||'').trim();
      if(!key)continue;
      if(!utilCache[key])utilCache[key]=[];
      utilCache[key].push(u);
    }
    utilCacheLoaded=true;
    console.log(`[UtilCache] ${allData.length} respostas, ${Object.keys(utilCache).length} tickets`);
    let matched=0,unmatched=0;
    for(const t of tickets){
      const key=String(t.ticket||'').trim();
      if(utilCache[key])matched++;else unmatched++;
    }
    console.log(`[UtilCache] Match: ${matched} tickets com dados 811, ${unmatched} sem dados`);
    syncUtilFilter();
  }catch(e){console.error('Util cache error:',e);}
}

function getTicketPendingUtils(ticketNum){
  const key=String(ticketNum||'').trim();
  const utils=utilCache[key]||[];
  return utils.filter(u=>u.status==='Pending');
}
function getTicketUtils(ticketNum){const key=String(ticketNum||'').trim();return utilCache[key]||[];}

function syncUtilFilter(){
  if(!utilCacheLoaded)return;
  const allUtils={};
  const openTicketNums=new Set(tickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t)).map(t=>String(t.ticket||'').trim()));
  for(const[tnum,resps]of Object.entries(utilCache)){
    if(!openTicketNums.has(tnum))continue;
    for(const u of resps){
      if(u.status==='Pending'){
        if(!allUtils[u.utility_name])allUtils[u.utility_name]=0;
        allUtils[u.utility_name]++;
      }
    }
  }
  const sorted=Object.entries(allUtils).sort((a,b)=>b[1]-a[1]);
  const el=document.getElementById('tbl-util');
  if(el){
    const prev=el.value;
    el.innerHTML='<option value="">Todas utilities</option><option value="__any_pending__">⚠ Qualquer pendente</option><option value="__all_clear__">✅ Todas clear</option>'+sorted.map(([name,count])=>`<option value="${name}">🔴 ${name} (${count})</option>`).join('');
    if(prev)el.value=prev;
  }
  console.log(`[UtilFilter] ${sorted.length} utilities pendentes:`,sorted.map(s=>s[0]+':'+s[1]));
}

async function initSupabase(){
  try{
    sb=supabase.createClient(SUPABASE_URL,SUPABASE_KEY);
    const timeout=new Promise((_,reject)=>setTimeout(()=>reject(new Error('timeout')),8000));
    const fetchData=async()=>{
      const{data:p,error:ep}=await sb.from('projects').select('*').order('name');
      const{data:t,error:et}=await sb.from('tickets').select('*').order('ticket');
      if(ep)throw new Error('Projetos: '+ep.message);
      if(et)throw new Error('Tickets: '+et.message);
      return{p,t};
    };
    const{p,t}=await Promise.race([fetchData(),timeout]);
    projects=(p||[]).map(dbToProject);
    tickets=(t||[]).map(dbToTicket);
    return true;
  }catch(e){console.error('Supabase error:',e);return false;}
}

function dbToProject(r){return{id:r.id,name:r.name,client:r.client||'',state:r.state||'',status:r.status||'Active',desc:r.description||'',totalFeet:r.total_feet||0,centerCoords:(r.center_lat&&r.center_lon)?[r.center_lat,r.center_lon]:null,_manual:r.is_manual||false};}
function dbToTicket(r){return{id:r.id,ticket:r.ticket,projectId:r.project_id||'',company:r.company||'',state:r.state||'',location:r.location||'',status:r.status||'Open',expire:r.expire||'',footage:r.footage||0,client:r.client||'',prime:r.prime||'',job:r.job||'',tipo:r.tipo||'',address:r.address||'',pending:r.pending||'',oldTicket2:r.old_ticket2||'',statusOld:r.status_old||'',expireOld:r.expire_old||'',notes:r.notes||'',fieldPath:r.field_path||null,_geocoded:(r.geocoded_lat&&r.geocoded_lon)?[r.geocoded_lat,r.geocoded_lon]:null,history:r.history||[],attachments:r.attachments||[],status_locked:r.status_locked||false};}
function ticketToDb(t){return{ticket:t.ticket,project_id:t.projectId||null,company:t.company||'',state:t.state||'',location:t.location||'',status:t.status||'Open',expire:t.expire||'',footage:t.footage||0,client:t.client||'',prime:t.prime||'',job:t.job||'',tipo:t.tipo||'',address:t.address||'',pending:t.pending||'',old_ticket2:t.oldTicket2||'',status_old:t.statusOld||'',expire_old:t.expireOld||'',notes:t.notes||'',field_path:t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null,geocoded_lat:t._geocoded?t._geocoded[0]:null,geocoded_lon:t._geocoded?t._geocoded[1]:null,history:t.history||[],attachments:t.attachments||[],status_locked:t.status_locked||false};}
function projectToDb(p){return{id:p.id,name:p.name,client:p.client||'',state:p.state||'',status:p.status||'Active',description:p.desc||'',total_feet:p.totalFeet||0,center_lat:p.centerCoords?p.centerCoords[0]:null,center_lon:p.centerCoords?p.centerCoords[1]:null,is_manual:p._manual||false};}

function setSyncStatus(ok,msg){const d=document.getElementById('sync-dot');const l=document.getElementById('sync-label');if(d)d.style.background=ok?'var(--green)':'var(--red)';if(l)l.textContent=msg;}

async function requireAuth(){
  const{data:{session}}=await sb.auth.getSession();
  if(!session){toast('Faça login como Admin para editar.','danger');return false;}
  return true;
}
async function saveTicketToDb(t){
  if(!await requireAuth())return false;
  setSyncStatus(true,'Salvando...');
  const data=ticketToDb(t);let res;
  if(typeof t.id==='number'&&t.id>0){res=await sb.from('tickets').update(data).eq('id',t.id);}
  else{res=await sb.from('tickets').insert(data).select().single();if(res.data)t.id=res.data.id;}
  if(res.error){setSyncStatus(false,'Erro ao salvar');toast(res.error.code==='42501'?'Sem permissão — faça login como Admin':'Erro: '+res.error.message,'danger');return false;}
  setSyncStatus(true,'Salvo ✓');return true;
}
async function saveProjectToDb(p){if(!await requireAuth())return false;const data=projectToDb(p);const res=await sb.from('projects').upsert(data,{onConflict:'id'});if(res.error){toast(res.error.code==='42501'?'Sem permissão — faça login como Admin':'Erro: '+res.error.message,'danger');return false;}return true;}
async function deleteProjectFromDb(id){if(!await requireAuth())return false;await sb.from('tickets').update({project_id:null}).eq('project_id',id);const res=await sb.from('projects').delete().eq('id',id);return!res.error;}

async function tryLogin(){
  const email=document.getElementById('admin-email').value.trim();
  const pw=document.getElementById('admin-pw').value;
  const errEl=document.getElementById('login-err');
  if(!email||!pw){errEl.textContent='Preencha email e senha';errEl.style.display='block';return;}
  errEl.style.display='none';
  document.querySelector('.login-admin-btn').disabled=true;
  document.querySelector('.login-admin-btn').textContent='Entrando...';
  try{
    const{data,error}=await sb.auth.signInWithPassword({email,password:pw});
    if(error){errEl.textContent=error.message==='Invalid login credentials'?'Email ou senha incorretos':error.message;errEl.style.display='block';document.querySelector('.login-admin-btn').disabled=false;document.querySelector('.login-admin-btn').textContent='Entrar como Admin';return;}
    const{data:roleData}=await sb.from('app_roles').select('role').eq('user_id',data.user.id).single();
    if(roleData&&roleData.role==='admin'){isAdmin=true;role='admin';}
    else{isAdmin=false;role='viewer';}
    document.getElementById('login-screen').style.display='none';
    enterApp();
  }catch(e){errEl.textContent='Erro de conexão';errEl.style.display='block';}
  document.querySelector('.login-admin-btn').disabled=false;
  document.querySelector('.login-admin-btn').textContent='Entrar como Admin';
}
function enterViewer(){isAdmin=false;role='viewer';document.getElementById('login-screen').style.display='none';enterApp();}
async function doLogout(){
  await sb.auth.signOut();
  isAdmin=false;role='viewer';
  document.getElementById('app-shell').style.display='none';
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('pg-dash').classList.add('active');
  document.getElementById('login-screen').style.display='flex';
}

function enterApp(){
  document.getElementById('app-shell').style.display='flex';
  document.getElementById('role-badge').textContent=isAdmin?'ADMIN':'VIEWER';
  document.getElementById('role-badge').style.background=isAdmin?'var(--green-bg)':'var(--accent-bg)';
  document.getElementById('role-badge').style.color=isAdmin?'var(--green)':'var(--accent)';
  const rbm=document.getElementById('role-badge-mob');if(rbm){rbm.textContent=isAdmin?'ADMIN':'VIEWER';rbm.style.background=isAdmin?'var(--green-bg)':'var(--accent-bg)';rbm.style.color=isAdmin?'var(--green)':'var(--accent)';}
  const logoutBtn=document.getElementById('btn-logout');
  if(logoutBtn)logoutBtn.style.display=isAdmin?'':'none';
  if(isAdmin){['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn'].forEach(id=>document.getElementById(id).style.display='');}
  else{['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn'].forEach(id=>document.getElementById(id).style.display='none');document.getElementById('field-status-section').style.display='none';}
  syncAll();renderDash();
  loadUtilCache().then(()=>{renderDash();renderTable();});
  setInterval(async()=>{if(fieldDrawing){console.log('[AutoRefresh] Pulado — desenho em andamento');return;}try{const{data:p}=await sb.from('projects').select('*').order('name');const{data:t}=await sb.from('tickets').select('*').order('ticket');if(p)projects=p.map(dbToProject);if(t)tickets=t.map(dbToTicket);await loadUtilCache();syncAll();setSyncStatus(true,'Atualizado');console.log('[AutoRefresh] OK');}catch(e){console.error('[AutoRefresh]',e);}},300000);
}

/* ========== SHARED PROJECT VIEW ========== */
function enterSharedView(pid){
  isSharedView=true;sharedProjectId=pid;
  const p=projects.find(x=>x.id===pid||x.name===pid);
  if(!p){toast('Projeto não encontrado','danger');enterViewer();return;}
  sharedProjectId=p.id;
  document.getElementById('login-screen').style.display='none';
  document.getElementById('app-shell').style.display='none';
  document.querySelectorAll('.page').forEach(pg=>pg.classList.remove('active'));
  document.getElementById('pg-shared').classList.add('active');
  const ts0=tickets.filter(t=>t.projectId===p.id);
  const locs0=[...new Set(ts0.map(t=>t.location).filter(Boolean).map(l=>l.replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()))].join(', ')||p.state||'';
  document.getElementById('shared-proj-name').textContent=locs0+(locs0?' — ':'')+p.name+(p.client?' · '+p.client:'');

  const ts=tickets.filter(t=>t.projectId===p.id&&!isSuperseded(t));
  const open=ts.filter(t=>t.status==='Open').length;
  const clear=ts.filter(t=>t.status==='Clear').length;
  const damage=ts.filter(t=>t.status==='Damage').length;
  const totalFt=ts.reduce((s,t)=>s+(t.footage||0),0);
  document.getElementById('shared-stats').innerHTML=`
    <div class="shared-stat"><span class="shared-stat-val">${ts.length}</span><span class="shared-stat-lbl">Total</span></div>
    <div class="shared-stat" style="border-color:var(--red-border)"><span class="shared-stat-val" style="color:var(--red)">${open}</span><span class="shared-stat-lbl">Open</span></div>
    <div class="shared-stat" style="border-color:var(--green-border)"><span class="shared-stat-val" style="color:var(--green)">${clear}</span><span class="shared-stat-lbl">Clear</span></div>
    <div class="shared-stat" style="border-color:var(--amber-border)"><span class="shared-stat-val" style="color:var(--amber)">${damage}</span><span class="shared-stat-lbl">Damage</span></div>`;

  renderSharedList();
  setTimeout(()=>initSharedMap(p),150);
}

function exitSharedView(){
  isSharedView=false;
  document.getElementById('pg-shared').classList.remove('active');
  history.replaceState(null,'',window.location.pathname);
  document.getElementById('login-screen').style.display='flex';
}

function toggleSharedPanel(){
  const sb=document.getElementById('shared-sidebar');
  const ov=document.getElementById('shared-overlay');
  const open=sb.classList.toggle('mob-open');
  ov.classList.toggle('open',open);
  document.getElementById('shared-toggle-label').textContent=open?'Fechar':'Ver tickets';
}

function sharedFiltered(){
  const sr=(document.getElementById('sh-srch')?.value||'').toLowerCase();
  const st=document.getElementById('sh-stat')?.value||'';
  return tickets.filter(t=>{
    if(t.projectId!==sharedProjectId)return false;
    if(isSuperseded(t))return false;
    if(st&&t.status!==st)return false;
    if(sr&&!t.ticket.toLowerCase().includes(sr)&&!(t.client||'').toLowerCase().includes(sr)&&!(t.address||'').toLowerCase().includes(sr))return false;
    return true;
  });
}

function renderSharedList(){
  const f=sharedFiltered();
  document.getElementById('shared-count').textContent=`${f.length} ticket${f.length!==1?'s':''}`;
  document.getElementById('shared-list').innerHTML=f.length?f.map(t=>`<div class="tcard s-${(t.status||'').toLowerCase()}" data-id="${t.id}" onclick="shFocusTicket(${t.id})"><div class="tcard-top"><span class="tcard-num">${t.ticket}</span><span class="sbadge b-${t.status.toLowerCase()}">${t.status}</span></div><div class="tcard-client">${t.client}${t.prime?' · '+t.prime:''}</div><div class="tcard-meta"><span>${t.location}, ${t.state}</span><span>${t.footage} ft</span>${t.tipo?`<span>${t.tipo}</span>`:''}</div></div>`).join(''):'<div style="text-align:center;padding:28px;color:var(--muted);font-size:13px">Nenhum ticket</div>';
  renderSharedMap();
}

function initSharedMap(p){
  if(shMap)return;
  const center=p.centerCoords||[28.5,-81.4];
  shMap=L.map('shared-map-el',{zoomControl:false,preferCanvas:true}).setView(center,14);
  shSatL=L.tileLayer('https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',{maxZoom:21});
  shHybL=L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',{maxZoom:21});
  shStrL=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:20});
  shHybL.addTo(shMap);
  L.control.zoom({position:'bottomright'}).addTo(shMap);
  renderSharedMap();
  setTimeout(()=>shFitAll(),300);
}

function renderSharedMap(){
  if(!shMap)return;
  shMkrs.forEach(m=>shMap.removeLayer(m));shLines.forEach(l=>shMap.removeLayer(l));shLabels.forEach(l=>shMap.removeLayer(l));
  shMkrs=[];shLines=[];shLabels=[];
  const f=sharedFiltered();
  for(const t of f){
    const c=scol(t.status),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
    const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null;
    if(coords){
      const mi=op=>L.divIcon({className:'',html:`<div style="width:9px;height:9px;border-radius:50%;background:${c};border:2px solid white;opacity:${op};box-shadow:0 1px 4px rgba(0,0,0,.3)"></div>`,iconSize:[9,9],iconAnchor:[4,4]});
      const m1=L.marker(coords[0],{icon:mi(1)}).addTo(shMap);const m2=L.marker(coords[coords.length-1],{icon:mi(.7)}).addTo(shMap);
      m1.bindPopup(buildPopup(t,c));m2.bindPopup(buildPopup(t,c));shMkrs.push(m1,m2);
      const ln=L.polyline(coords,{color:c,weight:lw,opacity:.92,dashArray:dash}).addTo(shMap);shLines.push(ln);
      const mid=coords[Math.floor(coords.length/2)]||coords[0];
      const lbl=L.marker(mid,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="border-left:3px solid ${c}">${t.ticket}</a>`,iconAnchor:[32,10]})}).addTo(shMap);shLabels.push(lbl);
    }else{
      let pos=t._geocoded||null;
      if(!pos){const p=projects.find(x=>x.id===sharedProjectId);const cc=p?.centerCoords||cityCoords(t.location);const jitter=()=>(Math.random()-.5)*0.002;pos=[cc[0]+jitter(),cc[1]+jitter()];
        if(t.address&&t.address!=='—'&&!t._geocoding){t._geocoding=true;geocodeAddress(t.address,t.location,t.state).then(coords=>{if(coords){t._geocoded=coords;t._geocoding=false;renderSharedMap();}else t._geocoding=false;});}}
      const mi=L.divIcon({className:'',html:`<div style="width:11px;height:11px;border-radius:50%;background:${c};border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,.3);opacity:${t._geocoded?1:.6}"></div>`,iconSize:[11,11],iconAnchor:[5,5]});
      const mk=L.marker(pos,{icon:mi}).addTo(shMap);mk.bindPopup(buildPopup(t,c));shMkrs.push(mk);
      const lbl=L.marker(pos,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="margin-top:12px;display:block;border-left:3px solid ${c}">${t.ticket}</a>`,iconAnchor:[32,-2]})}).addTo(shMap);shLabels.push(lbl);
    }
  }
}

function shFocusTicket(id){
  if(window.innerWidth<=768){const sb=document.getElementById('shared-sidebar');const ov=document.getElementById('shared-overlay');sb.classList.remove('mob-open');ov.classList.remove('open');document.getElementById('shared-toggle-label').textContent='Ver tickets';}
  const t=tickets.find(x=>x.id===id);if(!t||!shMap)return;
  document.querySelectorAll('#shared-list .tcard').forEach(c=>c.classList.remove('active'));const cd=document.querySelector(`#shared-list [data-id="${id}"]`);if(cd)cd.classList.add('active');
  if(t.fieldPath&&t.fieldPath.length>=2){shMap.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});}else if(t._geocoded){shMap.setView(t._geocoded,18);}
}

function shSetLayer(t){[shSatL,shStrL,shHybL].forEach(l=>{try{shMap.removeLayer(l)}catch{}});['sh-bsat','sh-bstr','sh-bhyb'].forEach(id=>{const el=document.getElementById(id);if(el)el.classList.remove('active')});if(t==='sat'){shSatL.addTo(shMap);document.getElementById('sh-bsat').classList.add('active');}else if(t==='hyb'){shHybL.addTo(shMap);document.getElementById('sh-bhyb').classList.add('active');}else{shStrL.addTo(shMap);document.getElementById('sh-bstr').classList.add('active');}}
function shFitAll(){if(!shMap)return;const ts=tickets.filter(t=>t.projectId===sharedProjectId);const wc=ts.filter(t=>t.fieldPath&&t.fieldPath.length>=2);if(wc.length)shMap.fitBounds(L.latLngBounds(wc.flatMap(t=>t.fieldPath)),{padding:[40,40]});else{const p=projects.find(x=>x.id===sharedProjectId);if(p?.centerCoords)shMap.setView(p.centerCoords,15);}}

/* ========== END SHARED VIEW ========== */

const CITY_COORDS={'vigo - terre haute':[39.4667,-87.4139],'terre haute':[39.4667,-87.4139],'vigo':[39.4667,-87.4139],'orlando - tangelo park':[28.4538,-81.4503],'tangelo park':[28.4538,-81.4503],'orlando':[28.5383,-81.3792],'arcadia':[27.2142,-81.8579],'volusia - deland':[29.0283,-81.3031],'deland':[29.0283,-81.3031],'pinelas - st. petersburg':[27.7676,-82.6403],'st. petersburg':[27.7676,-82.6403],'default':[28.5383,-81.3792]};
function cityCoords(l){const k=(l||'').toLowerCase().trim();if(CITY_COORDS[k])return CITY_COORDS[k];for(const[kk,v]of Object.entries(CITY_COORDS)){if(kk!=='default'&&(k.includes(kk)||kk.includes(k)))return v;}return CITY_COORDS['default']}
function projCenter(pid){const p=projects.find(x=>x.id===pid);return p?.centerCoords||null;}

async function geocodeAddress(address,location,state){
  if(!address||address==='—')return null;
  const queries=[`${address}, ${location}, ${state}, USA`,`${address}, ${location}, USA`,`${address}, ${state}, USA`];
  const stateL=(state||'').toLowerCase();
  const inBounds=(lat,lon)=>{if(stateL==='in')return lat>36&&lat<43&&lon>-89&&lon<-84;if(stateL==='fl')return lat>24&&lat<32&&lon>-88&&lon<-79;return true;};
  for(const q of queries){try{await new Promise(r=>setTimeout(r,200));const res=await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=3&countrycodes=us`,{headers:{'Accept-Language':'en','User-Agent':'OneDrill/1.0'}});const d=await res.json();if(d?.length){for(const item of d){const lat=parseFloat(item.lat),lon=parseFloat(item.lon);if(inBounds(lat,lon))return[lat,lon];}}}catch{}}
  return null;
}

function scol(s){const m={open:'#dc2626',clear:'#16a34a',damage:'#d97706',closed:'#1a1a18',cancel:'#6d28d9'};return m[(s||'').toLowerCase()]||'#9a9888'}
function tipoDash(t){return(t||'').toLowerCase().includes('main')?null:'6,4'}
function lineWeight(t){return(t||'').toLowerCase().includes('main')?5:3}

function initMap(){
  if(map)return;
  map=L.map('map',{zoomControl:false,preferCanvas:true}).setView([28.4,-81.4],12);
  satL=L.tileLayer('https://mt1.google.com/vt/lyrs=s&x={x}&y={y}&z={z}',{attribution:'© Google',maxZoom:21});
  hybL=L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',{attribution:'© Google',maxZoom:21});
  strL=L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{attribution:'© OSM',maxZoom:20});
  hybL.addTo(map);
  L.control.zoom({position:'bottomright'}).addTo(map);
  map.on('click',onMC);map.on('dblclick',onMDC);
  renderMap();
}
function setLayer(t){[satL,strL,hybL].forEach(l=>{try{map.removeLayer(l)}catch{}});['bsat','bstr','bhyb'].forEach(id=>{const el=document.getElementById(id);if(el)el.classList.remove('active')});if(t==='sat'){satL.addTo(map);document.getElementById('bsat').classList.add('active');}else if(t==='hyb'){hybL.addTo(map);document.getElementById('bhyb').classList.add('active');}else{strL.addTo(map);document.getElementById('bstr').classList.add('active');}}

async function renderMap(){
  if(!map)return;
  mkrs.forEach(m=>map.removeLayer(m));lines.forEach(l=>map.removeLayer(l));labels.forEach(l=>map.removeLayer(l));if(clusterGroup){map.removeLayer(clusterGroup);}mkrs=[];lines=[];labels=[];clusterGroup=L.markerClusterGroup({maxClusterRadius:40,spiderfyOnMaxZoom:true,showCoverageOnHover:false,disableClusteringAtZoom:17});
  for(const t of mapFiltered()){
    const c=scol(t.status),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
    const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null;
    if(coords){
      const mi=op=>L.divIcon({className:'',html:`<div style="width:9px;height:9px;border-radius:50%;background:${c};border:2px solid white;opacity:${op};box-shadow:0 1px 4px rgba(0,0,0,.3)"></div>`,iconSize:[9,9],iconAnchor:[4,4]});
      const m1=L.marker(coords[0],{icon:mi(1)}).addTo(map);const m2=L.marker(coords[coords.length-1],{icon:mi(.7)}).addTo(map);
      m1.bindPopup(buildPopup(t,c));m2.bindPopup(buildPopup(t,c));m1.on('click',()=>hiT(t.id));m2.on('click',()=>hiT(t.id));mkrs.push(m1,m2);
      const ln=L.polyline(coords,{color:c,weight:lw,opacity:.92,dashArray:dash}).addTo(map);ln.on('click',()=>{hiT(t.id);showPanel(t)});lines.push(ln);
      const mid=coords[Math.floor(coords.length/2)]||coords[0];
      const lbl=L.marker(mid,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="border-left:3px solid ${c}">${t.ticket}</a>`,iconAnchor:[32,10]})}).addTo(map);labels.push(lbl);
    }else{
      let pos=t._geocoded||null;
      if(!pos){const pc=projCenter(t.projectId);const cc=pc||cityCoords(t.location);const jitter=()=>(Math.random()-.5)*(pc?0.002:0.006);pos=[cc[0]+jitter(),cc[1]+jitter()];
        if(t.address&&t.address!=='—'&&!t._geocoding){t._geocoding=true;geocodeAddress(t.address,t.location,t.state).then(coords=>{if(coords){t._geocoded=coords;t._geocoding=false;renderMap();}else t._geocoding=false;});}}
      const mi=L.divIcon({className:'',html:`<div style="width:11px;height:11px;border-radius:50%;background:${c};border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,.3);opacity:${t._geocoded?1:.6}"></div>`,iconSize:[11,11],iconAnchor:[5,5]});
      const mk=L.marker(pos,{icon:mi});mk.bindPopup(buildPopup(t,c));mk.on('click',()=>hiT(t.id));mkrs.push(mk);clusterGroup.addLayer(mk);
      const lbl=L.marker(pos,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="margin-top:12px;display:block;border-left:3px solid ${c}">${t.ticket}</a>`,iconAnchor:[32,-2]})}).addTo(map);labels.push(lbl);
    }
  }
  if(clusterGroup)map.addLayer(clusterGroup);
}

function buildPopup(t,c){const proj=projects.find(p=>p.id===t.projectId);return`<div style="font-family:'DM Sans',sans-serif;font-size:13px;line-height:1.6;min-width:180px;padding:2px"><div style="font-weight:700;color:#18180f;margin-bottom:6px;font-size:14px;font-family:'DM Mono',monospace">${t.ticket}</div>${proj?`<div><span style="color:#9a9888">Projeto: </span>${proj.name}</div>`:''}<div><span style="color:#9a9888">Cliente: </span>${t.client}</div>${t.prime?`<div><span style="color:#9a9888">Prime: </span>${t.prime}</div>`:''}<div><span style="color:#9a9888">Footage: </span>${t.footage} ft</div>${t.tipo?`<div><span style="color:#9a9888">Tipo: </span>${t.tipo}</div>`:''}<div><span style="color:#9a9888">Status: </span><span style="color:${c};font-weight:700">${t.status}</span></div><div><span style="color:#9a9888">Expira: </span>${t.expire||'—'}</div>${t.address?`<div><span style="color:#9a9888">Endereço: </span>${t.address}</div>`:''}<div style="margin-top:7px;padding-top:7px;border-top:1px solid #e2e0da;display:flex;gap:8px"><a href="#" onclick="openTicketDetail(${t.id});return false;" style="color:#1a6cf0;font-size:12px;font-weight:600">Detalhes →</a><a href="#" onclick="openNavigation(${t.id});return false;" style="color:#16a34a;font-size:12px;font-weight:600">🗺 Navegar</a></div></div>`;}

function openNavigation(id){const t=tickets.find(x=>x.id===id);if(!t)return;const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath[0]:t._geocoded;if(coords){window.open(`https://www.google.com/maps/dir/?api=1&destination=${coords[0]},${coords[1]}&travelmode=driving`,'_blank');}else if(t.address){window.open(`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(t.address+', '+t.location+', '+t.state)}`,'_blank');}else{toast('Sem coordenadas para navegar','warn');}}

function showPanel(t){const c=scol(t.status);const proj=projects.find(p=>p.id===t.projectId);currentPanelId=t.id;document.getElementById('ptitle-txt').textContent=t.ticket;document.getElementById('pbody').innerHTML=`${proj?`<div class="mp-row"><span class="mp-key">Projeto</span><span class="mp-val">${proj.name}</span></div>`:''}<div class="mp-row"><span class="mp-key">Cliente</span><span class="mp-val">${t.client}</span></div>${t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${t.prime}</span></div>`:''}<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val" style="cursor:pointer;color:var(--accent)" onclick="quickEditFootage(currentDetailId);return false;" title="Clique para editar">${t.footage} ft ✏</span></div>${t.tipo?`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${t.tipo}</span></div>`:''}<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${t.status}</span></div><div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val">${t.expire||'—'}</span></div>`;document.getElementById('panel').classList.add('vis');}

function hiT(id){document.querySelectorAll('.tcard').forEach(c=>c.classList.remove('active'));const cd=document.querySelector(`[data-id="${id}"]`);if(cd){cd.classList.add('active');cd.scrollIntoView({behavior:'smooth',block:'nearest'})}const t=tickets.find(x=>x.id===id);if(t)showPanel(t);}

function isSuperseded(t){
  // Ticket foi renovado se outro ticket aponta pra ele no oldTicket2
  const tnum=String(t.ticket||'').trim();
  return tickets.some(other=>other.id!==t.id&&String(other.oldTicket2||'').trim()===tnum);
}

function mapFiltered(){const pf=document.getElementById('proj-filter')?.value||'';const sr=(document.getElementById('srch')?.value||'').toLowerCase();const cl=document.getElementById('fcli')?.value||'';const lc=document.getElementById('floc')?.value||'';return tickets.filter(t=>{
  if(isSuperseded(t))return false;
  const sl=t.status?.toLowerCase();if(sl==='open'&&!mf.open)return false;if(sl==='damage'&&!mf.damage)return false;if(sl==='clear'&&!mf.clear)return false;if(sl==='closed'&&!mf.closed)return false;if(sl==='cancel'&&!mf.cancel)return false;if(pf&&t.projectId!==pf)return false;if(cl&&t.client!==cl)return false;if(lc&&t.location!==lc)return false;if(sr&&!t.ticket.toLowerCase().includes(sr)&&!(t.client||'').toLowerCase().includes(sr)&&!(t.location||'').toLowerCase().includes(sr))return false;const muf=document.getElementById('map-util-filter')?.value||'';if(muf==='__pending__'){const pu=getTicketPendingUtils(String(t.ticket).trim());if(!pu.length)return false;}else if(muf){const pu=getTicketPendingUtils(String(t.ticket).trim());if(!pu.some(p=>p.utility_name===muf))return false;}return true;});}

function toggleMF(key){mf[key]=!mf[key];const btn=document.getElementById('ft-'+key);const oc={open:'on-open',damage:'on-damage',clear:'on-clear',closed:'on-closed',cancel:'on-cancel'};btn.className='ftog'+(mf[key]?' '+oc[key]:'');redrawAll();}
function redrawAll(){renderList();renderMap();}
function onProjFilter(){
  const pf=document.getElementById('proj-filter').value;
  if(pf&&map){
    const p=projects.find(x=>x.id===pf);
    if(p?.centerCoords){
      map.setView(p.centerCoords,16);
    }else{
      // Calcula centro a partir dos tickets do projeto
      const pts=tickets.filter(t=>t.projectId===pf);
      const allCoords=[];
      for(const t of pts){
        if(t.fieldPath&&t.fieldPath.length>=2){allCoords.push(...t.fieldPath);}
        else if(t._geocoded){allCoords.push(t._geocoded);}
      }
      if(allCoords.length){
        map.fitBounds(L.latLngBounds(allCoords),{padding:[60,60],maxZoom:17});
      }else{
        // Tenta geocodificar pelo nome da localidade dos tickets
        const locs=pts.map(t=>t.location).filter(Boolean);
        const loc=locs[0]||'';
        const cc=cityCoords(loc);
        if(cc)map.setView(cc,14);
      }
    }
  }
  redrawAll();
}
function fitAll(){if(!map)return;const filtered=mapFiltered();const allCoords=[];for(const t of filtered){if(t.fieldPath&&t.fieldPath.length>=2)allCoords.push(...t.fieldPath);else if(t._geocoded)allCoords.push(t._geocoded);}if(allCoords.length)map.fitBounds(L.latLngBounds(allCoords),{padding:[40,40]});else map.setView([28.4,-81.4],10);}

function renderList(){const f=mapFiltered();document.getElementById('tcount').textContent=`${f.length} ticket${f.length!==1?'s':''}`;document.getElementById('tlist').innerHTML=f.length?f.map(t=>`<div class="tcard s-${(t.status||'').toLowerCase()}" data-id="${t.id}" onclick="focusT(${t.id})"><div class="tcard-top"><span class="tcard-num">${t.ticket}</span><span class="sbadge b-${t.status.toLowerCase()}">${t.status}</span></div><div class="tcard-client">${t.client}${t.prime?' · '+t.prime:''}</div><div class="tcard-meta"><span>${t.location}, ${t.state}</span><span>${t.footage} ft</span>${t.tipo?`<span>${t.tipo}</span>`:''}</div>${t.pending?`<div style="font-size:10px;color:var(--amber);font-weight:600;margin-top:2px">⏳ ${t.pending}</div>`:''}</div>`).join(''):'<div style="text-align:center;padding:28px 16px;color:var(--muted);font-size:13px">Nenhum ticket</div>';}

function focusT(id){hiT(id);const t=tickets.find(x=>x.id===id);if(!t||!map)return;if(window.innerWidth<=768){const sb=document.getElementById('map-sidebar');const ov=document.getElementById('sb-overlay');sb.classList.remove('mob-open');ov.classList.remove('open');}if(t.fieldPath&&t.fieldPath.length>=2){map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});}else if(t._geocoded){map.setView(t._geocoded,18);}else{const pc=projCenter(t.projectId);const cc=pc||cityCoords(t.location);map.setView(cc,pc?17:15);if(t.address&&!t._geocoding){t._geocoding=true;geocodeAddress(t.address,t.location,t.state).then(coords=>{if(coords){t._geocoded=coords;t._geocoding=false;map.setView(coords,18);renderMap();}else t._geocoding=false;});}}}

function startFieldDraw(tid){const t=tickets.find(x=>x.id===tid);if(!t)return;if(fieldDrawing)cancelFieldDraw();fieldDrawing=true;fieldPts=[];fieldTicketId=tid;if(t.fieldPath&&t.fieldPath.length>=2)fieldPts=[...t.fieldPath];document.getElementById('field-draw-panel').style.display='block';document.getElementById('field-draw-ticket').textContent=t.ticket+' — '+(t.tipo||'');document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';map.getContainer().style.cursor='crosshair';if(fieldPts.length>=2){if(fieldLine)map.removeLayer(fieldLine);fieldLine=L.polyline(fieldPts,{color:scol(t.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);}closeModal('ov-detail');}
function cancelFieldDraw(){fieldDrawing=false;fieldPts=[];fieldTicketId=null;if(fieldLine){map.removeLayer(fieldLine);fieldLine=null;}document.getElementById('field-draw-panel').style.display='none';map.getContainer().style.cursor='';}
function undoFieldPt(){if(!fieldPts.length)return;fieldPts.pop();if(fieldLine)map.removeLayer(fieldLine);const t=tickets.find(x=>x.id===fieldTicketId);if(fieldPts.length>=2)fieldLine=L.polyline(fieldPts,{color:scol(t?.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);else fieldLine=null;document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';}
async function saveFieldPath(){if(fieldPts.length<2){toast('Mínimo 2 pontos.','warn');return;}const t=tickets.find(x=>x.id===fieldTicketId);if(!t)return;t.fieldPath=[...fieldPts];t.history.push({ts:Date.now(),action:`Trajeto desenhado (${fieldPts.length} pontos)`,color:'#6d28d9'});const ok=await saveTicketToDb(t);if(ok){toast(`Trajeto salvo — ${t.ticket}`,'success');}cancelFieldDraw();renderMap();setTimeout(()=>{if(map&&t.fieldPath)map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});},200);}
function clearFieldPath(){fieldPts=[];if(fieldLine){map.removeLayer(fieldLine);fieldLine=null;}document.getElementById('field-draw-count').textContent='0 pts';}
function onMC(e){if(fieldDrawing){const t=tickets.find(x=>x.id===fieldTicketId);fieldPts.push([e.latlng.lat,e.latlng.lng]);if(fieldLine)map.removeLayer(fieldLine);if(fieldPts.length>=2)fieldLine=L.polyline(fieldPts,{color:scol(t?.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';}}
function onMDC(e){if(fieldDrawing){saveFieldPath();}}
function goDrawField(tid){closeModal('ov-detail');nav('map');setTimeout(()=>{initMap();const t=tickets.find(x=>x.id===tid);if(t){if(t.fieldPath&&t.fieldPath.length>=2)map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[80,80],maxZoom:19});else if(t._geocoded)map.setView(t._geocoded,19);else{const pc=projCenter(t.projectId);map.setView(pc||cityCoords(t.location),pc?17:15);if(t.address){geocodeAddress(t.address,t.location,t.state).then(c=>{if(c){t._geocoded=c;map.setView(c,19);}});}}}setTimeout(()=>startFieldDraw(tid),400);},100);}

function openTicketDetail(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  currentDetailId=id;
  const c=scol(t.status);const proj=projects.find(p=>p.id===t.projectId);
  document.getElementById('det-title').textContent=t.ticket;
  document.getElementById('det-sub').textContent=(proj?proj.name+' · ':'')+t.client+(t.prime?' · '+t.prime:'');
  const hasOldInfo=t.oldTicket2||t.statusOld||t.expireOld||t.pending;
  document.getElementById('det-info').innerHTML=`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${t.status}${t.status_locked?' 🔒':''}</span></div><div class="mp-row"><span class="mp-key">Empresa</span><span class="mp-val">${t.company||'—'}</span></div>${t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${t.prime}</span></div>`:''}<div class="mp-row"><span class="mp-key">Local</span><span class="mp-val">${t.location}, ${t.state}</span></div><div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val">${t.footage} ft</span></div><div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${t.tipo||'—'}</span></div><div class="mp-row"><span class="mp-key">Job #</span><span class="mp-val">${t.job||'—'}</span></div><div class="mp-row"><span class="mp-key">Endereço</span><span class="mp-val">${t.address||'—'}</span></div><div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val">${t.expire||'—'}</span></div><div class="mp-row"><span class="mp-key">Trajeto</span><span class="mp-val" style="color:${t.fieldPath?'var(--purple)':'var(--muted)'}">${t.fieldPath?`✏️ Campo (${t.fieldPath.length} pts)`:'Sem trajeto'}</span></div>${t.notes?`<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--text2);white-space:pre-wrap;word-break:break-word">${t.notes}</div>`:''}${hasOldInfo?`<div style="margin-top:10px;padding:9px 11px;background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r)"><div style="font-size:10px;font-weight:700;color:#92400e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">📋 Ticket Anterior</div>${t.pending?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Pending</span><span class="mp-val" style="color:var(--amber)">${t.pending}</span></div>`:''}${t.oldTicket2?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Old Ticket #</span><span class="mp-val" style="font-family:var(--mono);color:#b45309">${t.oldTicket2}</span></div>`:''}${t.statusOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Status Ant.</span><span class="mp-val" style="color:#92400e">${t.statusOld}</span></div>`:''}${t.expireOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Exp. Ant.</span><span class="mp-val" style="color:#92400e">${t.expireOld}</span></div>`:''}</div>`:''}`;
  const lockBadge=document.getElementById('det-lock-badge');const unlockBtn=document.getElementById('det-unlock-btn');
  if(t.status_locked){lockBadge.style.display='';unlockBtn.style.display='';}else{lockBadge.style.display='none';unlockBtn.style.display='none';}
  if(!isSharedView&&isAdmin){document.getElementById('det-edit-btn').style.display='';document.getElementById('det-draw-btn').style.display='';document.getElementById('field-status-section').style.display='';}else{document.getElementById('det-edit-btn').style.display='none';document.getElementById('det-draw-btn').style.display='none';document.getElementById('field-status-section').style.display='none';}
  renderHistory(t);renderMiniMap(t);renderUtils(t);openModal('ov-detail');
}

function renderHistory(t){document.getElementById('det-hist').innerHTML=t.history?.length?[...t.history].reverse().map(h=>`<div class="hist-item"><div class="hist-dot" style="background:${h.color||'#9a9888'}"></div><div style="flex:1"><div style="color:var(--text2);font-size:12px">${h.action}</div><div class="hist-time">${fmtDt(h.ts)}</div></div></div>`).join(''):'<div style="color:var(--muted);font-size:12px">Sem histórico</div>';}
function fmtDt(ts){const d=new Date(ts);return d.toLocaleDateString('pt-BR')+' '+d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});}




async function quickEditFootage(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  const val=prompt('Footage para '+t.ticket+':',t.footage||0);
  if(val===null)return;
  const num=parseInt(val)||0;
  t.footage=num;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'Footage: '+num+' ft',color:'#1a6cf0'});
  const ok=await saveTicketToDb(t);
  if(ok){toast('Footage atualizado: '+num+' ft','success');openTicketDetail(id);syncAll();}
}

async function setManualStatus(newStatus){const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;const old=t.status;t.status=newStatus;t.status_locked=true;t.history=t.history||[];t.history.push({ts:Date.now(),action:`Status manual: ${old} → ${newStatus} 🔒`,color:scol(newStatus)});const ok=await saveTicketToDb(t);if(ok){toast(`✅ Status: ${old} → ${newStatus} (travado)`,'success');openTicketDetail(currentDetailId);syncAll();}}
async function unlockStatus(id){const t=tickets.find(x=>x.id===id);if(!t)return;t.status_locked=false;const ok=await saveTicketToDb(t);if(ok){toast('🔓 Status desbloqueado','success');openTicketDetail(id);}}

let miniMap=null;
function renderMiniMap(t){
  const container=document.getElementById('mini-map-container');if(!container)return;
  if(miniMap){try{miniMap.remove();}catch(e){}miniMap=null;}
  const hasPath=t.fieldPath&&t.fieldPath.length>=2;const hasGeo=t._geocoded;
  if(!hasPath&&!hasGeo){container.innerHTML='<div class="mini-map-empty">📍 Sem localização<br><span style="font-size:10px">Use ✏️ Desenhar para marcar no mapa</span></div>';return;}
  container.innerHTML='<div class="mini-map-wrap"><div id="mini-map"></div></div>';
  setTimeout(()=>{try{miniMap=L.map('mini-map',{zoomControl:false,attributionControl:false,dragging:false,scrollWheelZoom:false});L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',{maxZoom:21}).addTo(miniMap);const c=scol(t.status);if(hasPath){const ln=L.polyline(t.fieldPath,{color:c,weight:4,opacity:0.9}).addTo(miniMap);miniMap.fitBounds(ln.getBounds(),{padding:[14,14]});}else{miniMap.setView(t._geocoded,17);L.circleMarker(t._geocoded,{radius:8,color:c,fillColor:c,fillOpacity:0.9,weight:2}).addTo(miniMap);}}catch(e){console.error('Mini map:',e);}},120);
}
function openFullMap(id){closeModal('ov-detail');if(isSharedView){if(shMap){const t=tickets.find(x=>x.id===id);if(t?.fieldPath?.length>=2)shMap.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});else if(t?._geocoded)shMap.setView(t._geocoded,18);}return;}nav('map');setTimeout(()=>{initMap();const t=tickets.find(x=>x.id===id);if(!t)return;if(t.fieldPath&&t.fieldPath.length>=2){map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});}else if(t._geocoded){map.setView(t._geocoded,18);}hiT(id);},200);}

async function renderUtils(t){
  const el=document.getElementById('util-list');const sm=document.getElementById('util-summary');if(!el)return;
  el.innerHTML='<div style="color:var(--muted);font-size:12px">Carregando...</div>';
  try{
    const r=await fetch(`${SUPABASE_URL}/rest/v1/ticket_811_responses?ticket_num=eq.${t.ticket}&select=utility_name,status,response_text,responded_at&order=utility_name`,{headers:{apikey:SUPABASE_KEY,Authorization:`Bearer ${SUPABASE_KEY}`}});
    const data=await r.json();
    if(!data||!data.length){el.innerHTML='<div style="color:var(--muted);font-size:12px">Sem dados de utilities</div>';if(sm)sm.textContent='';return;}
    const pending=data.filter(u=>u.status==='Pending');const cleared=data.filter(u=>u.status==='Clear'||u.status==='Private');const marked=data.filter(u=>u.status==='Marked');
    if(sm){const parts=[];if(pending.length)parts.push(`<span style="color:var(--red)">${pending.length} pendente${pending.length>1?'s':''}</span>`);if(marked.length)parts.push(`<span style="color:var(--amber)">${marked.length} marcada${marked.length>1?'s':''}</span>`);if(cleared.length)parts.push(`<span style="color:var(--green)">${cleared.length} clear</span>`);sm.innerHTML=parts.join(' · ');}
    const badgeClass={Pending:'ub-pending',Clear:'ub-clear',Marked:'ub-marked',Private:'ub-private',Unmarked:'ub-clear'};
    const label={Pending:'Pendente',Clear:'Clear',Marked:'Marcado',Private:'Privado',Unmarked:'Desmarcado'};
    const order={Pending:0,Marked:1,Private:2,Clear:3,Unmarked:4};
    data.sort((a,b)=>(order[a.status]||9)-(order[b.status]||9));
    el.innerHTML=data.map(u=>{
      const resp=(u.response_text||'').trim();
      let detail='';
      if(resp && u.status==='Clear'){
        const short=resp.length>80?resp.substring(0,80)+'…':resp;
        detail=`<div style="font-size:10px;color:var(--green);margin-top:2px;line-height:1.3;opacity:.85">${short}</div>`;
      }
      return`<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:6px 0;border-bottom:1px solid var(--border)"><div style="flex:1;min-width:0"><span class="util-name" style="display:block">${u.utility_name}</span>${detail}</div><span class="util-badge ${badgeClass[u.status]||'ub-pending'}" style="flex-shrink:0;margin-top:2px">${label[u.status]||u.status}</span></div>`;
    }).join('');
  }catch(e){el.innerHTML='<div style="color:var(--muted);font-size:12px">Erro ao carregar utilities</div>';}
}

function renderTable(){
  const sr=(document.getElementById('tbl-srch').value||'').toLowerCase();const st=document.getElementById('tbl-stat').value;const pr=document.getElementById('tbl-proj').value;const cl=document.getElementById('tbl-cli').value;const ut=document.getElementById('tbl-util')?.value||'';
  let f=tickets.filter(t=>{if(isSuperseded(t))return false;if(st&&t.status!==st)return false;if(pr&&t.projectId!==pr)return false;if(cl&&t.client!==cl)return false;if(sr&&!t.ticket.toLowerCase().includes(sr)&&!(t.client||'').toLowerCase().includes(sr)&&!(t.location||'').toLowerCase().includes(sr))return false;
    if(ut){
      const tkey=String(t.ticket).trim();
      const pends=getTicketPendingUtils(tkey);
      const allU=getTicketUtils(tkey);
      if(ut==='__any_pending__'){if(!pends.length)return false;}
      else if(ut==='__all_clear__'){if(pends.length>0)return false;if(!allU.length)return false;}
      else{if(!pends.some(p=>p.utility_name===ut))return false;}
    }
    return true;});
  f.sort((a,b)=>{if(sortCol==='footage')return sortAsc?(a.footage||0)-(b.footage||0):(b.footage||0)-(a.footage||0);return sortAsc?String(a[sortCol]||'').localeCompare(String(b[sortCol]||'')):String(b[sortCol]||'').localeCompare(String(a[sortCol]||''));});
  document.getElementById('tbl-count').textContent=`${f.length} tickets · ${f.reduce((s,t)=>s+(t.footage||0),0).toLocaleString()} ft`;
  document.getElementById('tbl-body').innerHTML=f.map(t=>{const pends=getTicketPendingUtils(String(t.ticket).trim());const pendNames=pends.map(p=>p.utility_name);const pendChips=pendNames.length?`<div style="display:flex;flex-wrap:wrap;gap:2px;margin-top:3px">${pendNames.slice(0,3).map(n=>`<span style="font-size:9px;padding:1px 5px;border-radius:10px;background:var(--red-bg);color:var(--red);font-family:var(--mono);white-space:nowrap">${n.length>20?n.substring(0,20)+'…':n}</span>`).join('')}${pendNames.length>3?`<span style="font-size:9px;color:var(--muted)">+${pendNames.length-3}</span>`:''}</div>`:'';return`<tr onclick="openTicketDetail(${t.id})"><td style="font-family:var(--mono);font-weight:500">${t.ticket}</td><td style="color:var(--text2);font-size:12px">${t.client}</td><td style="color:var(--muted);font-size:12px">${t.prime||'—'}</td><td>${t.location}, ${t.state}</td><td class="tc-${(t.status||'').toLowerCase()}">${t.status}${pendChips}</td><td style="font-family:var(--mono)">${t.footage} ft</td><td style="font-family:var(--mono);font-size:12px">${t.expire||'—'}</td><td style="color:var(--muted)">${t.tipo||'—'}</td><td onclick="event.stopPropagation()"><div style="display:flex;gap:5px"><button class="btn btn-sm" onclick="openTicketDetail(${t.id})">Ver</button>${isAdmin?`<button class="btn btn-sm" onclick="editFromTbl(${t.id})">Editar</button>`:''}</div></td></tr>`;}).join('');
}
function sortBy(col){sortAsc=sortCol===col?!sortAsc:true;sortCol=col;renderTable();}
function editFromTbl(id){currentDetailId=id;editCurrentTicket();}

function renderDash(){
  const states=[...new Set(tickets.map(t=>t.state).filter(Boolean))].sort();
  const dsf=dashStateVal;
  const dashStateFilter=`<select class="fi" id="dash-state-filter" onchange="dashStateVal=this.value;renderDash()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px"><option value="">Todos estados</option>${states.map(s=>`<option value="${s}"${dsf===s?' selected':''}>${s}</option>`).join('')}</select>`;
  const fTickets=dsf?tickets.filter(t=>t.state===dsf&&!isSuperseded(t)):tickets.filter(t=>!isSuperseded(t));
  const allFTickets=dsf?tickets.filter(t=>t.state===dsf):tickets;
  const total=fTickets.length,open=fTickets.filter(t=>t.status==='Open').length,clear=fTickets.filter(t=>t.status==='Clear').length,damage=fTickets.filter(t=>t.status==='Damage').length,closed=fTickets.filter(t=>t.status==='Closed').length;
  const totalFt=fTickets.reduce((s,t)=>s+(t.footage||0),0);
  const openFt=fTickets.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
  const clearFt=fTickets.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
  const damageFt=fTickets.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
  const noMap=fTickets.filter(t=>(!t.fieldPath||t.fieldPath.length<2)&&t.status!=='Cancel'&&t.status!=='Closed');
  const soon=fTickets.filter(t=>{if(!t.expire||t.expire==='—')return false;const d=new Date(t.expire);const diff=(d-Date.now())/86400000;return diff>=0&&diff<=10&&t.status!=='Closed'&&t.status!=='Cancel';});
  const fProjects=dsf?projects.filter(p=>p.state===dsf):projects;
  const projStats=fProjects.filter(p=>p.status!=='Completed').map(p=>{const ts=fTickets.filter(t=>t.projectId===p.id);const clearFtP=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);const openFtP=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);const concluidoFt=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);const damageFtP=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);const ticketFt=ts.reduce((s,t)=>s+(t.footage||0),0);const totalFt=p.totalFeet||ticketFt||1;const locs=[...new Set(ts.map(t=>t.location).filter(Boolean).map(l=>l.replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()))].join(', ')||'';return{name:p.name,id:p.id,count:ts.length,clearFtP,openFtP,concluidoFt,damageFt:damageFtP,ticketFt,totalFt,pctClear:totalFt>0?Math.round(clearFtP/totalFt*100):0,pctOpen:totalFt>0?Math.round(openFtP/totalFt*100):0,pctConcluido:totalFt>0?Math.round(concluidoFt/totalFt*100):0,pctDamage:totalFt>0?Math.round(damageFtP/totalFt*100):0,hasTotalFromSheet:!!p.totalFeet,locs,state:p.state||''};}).sort((a,b)=>b.count-a.count);
  const el=document.getElementById('dash-content');if(!el)return;
  el.innerHTML=`<div class="page-title">Dashboard <span style="font-size:13px;font-weight:400;color:var(--muted);font-family:var(--mono)">${new Date().toLocaleDateString('pt-BR')}</span><span style="margin-left:auto">${dashStateFilter}</span></div><div class="stat-grid"><div class="stat-card"><div class="stat-label">Total tickets</div><div class="stat-val">${total}</div><div class="stat-sub">${totalFt.toLocaleString()} ft</div></div><div class="stat-card" style="border-left:3px solid var(--red)"><div class="stat-label">Open</div><div class="stat-val" style="color:var(--red)">${open}</div><div class="stat-sub" style="color:var(--red)">${openFt.toLocaleString()} ft</div></div><div class="stat-card" style="border-left:3px solid var(--green)"><div class="stat-label">Clear</div><div class="stat-val" style="color:var(--green)">${clear}</div><div class="stat-sub" style="color:var(--green)">${clearFt.toLocaleString()} ft</div></div><div class="stat-card" style="border-left:3px solid var(--amber)"><div class="stat-label">Damage</div><div class="stat-val" style="color:var(--amber)">${damage}</div><div class="stat-sub" style="color:var(--amber)">${damageFt.toLocaleString()} ft</div></div><div class="stat-card" style="border-left:3px solid var(--purple)"><div class="stat-label">✏️ Sem trajeto</div><div class="stat-val" style="color:var(--purple)">${noMap.length}</div><div class="stat-sub" style="color:var(--purple)">de ${total}</div></div></div>${soon.length?`<div class="warn-banner"><div class="warn-title">⚠ ${soon.length} ticket(s) vencendo nos próximos 10 dias</div><div class="warn-chips">${soon.map(t=>`<span class="warn-chip" onclick="openTicketDetail(${t.id})">${t.ticket} · ${t.expire}</span>`).join('')}</div></div>`:''}${noMap.length&&isAdmin?`<div style="background:var(--purple-bg);border:1px solid var(--purple-border);border-radius:var(--r-lg);padding:12px 16px;margin-bottom:16px"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:7px"><div style="font-size:13px;font-weight:600;color:var(--purple)">✏️ ${noMap.length} ticket(s) sem trajeto</div><button onclick="nav('map')" class="btn btn-sm" style="background:var(--purple);color:white;border-color:var(--purple)">Ir para o mapa</button></div><div style="display:flex;flex-wrap:wrap;gap:5px">${noMap.slice(0,20).map(t=>`<span style="font-size:11px;font-family:var(--mono);padding:2px 9px;border-radius:20px;background:rgba(109,40,217,.1);color:var(--purple);cursor:pointer;border:1px solid var(--purple-border)" onclick="goDrawField(${t.id})">${t.ticket}</span>`).join('')}${noMap.length>20?`<span style="font-size:11px;color:var(--muted)">+${noMap.length-20} mais</span>`:''}</div></div>`:''}
  ${renderClearedStats(allFTickets)}${renderProjectProgress(projStats)}${renderClearTimeMetrics(allFTickets)}${renderUtilSummaryHtml()}
  `;
}

function renderProjects(){
  const g=document.getElementById('proj-grid');if(!g)return;
  // Sync state filter options
  const stateFilter=document.getElementById('proj-state-filter');
  if(stateFilter){
    const states=[...new Set(projects.map(p=>p.state).filter(Boolean))].sort();
    const prev=stateFilter.value;
    stateFilter.innerHTML='<option value="">Todos estados</option>'+states.map(s=>`<option value="${s}">${s}</option>`).join('');
    if(prev)stateFilter.value=prev;
  }
  const sf=stateFilter?.value||'';
  const filteredProjects=sf?projects.filter(p=>p.state===sf):projects;
  if(!filteredProjects.length){g.innerHTML='<div style="color:var(--muted);font-size:13px">Nenhum projeto.</div>';return}
  const active=filteredProjects.filter(p=>p.status!=='Completed');
  const completed=filteredProjects.filter(p=>p.status==='Completed');
  const renderCard=(p)=>{
    const ts=tickets.filter(t=>t.projectId===p.id&&!isSuperseded(t));
    const open=ts.filter(t=>t.status==='Open').length,clear=ts.filter(t=>t.status==='Clear').length,damage=ts.filter(t=>t.status==='Damage').length,closed=ts.filter(t=>t.status==='Closed').length;
    const ticketFt=ts.reduce((s,t)=>s+(t.footage||0),0);const projTotal=p.totalFeet||ticketFt||1;
    const clearFtP=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
    const openFtP=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
    const concluidoFt=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);
    const damageFt=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
    const pctConcluido=projTotal>0?Math.round(concluidoFt/projTotal*100):0;
    const pctClear=projTotal>0?Math.round(clearFtP/projTotal*100):0;
    const pctOpen=projTotal>0?Math.round(openFtP/projTotal*100):0;
    const pctDamage=projTotal>0?Math.round(damageFt/projTotal*100):0;
    const locations=[...new Set(ts.map(t=>t.location).filter(Boolean))].map(l=>{
      // Limpa textos do 811 — pega só a parte antes de "Inside:" ou "Near:"
      const clean=l.replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim();
      return clean||l;
    });
    const locStr=[...new Set(locations)].join(', ')||p.state;
    return `<div class="pcard"><div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:3px"><div style="flex:1"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><div class="pcard-name">📍 ${locStr}</div><div style="font-size:12px;color:var(--muted);font-family:var(--mono)">${p.name}</div></div></div><span class="status-pill pill-${p.status==='Active'?'active':'done'}" style="flex-shrink:0;margin-left:8px">${p.status}</span></div><div class="pcard-meta">${p.client} · ${p.state}</div><div class="prog-bar"><div style="width:${pctClear}%;background:var(--green)"></div><div style="width:${Math.min(pctOpen,100-pctClear)}%;background:var(--red)"></div><div style="width:${Math.min(pctDamage,100-pctClear-pctOpen)}%;background:#f59e0b"></div><div style="width:${Math.min(pctConcluido,100-pctClear-pctOpen-pctDamage)}%;background:var(--text)"></div></div><div class="pcard-stats"><div class="pstat"><span class="pstat-val" style="color:var(--red)">${open}</span><span class="pstat-lbl">Open</span></div><div class="pstat"><span class="pstat-val" style="color:var(--green)">${clear}</span><span class="pstat-lbl">Clear</span></div><div class="pstat"><span class="pstat-val" style="color:var(--amber)">${damage}</span><span class="pstat-lbl">Damage</span></div><div class="pstat"><span class="pstat-val" style="color:var(--muted)">${closed}</span><span class="pstat-lbl">Closed</span></div><div class="pstat"><span class="pstat-val">${ts.length}</span><span class="pstat-lbl">Total</span></div></div><div style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-bottom:10px">${ticketFt.toLocaleString()} ft${p.totalFeet?' / '+p.totalFeet.toLocaleString()+' ft total':''}</div><div style="display:flex;gap:6px;flex-wrap:wrap"><button class="btn btn-sm" onclick="shareProject('${p.id}')" style="background:var(--accent);color:white;border-color:var(--accent)">📤 Compartilhar</button><button class="btn btn-sm" onclick="openProjectMap('${p.id}')">Ver no mapa</button>${isAdmin?`<button class="btn btn-sm" onclick="editProject('${p.id}')">Editar</button><button class="btn btn-sm btn-danger" onclick="openDelProj('${p.id}')">Excluir</button>`:''}</div></div>`;
  };
  g.innerHTML=(active.length?active.map(renderCard).join(''):'')+(completed.length?`<div style="grid-column:1/-1;margin-top:24px;padding-top:20px;border-top:2px solid var(--border)"><div style="font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;margin-bottom:14px">✅ Projetos Concluídos (${completed.length})</div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px">${completed.map(renderCard).join('')}</div></div>`:'');
}

function openProjectMap(pid){nav('map');setTimeout(()=>{document.getElementById('proj-filter').value=pid;onProjFilter();},200);}

function openNewProject(){editingProjectId=null;document.getElementById('proj-modal-title').textContent='Novo projeto';['pm-loc','pm-num','pm-client','pm-state','pm-coords'].forEach(id=>document.getElementById(id).value='');document.getElementById('pm-status').value='Active';document.getElementById('pm-feet').value='';openModal('ov-proj');}
function editProject(pid){editingProjectId=pid;const p=projects.find(x=>x.id===pid);if(!p)return;document.getElementById('proj-modal-title').textContent='Editar projeto';const parts=p.name.split(' — ');if(parts.length>=2){document.getElementById('pm-loc').value=parts[0].trim();document.getElementById('pm-num').value=parts.slice(1).join(' — ').trim();}else{document.getElementById('pm-loc').value='';document.getElementById('pm-num').value=p.name;}document.getElementById('pm-client').value=p.client;document.getElementById('pm-state').value=p.state;document.getElementById('pm-status').value=p.status;document.getElementById('pm-feet').value=p.totalFeet||'';document.getElementById('pm-coords').value=p.centerCoords?p.centerCoords.join(', '):'';openModal('ov-proj');}
async function saveProject(){const loc=document.getElementById('pm-loc').value.trim();const num=document.getElementById('pm-num').value.trim();if(!loc&&!num){toast('Preencha localidade ou número.','danger');return}const name=loc&&num?`${loc} — ${num}`:loc||num;const coordStr=document.getElementById('pm-coords').value.trim();let centerCoords=null;if(coordStr){const m=coordStr.match(/([-\d.]+)\s*,\s*([-\d.]+)/);if(m)centerCoords=[parseFloat(m[1]),parseFloat(m[2])];}const data={name,client:document.getElementById('pm-client').value,state:document.getElementById('pm-state').value,status:document.getElementById('pm-status').value,desc:'',totalFeet:parseFloat(document.getElementById('pm-feet').value)||0,centerCoords,_manual:true};if(editingProjectId){const p=projects.find(x=>x.id===editingProjectId);if(p)Object.assign(p,data);await saveProjectToDb(projects.find(x=>x.id===editingProjectId));}else{const id='p'+Date.now();const p={...data,id};projects.push(p);await saveProjectToDb(p);}closeModal('ov-proj');syncAll();toast('Projeto salvo!','success');}

function openDelProj(pid){deletingProjectId=pid;const p=projects.find(x=>x.id===pid);if(!p)return;const ts=tickets.filter(t=>t.projectId===pid);document.getElementById('del-proj-info').innerHTML=`Projeto: <strong>${p.name}</strong><br>Este projeto tem <strong>${ts.length} ticket(s)</strong> vinculado(s).`;openModal('ov-del-proj');}
async function confirmDelProj(){if(!deletingProjectId)return;const ok=await deleteProjectFromDb(deletingProjectId);if(ok){projects=projects.filter(p=>p.id!==deletingProjectId);tickets.forEach(t=>{if(t.projectId===deletingProjectId)t.projectId='';});deletingProjectId=null;closeModal('ov-del-proj');syncAll();toast('Projeto excluído!','success');}else{toast('Erro ao excluir projeto','danger');}}

function openMoveProj(tid){const t=tickets.find(x=>x.id===tid);if(!t)return;document.getElementById('move-proj-ticket-info').textContent=`Ticket: ${t.ticket}`;const sel=document.getElementById('move-proj-sel');sel.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}"${t.projectId===p.id?' selected':''}>${p.name}</option>`).join('');openModal('ov-move-proj');}
async function saveMoveProj(){const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;t.projectId=document.getElementById('move-proj-sel').value;t.history.push({ts:Date.now(),action:`Movido para projeto: ${projects.find(p=>p.id===t.projectId)?.name||'Sem projeto'}`,color:'#1a6cf0'});await saveTicketToDb(t);closeModal('ov-move-proj');openTicketDetail(currentDetailId);syncAll();toast('Projeto atualizado!','success');}

function openNewTicket(){editingTicketId=null;document.getElementById('ticket-modal-title').textContent='Novo ticket';['tm-t','tm-c','tm-co','tm-l','tm-st','tm-f','tm-notes','tm-tipo','tm-job','tm-prime','tm-addr'].forEach(id=>document.getElementById(id).value='');document.getElementById('tm-s').value='Open';document.getElementById('tm-e').value='';document.getElementById('tm-proj').value='';openModal('ov-ticket');}
function editCurrentTicket(){closeModal('ov-detail');const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;editingTicketId=t.id;document.getElementById('ticket-modal-title').textContent='Editar ticket';document.getElementById('tm-t').value=t.ticket;document.getElementById('tm-s').value=t.status;document.getElementById('tm-proj').value=t.projectId||'';document.getElementById('tm-c').value=t.client;document.getElementById('tm-co').value=t.company;document.getElementById('tm-l').value=t.location;document.getElementById('tm-st').value=t.state;document.getElementById('tm-f').value=t.footage;document.getElementById('tm-e').value=t.expire;document.getElementById('tm-notes').value=t.notes||'';document.getElementById('tm-tipo').value=t.tipo||'';document.getElementById('tm-job').value=t.job||'';document.getElementById('tm-prime').value=t.prime||'';document.getElementById('tm-addr').value=t.address||'';openModal('ov-ticket');}
async function saveTicket(){const tnum=document.getElementById('tm-t').value.trim();if(!tnum){toast('Preencha o número.','danger');return}const newStatus=document.getElementById('tm-s').value;let savedId=null;if(editingTicketId){const t=tickets.find(x=>x.id===editingTicketId);if(t){const old=t.status;Object.assign(t,{ticket:tnum,projectId:document.getElementById('tm-proj').value,client:document.getElementById('tm-c').value,company:document.getElementById('tm-co').value,location:document.getElementById('tm-l').value,state:document.getElementById('tm-st').value,footage:parseInt(document.getElementById('tm-f').value)||0,expire:document.getElementById('tm-e').value||'',notes:document.getElementById('tm-notes').value,status:newStatus,tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,prime:document.getElementById('tm-prime').value,address:document.getElementById('tm-addr').value});if(old!==newStatus)t.history.push({ts:Date.now(),action:`Status: ${old} → ${newStatus}`,color:scol(newStatus)});t.history.push({ts:Date.now(),action:'Editado',color:'#9a9888'});await saveTicketToDb(t);savedId=t.id;}toast('Ticket atualizado!','success');}else{const t={id:null,ticket:tnum,projectId:document.getElementById('tm-proj').value,company:document.getElementById('tm-co').value||'One Drill',state:document.getElementById('tm-st').value||'FL',location:document.getElementById('tm-l').value||'',status:newStatus,expire:document.getElementById('tm-e').value||'',footage:parseInt(document.getElementById('tm-f').value)||0,client:document.getElementById('tm-c').value||'—',prime:document.getElementById('tm-prime').value,tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,address:document.getElementById('tm-addr').value,notes:document.getElementById('tm-notes').value,fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Ticket criado',color:'#1a6cf0'}],attachments:[],pending:'',oldTicket2:'',statusOld:'',expireOld:'',status_locked:false};tickets.push(t);await saveTicketToDb(t);savedId=t.id;toast('Ticket criado!','success');}closeModal('ov-ticket');syncAll();if(savedId)setTimeout(()=>openTicketDetail(savedId),200);}

function openImport(){parsed=[];parsedProjectTotals={};parsedProjectCoords={};document.getElementById('prevarea').style.display='none';document.getElementById('bimport').style.display='none';document.getElementById('progwrap').style.display='none';document.getElementById('progfill').style.width='0%';document.getElementById('ffile').value='';openModal('ov-import');}
function onDrop(e){e.preventDefault();document.getElementById('uzone').classList.remove('drag');if(e.dataTransfer.files[0])readFile(e.dataTransfer.files[0]);}
function onFileIn(e){if(e.target.files[0])readFile(e.target.files[0]);}
function nk(k){return String(k||'').toLowerCase().replace(/[^a-z0-9]/g,'');}

function readFile(file){
  const reader=new FileReader();
  reader.onload=e=>{
    try{
      const wb=XLSX.read(e.target.result,{type:'binary',cellDates:true});
      if(wb.SheetNames.length>1){const wsP=wb.Sheets[wb.SheetNames[1]];const projRows=XLSX.utils.sheet_to_json(wsP,{header:1,defval:''});for(let i=0;i<projRows.length;i++){if(projRows[i].some(c=>String(c||'').toLowerCase().includes('project'))){const hdr=projRows[i].map(h=>String(h||'').toLowerCase().replace(/[^a-z0-9]/g,''));for(let j=i+1;j<projRows.length;j++){const r=projRows[j];if(!r.some(c=>c!==null&&c!==''&&c!==undefined))continue;const pidIdx=hdr.findIndex(h=>h.includes('project'));const ftIdx=hdr.findIndex(h=>h.includes('feet')||h.includes('total'));const coordIdx=hdr.findIndex(h=>h.includes('coord')||h.includes('lat'));const pid=String(r[pidIdx]||'').trim();const ft=parseFloat(r[ftIdx])||0;if(pid){parsedProjectTotals[pid]=ft;if(coordIdx>=0){const coordStr=String(r[coordIdx]||'').trim();const m=coordStr.match(/([-\d.]+)\s*,\s*([-\d.]+)/);if(m)parsedProjectCoords[pid]=[parseFloat(m[1]),parseFloat(m[2])];}}};break;}}}
      const ws=wb.Sheets[wb.SheetNames[0]];const allRows=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});if(!allRows.length){toast('Arquivo vazio.','danger');return;}
      let headerRowIdx=0;for(let i=0;i<Math.min(5,allRows.length);i++){if(allRows[i].some(c=>String(c||'').toLowerCase().replace(/\s/g,'').includes('ticket'))){headerRowIdx=i;break;}}
      const headers=allRows[headerRowIdx].map(h=>nk(h));
      const dataRows=allRows.slice(headerRowIdx+1).filter(r=>r.some(c=>c!==null&&c!==''&&c!==undefined));
      const ci=(...names)=>{for(const n of names){const i=headers.findIndex(h=>h&&h===n);if(i>=0)return i;}for(const n of names){const i=headers.findIndex(h=>h&&h.startsWith(n)&&h.length<=n.length+3);if(i>=0)return i;}return -1;};
      const idx={ticket:ci('ticket'),company:ci('company'),state:ci('state'),location:ci('location'),status:ci('status'),expire:ci('expireon'),footage:ci('footage'),client:ci('client'),prime:ci('prime'),job:ci('jobnumber'),tipo:ci('tipo'),address:ci('mainaddress'),project:ci('project'),pending:ci('pending'),oldTicket2:ci('oldticket'),statusOld:ci('statusold'),expireOld:ci('oldexpirationdate')};
      const getCell=(row,i)=>{if(i<0||i>=row.length)return'';const v=row[i];if(v===null||v===undefined)return'';if(v instanceof Date)return v.toLocaleDateString('en-US');return String(v).replace(/\xa0/g,'').trim();};
      parsed=dataRows.map(row=>{const ticket=getCell(row,idx.ticket);if(!ticket)return null;let rawStatus=getCell(row,idx.status);if(rawStatus.includes('✅')||rawStatus.includes('⚠'))rawStatus='Open';let status='Open';const sl=rawStatus.toLowerCase();if(sl==='clear')status='Clear';else if(sl==='open')status='Open';else if(sl==='closed'||sl==='close')status='Closed';else if(sl==='damage')status='Damage';else if(sl==='cancel')status='Cancel';else if(rawStatus)status=rawStatus;const rawExpOld=getCell(row,idx.expireOld);let expireOld='';if(rawExpOld){try{const d=new Date(rawExpOld);expireOld=isNaN(d.getTime())?rawExpOld:d.toLocaleDateString('en-US');}catch{expireOld=rawExpOld;}}return{ticket,company:getCell(row,idx.company)||'One Drill',state:getCell(row,idx.state),location:getCell(row,idx.location),status,expire:getCell(row,idx.expire),footage:parseFloat(getCell(row,idx.footage))||0,client:getCell(row,idx.client),prime:getCell(row,idx.prime),job:getCell(row,idx.job),tipo:getCell(row,idx.tipo),address:getCell(row,idx.address),projectName:getCell(row,idx.project),pending:getCell(row,idx.pending),oldTicket2:getCell(row,idx.oldTicket2),statusOld:getCell(row,idx.statusOld),expireOld};}).filter(Boolean);
      if(!parsed.length){toast('Nenhuma linha válida.','danger');return;}
      const cols=['ticket','client','prime','status','footage','tipo'];
      document.getElementById('prevlabel').textContent=`${parsed.length} ticket(s) detectados`;
      document.getElementById('ptbl').innerHTML='<thead><tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr></thead><tbody>'+parsed.slice(0,10).map(r=>'<tr>'+cols.map(c=>`<td>${r[c]||'—'}</td>`).join('')+'</tr>').join('')+(parsed.length>10?`<tr><td colspan="${cols.length}" style="color:var(--muted);text-align:center">... +${parsed.length-10} linhas</td></tr>`:'')+  '</tbody>';
      document.getElementById('prevarea').style.display='block';document.getElementById('bimport').style.display='';
    }catch(err){toast('Erro: '+err.message,'danger');console.error(err);}
  };reader.readAsBinaryString(file);
}

async function doImport(){
  if(!parsed.length)return;
  if(!await requireAuth())return;
  const mode=document.querySelector('input[name="importmode"]:checked')?.value||'replace';
  const pw=document.getElementById('progwrap'),pf=document.getElementById('progfill'),pt=document.getElementById('progtxt');
  pw.style.display='block';document.getElementById('bimport').disabled=true;setSyncStatus(true,'Importando...');
  for(const[projId,ft]of Object.entries(parsedProjectTotals)){const pc=parsedProjectCoords[projId]||null;let p=projects.find(x=>x.name===projId||x.id===projId);if(!p){p={id:'p'+projId,name:projId,client:'',state:'',status:'Active',desc:'',totalFeet:ft,centerCoords:pc,_manual:false};projects.push(p);}else{if(!p.totalFeet)p.totalFeet=ft;if(!p.centerCoords)p.centerCoords=pc;}await saveProjectToDb(p);}
  if(mode==='replace'){pt.textContent='Limpando tickets antigos...';await sb.from('tickets').delete().neq('id',0);tickets=[];projects=projects.filter(p=>p._manual);}
  const novo=[];let updated=0;
  for(let i=0;i<parsed.length;i++){
    const r=parsed[i];pf.style.width=Math.round(((i+1)/parsed.length)*100)+'%';pt.textContent=`${i+1}/${parsed.length}: ${r.ticket}...`;
    let pid='';if(r.projectName){let p=projects.find(x=>x.name.toLowerCase()===r.projectName.toLowerCase()||x.id==='p'+r.projectName);if(!p){const projId=String(r.projectName).trim();const tf=parsedProjectTotals[projId]||0;const pc=parsedProjectCoords[projId]||null;p={id:'p'+projId,name:projId,client:r.client,state:r.state,status:'Active',desc:'',_manual:false,totalFeet:tf,centerCoords:pc};projects.push(p);await saveProjectToDb(p);}else{const projId=String(r.projectName).trim();if(!p.totalFeet)p.totalFeet=parsedProjectTotals[projId]||0;if(!p.centerCoords)p.centerCoords=parsedProjectCoords[projId]||null;}pid=p.id;}
    if(mode==='update'){const existing=tickets.find(t=>String(t.ticket).trim()===String(r.ticket).trim());if(existing){const oldStatus=existing.status;Object.assign(existing,{company:r.company,state:r.state,location:r.location,status:r.status,expire:r.expire,footage:r.footage,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,projectId:pid||existing.projectId});if(oldStatus!==r.status)existing.history.push({ts:Date.now(),action:`Status: ${oldStatus} → ${r.status}`,color:scol(r.status)});existing.history.push({ts:Date.now(),action:'Atualizado via Excel ✅',color:'#16a34a'});await saveTicketToDb(existing);updated++;continue;}}
    const t={id:null,ticket:r.ticket,projectId:pid,company:r.company||'One Drill',state:r.state,location:r.location,status:r.status,expire:r.expire,footage:r.footage,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,notes:'',fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Importado via Excel',color:'#1a6cf0'}],attachments:[],status_locked:false};
    tickets.push(t);await saveTicketToDb(t);novo.push(t);
  }
  document.getElementById('bimport').disabled=false;closeModal('ov-import');syncAll();setSyncStatus(true,'Sincronizado ✓');
  if(mode==='update')toast(`✅ ${updated} atualizados · ${novo.length} novos`,'success');else toast(`${novo.length} tickets importados`,'success');
}

function exportFiltered(){
  const sr=(document.getElementById('tbl-srch')?.value||'').toLowerCase();
  const st=document.getElementById('tbl-stat')?.value||'';
  const pr=document.getElementById('tbl-proj')?.value||'';
  const cl=document.getElementById('tbl-cli')?.value||'';
  const ut=document.getElementById('tbl-util')?.value||'';
  let f=tickets.filter(t=>{
    if(isSuperseded(t))return false;
    if(st&&t.status!==st)return false;
    if(pr&&t.projectId!==pr)return false;
    if(cl&&t.client!==cl)return false;
    if(sr&&!t.ticket.toLowerCase().includes(sr)&&!(t.client||'').toLowerCase().includes(sr)&&!(t.location||'').toLowerCase().includes(sr))return false;
    if(ut){const tkey=String(t.ticket).trim();const pends=getTicketPendingUtils(tkey);const allU=getTicketUtils(tkey);if(ut==='__any_pending__'){if(!pends.length)return false;}else if(ut==='__all_clear__'){if(pends.length>0)return false;if(!allU.length)return false;}else{if(!pends.some(p=>p.utility_name===ut))return false;}}
    return true;
  });
  if(!f.length){toast('Nenhum ticket para exportar com esses filtros.','warn');return;}
  const totalFt=f.reduce((s,t)=>s+(t.footage||0),0);
  const wb=XLSX.utils.book_new();
  const tData=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Pending','Empresa'],...f.map(t=>[t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,t.pending,t.company]),['','','','','','','TOTAL:',totalFt,'','','','','','']];
  const ws=XLSX.utils.aoa_to_sheet(tData);XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  XLSX.writeFile(wb,'OneDrill_Filtrado_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Excel filtrado: '+f.length+' tickets · '+totalFt.toLocaleString()+' ft','success');
}
function exportExcel(){
  const wb=XLSX.utils.book_new();
  const tData=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Pending','Empresa'],...tickets.map(t=>[t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,t.pending,t.company])];
  const ws=XLSX.utils.aoa_to_sheet(tData);XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  const pData=[['Nome','Cliente','Estado','Status','Total Feet','Tickets'],...projects.map(p=>[p.name,p.client,p.state,p.status,p.totalFeet,tickets.filter(t=>t.projectId===p.id).length])];
  const wp=XLSX.utils.aoa_to_sheet(pData);XLSX.utils.book_append_sheet(wb,wp,'Projetos');
  const fname=`OneDrill_${new Date().toISOString().slice(0,10)}.xlsx`;
  XLSX.writeFile(wb,fname);toast(`Excel exportado: ${fname}`,'success');
}

function syncProjectSelects(){
  const cleanLoc=(l)=>(l||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()||l;
  const projLabel=(p)=>{const ts=tickets.filter(t=>t.projectId===p.id);const locs=[...new Set(ts.map(t=>cleanLoc(t.location)).filter(Boolean))].join(', ');const loc=locs||p.state||'';return loc?`${loc} — ${p.name}`:p.name;};
  const mkOpts=(label)=>'<option value="">'+label+'</option>'+projects.map(p=>`<option value="${p.id}">${projLabel(p)}</option>`).join('');
  const pf=document.getElementById('proj-filter');if(pf)pf.innerHTML=mkOpts('Todos os projetos');
  const tp=document.getElementById('tbl-proj');if(tp)tp.innerHTML=mkOpts('Todos projetos');
  const tm=document.getElementById('tm-proj');if(tm)tm.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}">${projLabel(p)}</option>`).join('');
}
function syncClients(){const cls=[...new Set(tickets.map(t=>t.client).filter(Boolean))].sort();['fcli','tbl-cli'].forEach(id=>{const el=document.getElementById(id);if(el)el.innerHTML='<option value="">Todos clientes</option>'+cls.map(c=>`<option>${c}</option>`).join('');});}
function syncMapUtilFilter(){if(!utilCacheLoaded)return;const el=document.getElementById('map-util-filter');if(!el)return;const prev=el.value;const allU={};const openNums=new Set(tickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel').map(t=>String(t.ticket).trim()));for(const[tn,resps]of Object.entries(utilCache)){if(!openNums.has(tn))continue;for(const u of resps){if(u.status==='Pending'){if(!allU[u.utility_name])allU[u.utility_name]=0;allU[u.utility_name]++;}}}const sorted=Object.entries(allU).sort((a,b)=>b[1]-a[1]);el.innerHTML='<option value="">Todas utilities</option><option value="__pending__">Com pendentes</option>'+sorted.map(([n,c])=>'<option value="'+n+'">'+n+' ('+c+')</option>').join('');if(prev)el.value=prev;}
function syncLocations(){const locs=[...new Set(tickets.map(t=>t.location).filter(Boolean))].sort();const el=document.getElementById('floc');if(el)el.innerHTML='<option value="">Todos locais</option>'+locs.map(l=>`<option>${l}</option>`).join('');}
function syncAll(){syncProjectSelects();syncClients();syncLocations();if(utilCacheLoaded){syncUtilFilter();syncMapUtilFilter();}renderList();if(map)renderMap();renderProjects();renderTable();renderDash();}


function renderProjectProgress(projStats){
  if(!projStats.length)return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">Progresso por projeto — Footage</div><div style="color:var(--muted);font-size:13px">Sem projetos</div></div></div>';

  // Totais gerais
  var tTotal=0,tClear=0,tOpen=0,tDamage=0,tConcluido=0,tTickets=0;
  for(var i=0;i<projStats.length;i++){
    var p=projStats[i];
    tTotal+=p.totalFt;tClear+=p.clearFtP;tOpen+=p.openFtP;tDamage+=p.damageFt;tConcluido+=p.concluidoFt;tTickets+=p.count;
  }
  var pctClear=tTotal>0?Math.round(tClear/tTotal*100):0;
  var pctOpen=tTotal>0?Math.round(tOpen/tTotal*100):0;
  var pctDamage=tTotal>0?Math.round(tDamage/tTotal*100):0;
  var pctConcluido=tTotal>0?Math.round(tConcluido/tTotal*100):0;

  // Dropdown de projetos
  var opts='<option value="">Todos os projetos ('+tTickets+' tickets · '+tTotal.toLocaleString()+' ft)</option>';
  for(var i=0;i<projStats.length;i++){
    var p=projStats[i];
    var label=(p.locs||p.state)+' ('+p.name+')';
    opts+='<option value="'+p.id+'"'+(progProjVal===p.id?' selected':'')+'>'+label+' — '+p.count+' tickets</option>';
  }

  // Quais projetos mostrar
  var show=progProjVal?projStats.filter(function(p){return p.id===progProjVal;}):projStats;

  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px">';
  h+='<div class="dash-card-title" style="margin-bottom:0">Progresso por projeto — Footage</div>';
  h+='<select class="fi" onchange="progProjVal=this.value;renderDash()" style="width:auto;min-width:250px;font-size:12px;padding:5px 8px">'+opts+'</select>';
  h+='</div>';

  // Totais (so mostra quando "todos")
  if(!progProjVal){
    h+='<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px">';
    h+='<div style="padding:12px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:18px;font-weight:700;font-family:var(--mono);color:var(--text)">'+tTotal.toLocaleString()+'</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-top:2px">Total ft</div></div>';
    h+='<div style="padding:12px;background:var(--green-bg);border-radius:var(--r);border:1px solid var(--green-border);text-align:center"><div style="font-size:18px;font-weight:700;font-family:var(--mono);color:var(--green)">'+tClear.toLocaleString()+'</div><div style="font-size:9px;color:var(--green);text-transform:uppercase;margin-top:2px">Clear '+pctClear+'%</div></div>';
    h+='<div style="padding:12px;background:var(--red-bg);border-radius:var(--r);border:1px solid var(--red-border);text-align:center"><div style="font-size:18px;font-weight:700;font-family:var(--mono);color:var(--red)">'+tOpen.toLocaleString()+'</div><div style="font-size:9px;color:var(--red);text-transform:uppercase;margin-top:2px">Em aberto '+pctOpen+'%</div></div>';
    h+='<div style="padding:12px;background:var(--amber-bg);border-radius:var(--r);border:1px solid var(--amber-border);text-align:center"><div style="font-size:18px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+tDamage.toLocaleString()+'</div><div style="font-size:9px;color:var(--amber);text-transform:uppercase;margin-top:2px">Damage '+pctDamage+'%</div></div>';
    h+='<div style="padding:12px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:18px;font-weight:700;font-family:var(--mono);color:var(--text)">'+tConcluido.toLocaleString()+'</div><div style="font-size:9px;color:var(--text2);text-transform:uppercase;margin-top:2px">Concluído '+pctConcluido+'%</div></div>';
    h+='</div>';

    // Ranking por projeto
    if(projStats.length>1){
      h+='<div style="margin-top:16px"><div style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:10px">Ranking por projeto</div>';
      h+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px">';
      for(var i=0;i<projStats.length;i++){
        var rp=projStats[i];
        h+='<div style="padding:10px 14px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);cursor:pointer" onclick="progProjVal=\''+rp.id+'\';renderDash()">';
        h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">';
        h+='<span style="font-size:12px;font-weight:600;color:var(--text)">'+(rp.locs||rp.state)+'</span>';
        h+='<span style="font-size:10px;color:var(--muted);font-family:var(--mono)">'+rp.name+'</span>';
        h+='</div>';
        h+='<div class="prog-bar" style="margin-bottom:4px"><div style="width:'+rp.pctClear+'%;background:var(--green)"></div><div style="width:'+Math.min(rp.pctOpen,100-rp.pctClear)+'%;background:var(--red)"></div><div style="width:'+Math.min(rp.pctDamage,100-rp.pctClear-rp.pctOpen)+'%;background:#f59e0b"></div></div>';
        h+='<div style="display:flex;justify-content:space-between;font-size:10px;font-family:var(--mono)">';
        h+='<span style="color:var(--green)">'+rp.pctClear+'% clear</span>';
        h+='<span style="color:var(--muted)">'+rp.count+' tkt · '+rp.ticketFt.toLocaleString()+' ft</span>';
        h+='</div></div>';
      }
      h+='</div></div>';
    }
  }

  // Detalhamento: so quando um projeto esta selecionado
  if(progProjVal){
    for(var i=0;i<show.length;i++){
    var p=show[i];
    h+='<div style="margin-bottom:18px;padding-bottom:18px;border-bottom:1px solid var(--border)">';
    h+='<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:8px;flex-wrap:wrap;gap:4px">';
    h+='<span style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><span style="font-size:13px;font-weight:700;color:var(--text)">📍 '+(p.locs||p.state)+'</span><span style="font-size:11px;color:var(--muted);font-family:var(--mono)">'+p.name+'</span></span>';
    h+='<span style="font-size:11px;color:var(--muted);font-family:var(--mono)">'+p.ticketFt.toLocaleString()+' ft'+(p.hasTotalFromSheet?' / <strong style="color:var(--text)">'+p.totalFt.toLocaleString()+' ft total</strong>':'')+'</span></div>';
    h+='<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:8px">';
    h+='<div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+p.totalFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-top:2px">Total ft'+(p.hasTotalFromSheet?'*':'')+'</div></div>';
    h+='<div style="padding:9px;background:var(--green-bg);border-radius:var(--r);border:1px solid var(--green-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--green)">'+p.clearFtP.toLocaleString()+'</div><div style="font-size:9px;color:var(--green);text-transform:uppercase;margin-top:2px">Clear '+p.pctClear+'%</div></div>';
    h+='<div style="padding:9px;background:var(--red-bg);border-radius:var(--r);border:1px solid var(--red-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--red)">'+p.openFtP.toLocaleString()+'</div><div style="font-size:9px;color:var(--red);text-transform:uppercase;margin-top:2px">Em aberto '+p.pctOpen+'%</div></div>';
    h+='<div style="padding:9px;background:var(--amber-bg);border-radius:var(--r);border:1px solid var(--amber-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+p.damageFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--amber);text-transform:uppercase;margin-top:2px">Damage '+p.pctDamage+'%</div></div>';
    h+='<div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+p.concluidoFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--text2);text-transform:uppercase;margin-top:2px">Concluído '+p.pctConcluido+'%</div></div>';
    h+='</div>';
    h+='<div class="prog-bar"><div style="width:'+p.pctClear+'%;background:var(--green)"></div><div style="width:'+Math.min(p.pctOpen,100-p.pctClear)+'%;background:var(--red)"></div><div style="width:'+Math.min(p.pctDamage,100-p.pctClear-p.pctOpen)+'%;background:#f59e0b"></div><div style="width:'+Math.min(p.pctConcluido,100-p.pctClear-p.pctOpen-p.pctDamage)+'%;background:var(--text)"></div></div>';
    h+='<div class="prog-legend"><span><span class="prog-dot" style="background:var(--green)"></span>Clear '+p.pctClear+'%</span><span><span class="prog-dot" style="background:var(--red)"></span>Aberto '+p.pctOpen+'%</span>'+(p.damageFt>0?'<span><span class="prog-dot" style="background:#f59e0b"></span>Damage '+p.pctDamage+'%</span>':'')+'<span><span class="prog-dot" style="background:var(--text)"></span>Concluído '+p.pctConcluido+'%</span><span style="margin-left:auto">'+p.count+' tickets</span></div>';
    h+='</div>';
  }
  }

  h+='</div></div>';
  return h;
}

function renderClearedStats(fTickets){
  var now=Date.now(),day1=now-864e5,day7=now-7*864e5,day30=now-30*864e5;

  // ── Dropdown de projetos (usa tickets que tem clear events) ──
  var projsWithClear=new Set();
  function getClearEvts(t){
    if(!t.history||!t.history.length)return[];
    return t.history.filter(function(h){var a=(h.action||'').toLowerCase();return a.indexOf('clear')>=0&&(a.indexOf('\u2192 clear')>=0||a.indexOf('auto-clear')>=0||a.indexOf('auto 811')>=0||a.indexOf('status manual')>=0);});
  }
  for(var i=0;i<fTickets.length;i++){
    var evts=getClearEvts(fTickets[i]);
    if(evts.length&&fTickets[i].projectId)projsWithClear.add(fTickets[i].projectId);
  }
  var projOpts='<option value="">Todos projetos</option>';
  projects.forEach(function(p){
    if(!projsWithClear.has(p.id))return;
    var ts=fTickets.filter(function(t){return t.projectId===p.id;});
    var locs=[...new Set(ts.map(function(t){return(t.location||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim();}).filter(Boolean))].join(', ');
    var label=locs?locs+' — '+p.name:p.name;
    projOpts+='<option value="'+p.id+'"'+(clearProjVal===p.id?' selected':'')+'>'+label+'</option>';
  });

  // ── Filtrar tickets pelo projeto selecionado ──
  var ft=clearProjVal?fTickets.filter(function(t){return t.projectId===clearProjVal;}):fTickets;

  var c24=[],c7=[],c30=[],byU7={};
  for(var i=0;i<ft.length;i++){var t=ft[i];var evts=getClearEvts(t);for(var j=0;j<evts.length;j++){if(evts[j].ts>=day1)c24.push(t);if(evts[j].ts>=day7)c7.push(t);if(evts[j].ts>=day30)c30.push(t);}}
  if(utilCacheLoaded){for(var i=0;i<c7.length;i++){var us=getTicketUtils(String(c7[i].ticket).trim());for(var j=0;j<us.length;j++){if(us[j].status==='Clear'){if(!byU7[us[j].utility_name])byU7[us[j].utility_name]=0;byU7[us[j].utility_name]++;}}}}
  var ft24=0,ft7=0,ft30=0;
  for(var i=0;i<c24.length;i++)ft24+=(c24[i].footage||0);
  for(var i=0;i<c7.length;i++)ft7+=(c7[i].footage||0);
  for(var i=0;i<c30.length;i++)ft30+=(c30[i].footage||0);
  var daily=[];
  for(var i=6;i>=0;i--){var ds=now-(i+1)*864e5,de=now-i*864e5;var lb=new Date(de).toLocaleDateString('pt-BR',{weekday:'short',day:'2-digit'});var cnt=0,dft=0;for(var k=0;k<ft.length;k++){var evts=getClearEvts(ft[k]);for(var j=0;j<evts.length;j++){if(evts[j].ts>=ds&&evts[j].ts<de){cnt++;dft+=(ft[k].footage||0);}}}daily.push({l:lb,c:cnt,f:dft});}
  var mx=1;for(var i=0;i<daily.length;i++)if(daily[i].c>mx)mx=daily[i].c;
  var su7=Object.entries(byU7).sort(function(a,b){return b[1]-a[1];}).slice(0,10);
  if(!c30.length&&!clearProjVal)return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">Tickets Clareados</div><div style="color:var(--muted);font-size:13px">Nenhum ticket clareado nos ultimos 30 dias.</div></div></div>';
  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px"><div class="dash-card-title" style="margin-bottom:0">\u2705 Tickets Clareados</div>';
  h+='<select class="fi" id="clear-proj-filter" onchange="clearProjVal=this.value;renderDash()" style="width:auto;min-width:180px;font-size:12px;padding:5px 8px">'+projOpts+'</select>';
  h+='</div>';
  if(!c30.length&&clearProjVal){h+='<div style="color:var(--muted);font-size:13px;padding:20px 0">Nenhum ticket clareado nos últimos 30 dias neste projeto.</div></div></div>';return h;}
  h+='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:18px">';
  h+='<div style="padding:14px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--green)">'+c24.length+'</div><div style="font-size:10px;color:var(--green);text-transform:uppercase;margin-top:2px">Ultimas 24h</div><div style="font-size:12px;color:var(--green);font-family:var(--mono);margin-top:4px">'+ft24.toLocaleString()+' ft</div></div>';
  h+='<div style="padding:14px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--green)">'+c7.length+'</div><div style="font-size:10px;color:var(--green);text-transform:uppercase;margin-top:2px">Ultimos 7 dias</div><div style="font-size:12px;color:var(--green);font-family:var(--mono);margin-top:4px">'+ft7.toLocaleString()+' ft</div></div>';
  h+='<div style="padding:14px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--green)">'+c30.length+'</div><div style="font-size:10px;color:var(--green);text-transform:uppercase;margin-top:2px">Ultimos 30 dias</div><div style="font-size:12px;color:var(--green);font-family:var(--mono);margin-top:4px">'+ft30.toLocaleString()+' ft</div></div>';
  h+='</div>';
  h+='<div style="display:grid;grid-template-columns:2fr 1fr;gap:16px"><div>';
  h+='<div style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:10px">Clareados por dia (ultimos 7 dias)</div>';
  h+='<div style="display:flex;align-items:flex-end;gap:6px;height:100px;padding-bottom:20px;position:relative">';
  for(var i=0;i<daily.length;i++){var d=daily[i];h+='<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:3px"><div style="font-size:10px;font-family:var(--mono);color:var(--green);font-weight:700">'+(d.c||'')+'</div><div style="width:100%;background:'+(d.c?'var(--green)':'var(--border)')+';border-radius:4px 4px 0 0;min-height:4px;height:'+Math.max(d.c/mx*70,4)+'px"></div><div style="font-size:9px;color:var(--muted);white-space:nowrap">'+d.l+'</div></div>';}
  h+='</div>';
  if(c24.length){h+='<div style="margin-top:10px"><div style="font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:6px">Clareados hoje ('+c24.length+')</div><div style="display:flex;flex-wrap:wrap;gap:4px">';for(var i=0;i<c24.length;i++)h+='<span style="font-size:10px;font-family:var(--mono);padding:2px 7px;border-radius:10px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border);cursor:pointer" onclick="openTicketDetail('+c24[i].id+')">'+c24[i].ticket+'</span>';h+='</div></div>';}
  h+='</div><div>';
  h+='<div style="font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:10px">Utilities que responderam (7d)</div>';
  if(su7.length){for(var i=0;i<su7.length;i++){var nm=su7[i][0],ct=su7[i][1];h+='<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid var(--border);font-size:12px"><span style="color:var(--text2)">'+nm+'</span><span style="font-size:10px;font-weight:700;color:var(--green);font-family:var(--mono);background:var(--green-bg);padding:2px 7px;border-radius:10px">'+ct+'</span></div>';}}
  else{h+='<div style="color:var(--muted);font-size:12px">Sem dados</div>';}
  h+='</div></div></div></div>';
  return h;
}

function renderUtilSummaryHtml(){
  if(!utilCacheLoaded)return'<div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">Utilities 811 — Pendentes</div><div style="color:var(--muted);font-size:12px">Carregando dados de utilities...</div></div>';
  const utilCount={};const utilTickets={};
  const openTickets=tickets.filter(t=>(t.status==='Open'||t.status==='Damage'||t.status==='Clear')&&!isSuperseded(t));
  for(const t of openTickets){
    const pends=getTicketPendingUtils(t.ticket);
    for(const p of pends){
      if(!utilCount[p.utility_name])utilCount[p.utility_name]=0;
      if(!utilTickets[p.utility_name])utilTickets[p.utility_name]=[];
      utilCount[p.utility_name]++;
      utilTickets[p.utility_name].push(t);
    }
  }
  const sorted=Object.entries(utilCount).sort((a,b)=>b[1]-a[1]);
  const totalPending=sorted.reduce((s,e)=>s+e[1],0);
  const ticketsWithPending=new Set();
  for(const t of openTickets){if(getTicketPendingUtils(t.ticket).length>0)ticketsWithPending.add(t.ticket);}
  if(!sorted.length)return'<div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">Utilities 811</div><div style="color:var(--green);font-size:13px;font-weight:600">✅ Nenhuma utility pendente nos tickets ativos!</div></div>';
  return`<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px"><div class="dash-card-title" style="margin-bottom:0">Utilities 811 — Pendentes por empresa</div><div style="display:flex;gap:12px;align-items:center"><span style="font-size:12px;font-family:var(--mono);color:var(--red);font-weight:600">${totalPending} pendências</span><span style="font-size:12px;font-family:var(--mono);color:var(--muted)">${ticketsWithPending.size} tickets</span><button class="btn btn-sm" onclick="exportAllPending()" style="font-size:11px">↓ Excel pendentes</button></div></div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px">${sorted.map(([name,count])=>{const tks=utilTickets[name]||[];const safeName=name.replace(/'/g,"\\'");return`<div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--r);padding:12px"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px"><span style="font-size:13px;font-weight:600;color:var(--text);cursor:pointer" onclick="filterByUtil('${safeName}')">${name}</span><div style="display:flex;gap:4px;align-items:center"><button style="font-size:10px;padding:2px 6px;border-radius:6px;background:var(--red-bg);color:var(--red);border:1px solid var(--red-border);cursor:pointer;font-family:var(--mono)" onclick="event.stopPropagation();exportUtilPending('${safeName}')" title="Exportar Excel">${count} ↓</button></div></div><div style="display:flex;flex-wrap:wrap;gap:3px">${tks.slice(0,5).map(t=>`<span style="font-size:10px;font-family:var(--mono);padding:1px 6px;border-radius:8px;background:var(--white);border:1px solid var(--border);color:var(--text2);cursor:pointer" onclick="event.stopPropagation();openTicketDetail(${t.id})">${t.ticket}</span>`).join('')}${tks.length>5?`<span style="font-size:10px;color:var(--muted)">+${tks.length-5} mais</span>`:''}</div></div>`;}).join('')}</div></div></div>`;
}

function exportUtilPending(utilName){
  const openTks=tickets.filter(t=>(t.status==='Open'||t.status==='Damage'||t.status==='Clear')&&!isSuperseded(t));
  const matching=openTks.filter(t=>{const pends=getTicketPendingUtils(t.ticket);return pends.some(p=>p.utility_name===utilName);});
  if(!matching.length){toast('Nenhum ticket pendente para '+utilName,'warn');return;}
  const wb=XLSX.utils.book_new();
  const rows=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Endereço']];
  for(const t of matching){
    const proj=projects.find(p=>p.id===t.projectId);
    rows.push([t.ticket,proj?proj.name:'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.address]);
  }
  rows.push(['','','','','','','TOTAL:',matching.reduce((s,t)=>s+(t.footage||0),0),'','']);
  const ws=XLSX.utils.aoa_to_sheet(rows);XLSX.utils.book_append_sheet(wb,ws,'Pendentes');
  XLSX.writeFile(wb,'Pendentes_'+utilName.replace(/[^a-zA-Z0-9]/g,'_')+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(matching.length+' tickets pendentes — '+utilName,'success');
}

function exportAllPending(){
  const openTks=tickets.filter(t=>(t.status==='Open'||t.status==='Damage'||t.status==='Clear')&&!isSuperseded(t));
  const rows=[['Utility','Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Endereço']];
  const utilMap={};
  for(const t of openTks){
    const pends=getTicketPendingUtils(t.ticket);
    for(const p of pends){
      if(!utilMap[p.utility_name])utilMap[p.utility_name]=[];
      utilMap[p.utility_name].push(t);
    }
  }
  const sorted=Object.entries(utilMap).sort((a,b)=>b[1].length-a[1].length);
  for(const[name,tks]of sorted){
    for(const t of tks){
      const proj=projects.find(p=>p.id===t.projectId);
      rows.push([name,t.ticket,proj?proj.name:'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.address]);
    }
  }
  const totalFt=rows.slice(1).reduce((s,r)=>s+(r[8]||0),0);
  rows.push(['','','','','','','','TOTAL:',totalFt,'','']);
  const wb=XLSX.utils.book_new();
  const ws=XLSX.utils.aoa_to_sheet(rows);XLSX.utils.book_append_sheet(wb,ws,'Todas Pendentes');
  XLSX.writeFile(wb,'Pendentes_Todas_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(sorted.length+' utilities · '+(rows.length-2)+' pendências','success');
}

function renderClearTimeMetrics(fTickets){
  if(!utilCacheLoaded)return'';

  // ── Calcular tempos por projeto → utility ──
  var projData={};
  var globalUtil={};
  for(var i=0;i<fTickets.length;i++){
    var t=fTickets[i];
    if(!t.history||!t.history.length)continue;
    var createdTs=t.history[0].ts;
    if(!createdTs)continue;
    var pid=t.projectId||'_none';
    var utils=getTicketUtils(String(t.ticket).trim());
    for(var j=0;j<utils.length;j++){
      var u=utils[j];
      if(u.status!=='Clear'||!u.responded_at)continue;
      var respTs=new Date(u.responded_at).getTime();
      if(isNaN(respTs)||respTs<createdTs)continue;
      var days=(respTs-createdTs)/86400000;
      if(days>90)continue;
      var name=u.utility_name;
      if(!projData[pid])projData[pid]={utils:{},totalDays:0,totalCount:0};
      if(!projData[pid].utils[name])projData[pid].utils[name]={total:0,count:0};
      projData[pid].utils[name].total+=days;
      projData[pid].utils[name].count++;
      projData[pid].totalDays+=days;
      projData[pid].totalCount++;
      if(!globalUtil[name])globalUtil[name]={total:0,count:0};
      globalUtil[name].total+=days;
      globalUtil[name].count++;
    }
  }

  // Montar array de projetos
  var projArr=[];
  for(var pid in projData){
    var pd=projData[pid];
    if(pd.totalCount<2)continue;
    var p=projects.find(function(x){return x.id===pid;});
    var pname=p?p.name:'Sem projeto';
    var ts=fTickets.filter(function(t){return t.projectId===pid;});
    var locs=[...new Set(ts.map(function(t){return(t.location||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim();}).filter(Boolean))].join(', ');
    var utilArr=[];
    for(var uname in pd.utils){
      if(pd.utils[uname].count>=1){
        utilArr.push({name:uname,avg:Math.round(pd.utils[uname].total/pd.utils[uname].count*10)/10,count:pd.utils[uname].count});
      }
    }
    utilArr.sort(function(a,b){return b.avg-a.avg;});
    var projAvg=Math.round(pd.totalDays/pd.totalCount*10)/10;
    projArr.push({pid:pid,name:locs||pname,projName:pname,avg:projAvg,count:pd.totalCount,utils:utilArr});
  }
  projArr.sort(function(a,b){return b.avg-a.avg;});

  if(!projArr.length)return'';

  // Utilities a mostrar: filtrado por projeto ou global
  var showUtils=[];
  var showLabel='';
  var showAvg=0;
  var showCount=0;
  if(clearTimeProjVal){
    var sel=projArr.find(function(p){return p.pid===clearTimeProjVal;});
    if(sel){showUtils=sel.utils;showLabel=sel.name;showAvg=sel.avg;showCount=sel.count;}
  }
  if(!showUtils.length&&!clearTimeProjVal){
    // Global
    var gArr=[];
    for(var name in globalUtil){
      if(globalUtil[name].count>=2){
        gArr.push({name:name,avg:Math.round(globalUtil[name].total/globalUtil[name].count*10)/10,count:globalUtil[name].count});
      }
    }
    gArr.sort(function(a,b){return b.avg-a.avg;});
    showUtils=gArr;
    var gTotal=0,gCount=0;
    for(var k in globalUtil){gTotal+=globalUtil[k].total;gCount+=globalUtil[k].count;}
    showAvg=gCount?Math.round(gTotal/gCount*10)/10:0;
    showCount=gCount;
    showLabel='Todos os projetos';
  }

  function colorFor(days,thLow,thMid){return days<=thLow?'var(--green)':days<=thMid?'var(--amber)':'var(--red)';}

  // Dropdown de projetos
  var projOpts='<option value="">Todos os projetos</option>';
  for(var i=0;i<projArr.length;i++){
    var pr=projArr[i];
    var c=colorFor(pr.avg,3,7);
    projOpts+='<option value="'+pr.pid+'"'+(clearTimeProjVal===pr.pid?' selected':'')+' style="color:'+c+'">'+pr.name+' ('+pr.projName+') — '+pr.avg+' dias</option>';
  }

  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;flex-wrap:wrap;gap:8px">';
  h+='<div class="dash-card-title" style="margin-bottom:0">Tempo médio para Clear (dias)</div>';
  h+='<select class="fi" onchange="clearTimeProjVal=this.value;renderDash()" style="width:auto;min-width:220px;font-size:12px;padding:5px 8px">'+projOpts+'</select>';
  h+='</div>';

  // Resumo do selecionado
  var mainColor=colorFor(showAvg,3,7);
  h+='<div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;padding:12px 16px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r)">';
  h+='<div style="font-size:28px;font-weight:700;font-family:var(--mono);color:'+mainColor+'">'+showAvg+'</div>';
  h+='<div><div style="font-size:13px;font-weight:600;color:var(--text)">'+showLabel+'</div>';
  h+='<div style="font-size:11px;color:var(--muted)">dias em média · '+showCount+' respostas</div></div>';
  h+='</div>';

  if(!showUtils.length){
    h+='<div style="color:var(--muted);font-size:13px;padding:12px 0">Sem dados suficientes para este projeto.</div>';
  }else{
    var uMax=showUtils[0].avg||1;
    h+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px 20px">';
    for(var i=0;i<showUtils.length;i++){
      var u=showUtils[i];
      var pct=Math.max(u.avg/uMax*100,6);
      var c=colorFor(u.avg,2,5);
      h+='<div style="margin-bottom:4px">';
      h+='<div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px">';
      h+='<span style="font-size:12px;color:var(--text2)">'+u.name+'</span>';
      h+='<span style="font-size:12px;font-weight:700;font-family:var(--mono);color:'+c+'">'+u.avg+' dias</span>';
      h+='</div>';
      h+='<div style="height:5px;background:var(--border);border-radius:3px;overflow:hidden">';
      h+='<div style="width:'+pct+'%;height:100%;background:'+c+';border-radius:3px"></div>';
      h+='</div>';
      h+='<div style="font-size:9px;color:var(--muted);margin-top:1px">'+u.count+' respostas</div>';
      h+='</div>';
    }
    h+='</div>';
  }

  h+='</div></div>';
  return h;
}

function filterByUtil(utilName){
  nav('tickets');
  setTimeout(()=>{
    const sel=document.getElementById('tbl-util');
    if(sel){sel.value=utilName;renderTable();}
  },100);
}

function nav(page){if(isSharedView)return;document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.snav-item').forEach(t=>t.classList.remove('active'));document.getElementById('pg-'+page).classList.add('active');const btn=document.querySelector('.snav-item[data-page="'+page+'"]');if(btn)btn.classList.add('active');if(page==='map'){setTimeout(()=>{initMap();if(map)map.invalidateSize();},80);}if(page==='proj')renderProjects();if(page==='tickets')renderTable();if(page==='dash')renderDash();}
function openModal(id){document.getElementById(id).classList.add('open');}
function closeModal(id){document.getElementById(id).classList.remove('open');}
let _t2;
function toast(msg,type='success'){const bg={success:'#16803d',danger:'#dc2626',warn:'#b45309',info:'#1a6cf0'};const dot={success:'#86efac',danger:'#fca5a5',warn:'#fde68a',info:'#93c5fd'};document.getElementById('toast').style.background=bg[type]||bg.success;document.getElementById('tdot').style.background=dot[type]||dot.success;document.getElementById('tmsg').textContent=msg;document.getElementById('toast').classList.add('show');clearTimeout(_t2);_t2=setTimeout(()=>document.getElementById('toast').classList.remove('show'),4000);}

function toggleSidebar(){const sb=document.getElementById('map-sidebar');const ov=document.getElementById('sb-overlay');sb.classList.toggle('mob-open');ov.classList.toggle('open');}

function toggleMobNav(){
  const nav=document.getElementById('sidebar-nav');
  const ov=document.getElementById('mob-nav-overlay');
  const open=nav.classList.toggle('mob-open');
  ov.classList.toggle('open',open);
}

function toggleSobrePanel(){
  const p=document.getElementById('sobre-panel');
  const o=document.getElementById('sobre-overlay');
  const open=p.style.display==='none';
  p.style.display=open?'block':'none';
  o.style.display=open?'block':'none';
  if(open)renderSobreRecent();
}
function renderSobreRecent(){
  const el=document.getElementById('sobre-recent');if(!el)return;
  const recent=[...tickets.filter(t=>!isSuperseded(t))].sort((a,b)=>(b.history?.[b.history.length-1]?.ts||0)-(a.history?.[a.history.length-1]?.ts||0)).slice(0,12);
  el.innerHTML=recent.length?recent.map(t=>{const last=t.history?.[t.history.length-1];return`<div style="display:flex;justify-content:space-between;align-items:center;padding:7px 0;border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail(${t.id});toggleSobrePanel()"><div><div style="font-size:12px;font-weight:500;font-family:var(--mono);color:var(--text)">${t.ticket}</div><div style="color:var(--muted);font-size:10px">${last?.action||'—'}</div></div><span class="sbadge b-${t.status.toLowerCase()}" style="font-size:10px">${t.status}</span></div>`;}).join(''):'<div style="color:var(--muted);font-size:12px">Sem atividade recente</div>';
}

function shareProject(pid){const p=projects.find(x=>x.id===pid);if(!p)return;const url=window.location.origin+window.location.pathname+'?p='+encodeURIComponent(p.id);if(navigator.clipboard){navigator.clipboard.writeText(url).then(()=>toast('Link copiado! Quem abrir verá só este projeto.','success')).catch(()=>{prompt('Copie o link:',url);});}else{prompt('Copie o link:',url);}}

function checkProjectUrl(){
  const params=new URLSearchParams(window.location.search);
  const pid=params.get('p');
  if(pid){
    let p=projects.find(x=>x.id===pid);
    if(!p)p=projects.find(x=>x.name===pid);
    if(!p)p=projects.find(x=>x.name.toLowerCase().includes(pid.toLowerCase()));
    if(p){enterSharedView(p.id);return true;}
  }
  return false;
}

window.addEventListener('load',async()=>{
  document.querySelector('#loading-screen div:last-child').textContent='Conectando ao Supabase...';
  const ok=await initSupabase();
  document.getElementById('loading-screen').style.display='none';
  if(!ok){document.getElementById('login-screen').style.display='flex';setTimeout(()=>toast('Aviso: erro ao conectar ao banco.','warn'),500);return;}
  // Check if shared project link
  if(checkProjectUrl())return;
  // Check for existing auth session (auto-login)
  try{
    const{data:{session}}=await sb.auth.getSession();
    if(session){
      const{data:roleData}=await sb.from('app_roles').select('role').eq('user_id',session.user.id).single();
      isAdmin=roleData&&roleData.role==='admin';
      role=isAdmin?'admin':'viewer';
      enterApp();
      return;
    }
  }catch(e){console.log('[Auth] No session:',e);}
  document.getElementById('login-screen').style.display='flex';
});

