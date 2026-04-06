
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
let map,satL,strL,hybL,mkrs=[],lines=[],labels=[];
let shMap,shSatL,shStrL,shHybL,shMkrs=[],shLines=[],shLabels=[];
let clusterGroup=null;
let fieldDrawing=false,fieldPts=[],fieldLine=null,fieldTicketId=null;

let utilCache={},utilCacheLoaded=false;
let dashStateVal='';
let _soonDays=10,_metricProjFilter='',_clearProjFilter='',_progProjFilter='',_velProjFilter='';

// Utilities
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms);};}
function esc(s){const d=document.createElement('div');d.textContent=s;return d.innerHTML;}
const debouncedRedraw=debounce(()=>redrawAll(),250);
const debouncedTable=debounce(()=>renderTable(),250);
const debouncedContacts=debounce(()=>renderContacts(),250);

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
    rebuildSupersededSet();
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
    try{const{data:roleData}=await sb.from('app_roles').select('role').eq('user_id',data.user.id).single();if(roleData&&roleData.role==='admin'){isAdmin=true;role='admin';}else{isAdmin=false;role='viewer';}}catch(e){isAdmin=false;role='viewer';}
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
  document.getElementById('app-shell').style.display='grid';
  document.getElementById('role-badge').textContent=isAdmin?'ADMIN':'VIEWER';
  document.getElementById('role-badge').style.background=isAdmin?'var(--green-bg)':'var(--accent-bg)';
  document.getElementById('role-badge').style.color=isAdmin?'var(--green)':'var(--accent)';
  const logoutBtn=document.getElementById('btn-logout');
  if(logoutBtn)logoutBtn.style.display='';
  if(isAdmin){['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn','btn-add-contact'].forEach(id=>{const e=document.getElementById(id);if(e)e.style.display='';});}
  else{['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn','btn-add-contact'].forEach(id=>{const e=document.getElementById(id);if(e)e.style.display='none';});document.getElementById('field-status-section').style.display='none';}
  syncAll();renderDash();
  loadUtilCache().then(()=>{renderDash();renderTable();buildNotifications();});
  loadContacts().then(()=>renderContacts());
  setInterval(async()=>{if(fieldDrawing){console.log('[AutoRefresh] Pulado — desenho em andamento');return;}if(document.querySelector('.overlay.open')){console.log('[AutoRefresh] Pulado — modal aberto');return;}try{const{data:p}=await sb.from('projects').select('*').order('name');const{data:t}=await sb.from('tickets').select('*').order('ticket');if(p)projects=p.map(dbToProject);if(t)tickets=t.map(dbToTicket);rebuildSupersededSet();await loadUtilCache();await loadLastSync();syncAll();setSyncStatus(true,'Atualizado');console.log('[AutoRefresh] OK');}catch(e){console.error('[AutoRefresh]',e);}},300000);
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

function buildPopup(t,c){const proj=projects.find(p=>p.id===t.projectId);return`<div style="font-family:'DM Sans',sans-serif;font-size:13px;line-height:1.6;min-width:180px;padding:2px"><div style="font-weight:700;color:#18180f;margin-bottom:6px;font-size:14px;font-family:'DM Mono',monospace">${t.ticket}</div>${proj?`<div><span style="color:#9a9888">Projeto: </span>${proj.name}</div>`:''}<div><span style="color:#9a9888">Cliente: </span>${t.client}</div>${t.prime?`<div><span style="color:#9a9888">Prime: </span>${t.prime}</div>`:''}<div><span style="color:#9a9888">Footage: </span>${t.footage} ft</div>${t.tipo?`<div><span style="color:#9a9888">Tipo: </span>${t.tipo}</div>`:''}<div><span style="color:#9a9888">Status: </span><span style="color:${c};font-weight:700">${t.status}</span></div><div><span style="color:#9a9888">Expira: </span>${t.expire||'—'}</div>${t.address?`<div><span style="color:#9a9888">Endereço: </span>${esc(t.address||"")}</div>`:''}<div style="margin-top:7px;padding-top:7px;border-top:1px solid #e2e0da;display:flex;gap:8px"><a href="#" onclick="openTicketDetail(${t.id});return false;" style="color:#1a6cf0;font-size:12px;font-weight:600">Detalhes →</a><a href="#" onclick="openNavigation(${t.id});return false;" style="color:#16a34a;font-size:12px;font-weight:600">🗺 Navegar</a></div></div>`;}

function openNavigation(id){const t=tickets.find(x=>x.id===id);if(!t)return;const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath[0]:t._geocoded;if(coords){window.open(`https://www.google.com/maps/dir/?api=1&destination=${coords[0]},${coords[1]}&travelmode=driving`,'_blank');}else if(t.address){window.open(`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(t.address+', '+t.location+', '+t.state)}`,'_blank');}else{toast('Sem coordenadas para navegar','warn');}}

function showPanel(t){const c=scol(t.status);const proj=projects.find(p=>p.id===t.projectId);currentPanelId=t.id;document.getElementById('ptitle-txt').textContent=t.ticket;document.getElementById('pbody').innerHTML=`${proj?`<div class="mp-row"><span class="mp-key">Projeto</span><span class="mp-val">${proj.name}</span></div>`:''}<div class="mp-row"><span class="mp-key">Cliente</span><span class="mp-val">${t.client}</span></div>${t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${t.prime}</span></div>`:''}<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val" style="cursor:pointer;color:var(--accent)" onclick="quickEditFootage(currentDetailId);return false;" title="Clique para editar">${t.footage} ft ✏</span></div>${t.tipo?`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${t.tipo}</span></div>`:''}<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${t.status}</span></div><div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val">${t.expire||'—'}</span></div>`;document.getElementById('panel').classList.add('vis');}

function hiT(id){document.querySelectorAll('.tcard').forEach(c=>c.classList.remove('active'));const cd=document.querySelector(`[data-id="${id}"]`);if(cd){cd.classList.add('active');cd.scrollIntoView({behavior:'smooth',block:'nearest'})}const t=tickets.find(x=>x.id===id);if(t)showPanel(t);}

let supersededSet=new Set();
function rebuildSupersededSet(){supersededSet=new Set(tickets.map(t=>String(t.oldTicket2||'').trim()).filter(Boolean));}
function isSuperseded(t){return supersededSet.has(String(t.ticket||'').trim());}

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
  document.getElementById('det-info').innerHTML=`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${t.status}${t.status_locked?' 🔒':''}</span></div><div class="mp-row"><span class="mp-key">Empresa</span><span class="mp-val">${t.company||'—'}</span></div>${t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${t.prime}</span></div>`:''}<div class="mp-row"><span class="mp-key">Local</span><span class="mp-val">${t.location}, ${t.state}</span></div><div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val">${t.footage} ft</span></div><div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${t.tipo||'—'}</span></div><div class="mp-row"><span class="mp-key">Job #</span><span class="mp-val">${t.job||'—'}</span></div><div class="mp-row"><span class="mp-key">Endereço</span><span class="mp-val">${t.address||'—'}</span></div><div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val">${t.expire||'—'}</span></div><div class="mp-row"><span class="mp-key">Trajeto</span><span class="mp-val" style="color:${t.fieldPath?'var(--purple)':'var(--muted)'}">${t.fieldPath?`✏️ Campo (${t.fieldPath.length} pts)`:'Sem trajeto'}</span></div>${t.notes?`<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--text2);white-space:pre-wrap;word-break:break-word">${esc(t.notes)}</div>`:''}${hasOldInfo?`<div style="margin-top:10px;padding:9px 11px;background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r)"><div style="font-size:10px;font-weight:700;color:#92400e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">📋 Ticket Anterior</div>${t.pending?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Pending</span><span class="mp-val" style="color:var(--amber)">${t.pending}</span></div>`:''}${t.oldTicket2?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Old Ticket #</span><span class="mp-val" style="font-family:var(--mono);color:#b45309">${t.oldTicket2}</span></div>`:''}${t.statusOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Status Ant.</span><span class="mp-val" style="color:#92400e">${t.statusOld}</span></div>`:''}${t.expireOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Exp. Ant.</span><span class="mp-val" style="color:#92400e">${t.expireOld}</span></div>`:''}</div>`:''}`;
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
  f.sort((a,b)=>{if(sortCol==='risk'){const ra=riskScore(a),rb=riskScore(b);return sortAsc?ra-rb:rb-ra;}if(sortCol==='footage')return sortAsc?(a.footage||0)-(b.footage||0):(b.footage||0)-(a.footage||0);return sortAsc?String(a[sortCol]||'').localeCompare(String(b[sortCol]||'')):String(b[sortCol]||'').localeCompare(String(a[sortCol]||''));});
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
  const total=fTickets.length,open=fTickets.filter(t=>t.status==='Open').length,clear=fTickets.filter(t=>t.status==='Clear').length,damage=fTickets.filter(t=>t.status==='Damage').length,closed=fTickets.filter(t=>t.status==='Closed').length;
  const totalFt=fTickets.reduce((s,t)=>s+(t.footage||0),0);
  const openFt=fTickets.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
  const clearFt=fTickets.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
  const damageFt=fTickets.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
  const noMap=fTickets.filter(t=>(!t.fieldPath||t.fieldPath.length<2)&&t.status!=='Cancel'&&t.status!=='Closed');
  const soon=fTickets.filter(t=>{if(!t.expire||t.expire==='—')return false;const d=new Date(t.expire);const diff=(d-Date.now())/86400000;return diff>=0&&diff<=_soonDays&&t.status!=='Closed'&&t.status!=='Cancel';});
  const fProjects=dsf?projects.filter(p=>p.state===dsf):projects;
  const projStats=fProjects.filter(p=>p.status!=='Completed').map(p=>{const ts=fTickets.filter(t=>t.projectId===p.id);const clearFtP=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);const openFtP=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);const concluidoFt=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);const damageFtP=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);const ticketFt=ts.reduce((s,t)=>s+(t.footage||0),0);const totalFt=p.totalFeet||ticketFt||1;const locs=[...new Set(ts.map(t=>t.location).filter(Boolean).map(l=>l.replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()))].join(', ')||'';return{name:p.name,id:p.id,count:ts.length,clearFtP,openFtP,concluidoFt,damageFt:damageFtP,ticketFt,totalFt,pctClear:totalFt>0?Math.round(clearFtP/totalFt*100):0,pctOpen:totalFt>0?Math.round(openFtP/totalFt*100):0,pctConcluido:totalFt>0?Math.round(concluidoFt/totalFt*100):0,pctDamage:totalFt>0?Math.round(damageFtP/totalFt*100):0,hasTotalFromSheet:!!p.totalFeet,locs,state:p.state||''};}).sort((a,b)=>b.count-a.count);
  const recent=[...fTickets].sort((a,b)=>(b.history?.[b.history.length-1]?.ts||0)-(a.history?.[a.history.length-1]?.ts||0)).slice(0,8);
  const el=document.getElementById('dash-content');if(!el)return;
  el.innerHTML=`<div class="page-title">Dashboard <span style="font-size:13px;font-weight:400;color:var(--muted);font-family:var(--mono)">${new Date().toLocaleDateString('pt-BR')}</span><span style="margin-left:auto">${dashStateFilter}</span></div>
  <div class="stat-grid">
    <div class="stat-card"><div class="stat-label">Total tickets</div><div class="stat-val">${total}</div><div class="stat-sub">${totalFt.toLocaleString()} ft</div></div>
    <div class="stat-card" style="border-left:3px solid var(--red)"><div class="stat-label">Open</div><div class="stat-val" style="color:var(--red)">${open}</div><div class="stat-sub" style="color:var(--red)">${openFt.toLocaleString()} ft</div></div>
    <div class="stat-card" style="border-left:3px solid var(--green)"><div class="stat-label">Clear</div><div class="stat-val" style="color:var(--green)">${clear}</div><div class="stat-sub" style="color:var(--green)">${clearFt.toLocaleString()} ft</div></div>
    <div class="stat-card" style="border-left:3px solid var(--amber)"><div class="stat-label">Damage</div><div class="stat-val" style="color:var(--amber)">${damage}</div><div class="stat-sub" style="color:var(--amber)">${damageFt.toLocaleString()} ft</div></div>
    <div class="stat-card" style="border-left:3px solid var(--purple)"><div class="stat-label">✏️ Sem trajeto</div><div class="stat-val" style="color:var(--purple)">${noMap.length}</div><div class="stat-sub" style="color:var(--purple)">de ${total}</div></div>
  </div><div style="background:var(--white);border:1px solid var(--border);border-radius:var(--r-lg);padding:14px 16px;margin-bottom:16px"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:${soon.length?'10px':'0'}"><div style="font-size:13px;font-weight:600;color:${soon.length?'var(--red)':'var(--green)'}">${soon.length?'⚠ '+soon.length+' ticket(s) vencendo':'✅ Nenhum ticket vencendo'} nos próximos ${_soonDays} dias</div><div style="display:flex;gap:6px;align-items:center"><select class="fi" onchange="_soonDays=parseInt(this.value);renderDash()" style="font-size:11px;padding:4px 7px;width:auto"><option value="3" ${_soonDays===3?'selected':''}>3 dias</option><option value="5" ${_soonDays===5?'selected':''}>5 dias</option><option value="10" ${_soonDays===10?'selected':''}>10 dias</option><option value="15" ${_soonDays===15?'selected':''}>15 dias</option><option value="30" ${_soonDays===30?'selected':''}>30 dias</option></select>${soon.length?`<button class="btn btn-sm" onclick="exportExpiring()" style="background:var(--red);color:white;border-color:var(--red);font-size:11px">↓ Excel</button>`:''}</div></div>${soon.length?`<div style="display:flex;flex-wrap:wrap;gap:5px">${soon.map(t=>{const diff2=Math.round((new Date(t.expire)-Date.now())/86400000);return`<span style="font-size:11px;font-family:var(--mono);padding:3px 8px;border-radius:10px;background:${diff2<=3?'var(--red-bg)':'var(--bg)'};color:${diff2<=3?'var(--red)':'var(--text2)'};border:1px solid ${diff2<=3?'var(--red-border)':'var(--border)'};cursor:pointer" onclick="openTicketDetail(${t.id})">${t.ticket} · ${t.expire}</span>\`;}).join('')}</div>`:''}</div>${noMap.length&&isAdmin?`<div style="background:var(--purple-bg);border:1px solid var(--purple-border);border-radius:var(--r-lg);padding:12px 16px;margin-bottom:16px"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:7px"><div style="font-size:13px;font-weight:600;color:var(--purple)">✏️ ${noMap.length} ticket(s) sem trajeto</div><button onclick="nav('map')" class="btn btn-sm" style="background:var(--purple);color:white;border-color:var(--purple)">Ir para o mapa</button></div><div style="display:flex;flex-wrap:wrap;gap:5px">${noMap.slice(0,20).map(t=>`<span style="font-size:11px;font-family:var(--mono);padding:2px 9px;border-radius:20px;background:rgba(109,40,217,.1);color:var(--purple);cursor:pointer;border:1px solid var(--purple-border)" onclick="goDrawField(${t.id})">${t.ticket}</span>`).join('')}${noMap.length>20?`<span style="font-size:11px;color:var(--muted)">+${noMap.length-20} mais</span>`:''}</div></div>`:''}
  ${renderDashRiskSummary(fTickets)}${renderDashClearedToday(fTickets)}<div style="text-align:center;padding:20px"><button class="btn" onclick="nav('analytics')" style="font-size:13px;padding:10px 24px">📊 Ver Analytics completo →</button></div>`;
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


function exportExpiring(){
  const days=_soonDays||10;
  const f=tickets.filter(t=>{if(!t.expire||t.expire==='—')return false;const d=new Date(t.expire);const diff=(d-Date.now())/86400000;return diff>=0&&diff<=days&&t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t);});
  if(!f.length){toast('Nenhum ticket vencendo nesse período.','warn');return;}
  const wb=XLSX.utils.book_new();const rows=[['Ticket #','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Projeto','Utilities Pendentes']];
  for(const t of f){const proj=projects.find(p=>p.id===t.projectId)?.name||'';const pends=getTicketPendingUtils(String(t.ticket).trim()).map(u=>u.utility_name).join(', ');rows.push([t.ticket,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,proj,pends]);}
  const ws=XLSX.utils.aoa_to_sheet(rows);ws['!cols']=[{wch:14},{wch:20},{wch:16},{wch:7},{wch:20},{wch:8},{wch:9},{wch:12},{wch:12},{wch:24},{wch:10},{wch:20},{wch:30}];
  XLSX.utils.book_append_sheet(wb,ws,'Vencendo');XLSX.writeFile(wb,'OneDrill_Vencendo_'+days+'dias_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(f.length+' tickets exportados','success');
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
function syncAll(){rebuildSupersededSet();syncProjectSelects();syncClients();syncLocations();if(utilCacheLoaded){syncUtilFilter();syncMapUtilFilter();}const ap=document.querySelector('.page.active')?.id;if(ap==='pg-map'){renderList();renderMap();}else if(ap==='pg-tickets')renderTable();else if(ap==='pg-proj')renderProjects();else if(ap==='pg-dash')renderDash();else if(ap==='pg-contacts')renderContacts();else if(ap==='pg-analytics')renderAnalytics();else{renderDash();}}





function riskScore(t){if(!utilCacheLoaded)return 0;let score=0;const now=Date.now();if(t.expire&&t.expire!=='—'){const exp=new Date(t.expire);const diff=(exp-now)/86400000;if(diff<0)score+=60;else if(diff<=2)score+=45;else if(diff<=5)score+=30;else if(diff<=10)score+=18;else if(diff<=20)score+=8;}const pends=getTicketPendingUtils(String(t.ticket).trim());score+=Math.min(pends.length*8,35);if(t.status==='Damage')score+=30;else if(t.status==='Clear')score=Math.max(score-20,0);else if(t.status==='Closed'||t.status==='Cancel')return 0;if(t.history&&t.history.length){const lastTs=t.history[t.history.length-1].ts||0;const daysSince=(now-lastTs)/86400000;if(daysSince>30)score+=15;else if(daysSince>14)score+=8;}if(!t.fieldPath||t.fieldPath.length<2)score+=3;return Math.min(score,100);}
function riskLabel(score){if(score>=60)return{label:'CRÍTICO',color:'#dc2626',bg:'#fef2f2',border:'#fecaca'};if(score>=35)return{label:'ALTO',color:'#d97706',bg:'#fffbeb',border:'#fde68a'};if(score>=15)return{label:'MÉDIO',color:'#2563eb',bg:'#eff6ff',border:'#bfdbfe'};return{label:'BAIXO',color:'#16a34a',bg:'#f0fdf4',border:'#bbf7d0'};}
function renderRiskKPIs(fTickets){if(!utilCacheLoaded)return'';const active=fTickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t));const scored=active.map(t=>({t,s:riskScore(t)}));const critical=scored.filter(x=>x.s>=60);const high=scored.filter(x=>x.s>=35&&x.s<60);const medium=scored.filter(x=>x.s>=15&&x.s<35);const low=scored.filter(x=>x.s<15);const expired=active.filter(t=>{if(!t.expire||t.expire==='—'||t.status!=='Open')return false;return new Date(t.expire)<new Date();});const mkCard=(label,count,color,bg,border,onclick,sub)=>`<div style="padding:14px;background:${bg};border:1px solid ${border};border-radius:var(--r);cursor:pointer" onclick="${onclick}"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:${color}">${count}</div><div style="font-size:10px;font-weight:700;color:${color};text-transform:uppercase;margin-top:2px">${label}</div>${sub?`<div style="font-size:10px;color:${color};opacity:.7;margin-top:3px">${sub}</div>`:''}</div>`;return`<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin:0">🎯 Score de Risco</div><button class="btn btn-sm" onclick="nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)" style="font-size:11px">Tabela por risco →</button></div><div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:${critical.length?'14px':'0'}">${mkCard('Crítico ≥60',critical.length,'#dc2626','#fef2f2','#fecaca',"nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)",critical.length?critical.map(x=>x.t.ticket).slice(0,2).join(', ')+(critical.length>2?'…':''):'Nenhum')}${mkCard('Alto 35–59',high.length,'#d97706','#fffbeb','#fde68a',"nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)",'')}${mkCard('Médio 15–34',medium.length,'#2563eb','#eff6ff','#bfdbfe',"nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)",'')}${mkCard('Baixo <15',low.length,'#16a34a','#f0fdf4','#bbf7d0',"nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)",'')}${mkCard('⚠ Vencidos',expired.length,'#7c3aed','#f5f3ff','#ddd6fe',"nav('tickets');setTimeout(()=>{document.getElementById('tbl-stat').value='Open';renderTable();},100)",expired.length?'ainda Open':'Nenhum')}</div>${critical.length?`<div><div style="font-size:10px;font-weight:700;color:var(--muted);text-transform:uppercase;margin-bottom:6px">Críticos agora</div><div style="display:flex;flex-wrap:wrap;gap:5px">${critical.sort((a,b)=>b.s-a.s).slice(0,15).map(({t,s})=>`<span style="font-size:11px;font-family:var(--mono);padding:3px 9px;border-radius:10px;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;cursor:pointer" onclick="openTicketDetail(${t.id})" title="Score: ${s}">${t.ticket} · ${s}</span>`).join('')}</div></div>`:''}</div></div>`;}
function renderSyncHealthCard(){setTimeout(()=>renderSyncHealth(),100);return`<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">📡 Saúde do Sync</div><div id="sync-health-widget"><div style="color:var(--muted);font-size:12px">Carregando...</div></div></div></div>`;}
async function renderSyncHealth(){try{const r=await fetch(`${SUPABASE_URL}/rest/v1/sync_811_log?select=state,status,started_at&order=started_at.desc&limit=40`,{headers:{apikey:SUPABASE_KEY,Authorization:`Bearer ${SUPABASE_KEY}`}});if(!r.ok)return;const rows=await r.json();const el=document.getElementById('sync-health-widget');if(!el)return;const byState={IN:[],FL:[]};for(const row of rows){if(byState[row.state])byState[row.state].push(row);}let html='';for(const[state,logs]of Object.entries(byState)){if(!logs.length)continue;const last20=logs.slice(0,20);const successes=last20.filter(l=>l.status==='success').length;const rate=Math.round((successes/last20.length)*100);const color=rate>=90?'var(--green)':rate>=70?'var(--amber)':'var(--red)';const dots=last20.map(l=>`<span title="${l.started_at?.slice(0,16)||''}" style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${l.status==='success'?'var(--green)':'var(--red)'};margin:1px"></span>`).join('');html+=`<div style="display:flex;align-items:center;gap:12px;padding:6px 0;border-bottom:1px solid var(--border)"><span style="font-weight:700;font-family:var(--mono);width:24px;color:var(--text)">${state}</span><span style="font-size:16px;font-weight:700;font-family:var(--mono);color:${color};width:42px">${rate}%</span><div style="flex:1;display:flex;flex-wrap:wrap;gap:2px">${dots}</div><span style="font-size:10px;color:var(--muted)">${successes}/${last20.length}</span></div>`;}el.innerHTML=html||'<div style="color:var(--muted);font-size:12px">Sem dados</div>';}catch(e){}}
function renderVelocity(fTickets,projStats){try{const vf=_velProjFilter||'';const week=7*86400000;const now=Date.now();const projVel=projStats.map(p=>{const ts=fTickets.filter(t=>t.projectId===p.id);const cleared4w=ts.filter(t=>{if(!t.history)return false;return t.history.some(h=>{const a=(h.action||'').toLowerCase();return h.ts>=now-4*week&&(a.includes('→ clear')||a.includes('auto 811')||a.includes('auto-clear'));});});const ftPer4w=cleared4w.reduce((s,t)=>s+(t.footage||0),0);const ftPerWeek=ftPer4w/4;const remaining=p.openFtP||0;const weeksLeft=ftPerWeek>0?Math.ceil(remaining/ftPerWeek):null;return{...p,ftPerWeek:Math.round(ftPerWeek),weeksLeft};}).filter(p=>p.count>0);if(!projVel.length)return'';const projOpts='<option value="">Todos projetos</option>'+projVel.map(p=>`<option value="${p.id}"${vf===p.id?' selected':''}>${p.locs?p.locs+' ('+p.name+')':p.name}</option>`).join('');const toShow=vf?projVel.filter(p=>p.id===vf):projVel.filter(p=>p.ftPerWeek>0||p.openFtP>0).slice(0,8);const rows=toShow.map(p=>{const forecast=p.weeksLeft!==null?(p.weeksLeft<=0?'<span style="color:var(--green);font-weight:700">Concluído</span>':p.weeksLeft===1?'<span style="color:var(--amber);font-weight:700">~1 semana</span>':`<span style="color:var(--text2)">~${p.weeksLeft} sem.</span>`):'<span style="color:var(--muted)">sem dados</span>';const vel=p.ftPerWeek>0?`<span style="color:var(--green);font-family:var(--mono);font-weight:600">${p.ftPerWeek.toLocaleString()} ft/sem</span>`:'<span style="color:var(--muted);font-size:11px">parado</span>';return`<tr style="border-bottom:1px solid var(--border)"><td style="padding:8px 6px;font-size:12px;font-weight:600">${p.locs||p.name}</td><td style="padding:8px 6px">${vel}</td><td style="padding:8px 6px">${forecast}</td><td style="padding:8px 6px;min-width:120px"><div style="display:flex;align-items:center;gap:6px"><div style="flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:${p.pctClear}%;height:100%;background:var(--green);border-radius:3px"></div></div><span style="font-size:10px;font-family:var(--mono);color:var(--muted);width:28px">${p.pctClear}%</span></div></td><td style="padding:8px 6px;font-size:11px;color:var(--muted);font-family:var(--mono)">${(p.openFtP||0).toLocaleString()} ft</td></tr>`;}).join('');return`<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin:0">🚀 Velocity & Previsão</div><select class="fi" onchange="_velProjFilter=this.value;renderAnalytics?renderAnalytics():renderDash()" style="width:auto;min-width:160px;font-size:11px;padding:4px 6px">${projOpts}</select></div><table style="width:100%;border-collapse:collapse"><thead><tr style="border-bottom:2px solid var(--border)"><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Projeto</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Velocidade</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Previsão</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Progresso</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Restante</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;}catch(e){return'';}}
/* ══════════ DASHBOARD COMPACT HELPERS ══════════ */
function renderDashRiskSummary(fTickets) {
  if (!utilCacheLoaded) return '';
  const active = fTickets.filter(t => t.status !== 'Closed' && t.status !== 'Cancel' && !isSuperseded(t));
  const scored = active.map(t => ({ t, s: riskScore(t) }));
  const critical = scored.filter(x => x.s >= 60);
  const high     = scored.filter(x => x.s >= 35 && x.s < 60);
  const medium   = scored.filter(x => x.s >= 15 && x.s < 35);
  const low      = scored.filter(x => x.s < 15);
  const expired  = active.filter(t => t.expire && t.expire !== '—' && t.status === 'Open' && new Date(t.expire) < new Date());

  return `<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <div class="dash-card-title" style="margin:0">🎯 Score de Risco</div>
      <button class="btn btn-sm" onclick="nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)" style="font-size:11px">Tabela por risco →</button>
    </div>
    <div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px">
      ${[
        {label:'Crítico ≥60', count:critical.length, color:'#dc2626', bg:'#fef2f2', border:'#fecaca'},
        {label:'Alto 35–59',  count:high.length,     color:'#d97706', bg:'#fffbeb', border:'#fde68a'},
        {label:'Médio 15–34', count:medium.length,   color:'#2563eb', bg:'#eff6ff', border:'#bfdbfe'},
        {label:'Baixo <15',   count:low.length,      color:'#16a34a', bg:'#f0fdf4', border:'#bbf7d0'},
        {label:'⚠ Vencidos',  count:expired.length,  color:'#7c3aed', bg:'#f5f3ff', border:'#ddd6fe'},
      ].map(c => `<div style="padding:12px;background:${c.bg};border:1px solid ${c.border};border-radius:var(--r);cursor:pointer" onclick="nav('tickets');setTimeout(()=>{sortCol='risk';sortAsc=false;renderTable();},100)">
        <div style="font-size:20px;font-weight:700;font-family:var(--mono);color:${c.color}">${c.count}</div>
        <div style="font-size:9px;font-weight:700;color:${c.color};text-transform:uppercase;margin-top:2px">${c.label}</div>
      </div>`).join('')}
    </div>
    ${critical.length ? `<div style="margin-top:12px;display:flex;flex-wrap:wrap;gap:4px">${critical.sort((a,b)=>b.s-a.s).slice(0,10).map(({t,s})=>`<span style="font-size:10px;font-family:var(--mono);padding:2px 7px;border-radius:10px;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;cursor:pointer" onclick="openTicketDetail(${t.id})">${t.ticket} · ${s}</span>`).join('')}</div>` : ''}
  </div></div>`;
}

function renderDashClearedToday(fTickets) {
  const now = Date.now();
  const day1 = now - 86400000;
  const today = fTickets.filter(t => {
    if (!t.history) return false;
    return t.history.some(h => {
      const a = (h.action || '').toLowerCase();
      return h.ts >= day1 && (a.includes('→ clear') || a.includes('auto 811') || a.includes('auto-clear'));
    });
  });
  const ft = today.reduce((s, t) => s + (t.footage || 0), 0);

  // Tickets vencendo hoje/amanhã
  const urgent = fTickets.filter(t => {
    if (!t.expire || t.expire === '—' || t.status === 'Closed' || t.status === 'Cancel') return false;
    const diff = (new Date(t.expire) - now) / 86400000;
    return diff >= 0 && diff <= 2;
  });

  return `<div class="dash-row" style="display:grid;grid-template-columns:1fr 1fr;gap:14px">
    <div class="dash-card">
      <div class="dash-card-title">✅ Clareados hoje</div>
      <div style="font-size:28px;font-weight:700;font-family:var(--mono);color:var(--green)">${today.length}</div>
      <div style="font-size:12px;color:var(--green);font-family:var(--mono);margin-top:4px">${ft.toLocaleString()} ft</div>
      ${today.length ? `<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:3px">${today.slice(0,8).map(t=>`<span style="font-size:10px;font-family:var(--mono);padding:2px 6px;border-radius:8px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border);cursor:pointer" onclick="openTicketDetail(${t.id})">${t.ticket}</span>`).join('')}</div>` : '<div style="font-size:12px;color:var(--muted);margin-top:8px">Nenhum ainda</div>'}
    </div>
    <div class="dash-card">
      <div class="dash-card-title">🔥 Vencendo em 48h</div>
      <div style="font-size:28px;font-weight:700;font-family:var(--mono);color:${urgent.length?'var(--red)':'var(--green)'}">${urgent.length}</div>
      <div style="font-size:12px;color:var(--muted);margin-top:4px">${urgent.length ? 'requerem atenção imediata' : 'Nenhum urgente'}</div>
      ${urgent.length ? `<div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:3px">${urgent.slice(0,8).map(t=>`<span style="font-size:10px;font-family:var(--mono);padding:2px 6px;border-radius:8px;background:var(--red-bg);color:var(--red);border:1px solid var(--red-border);cursor:pointer" onclick="openTicketDetail(${t.id})">${t.ticket} · ${t.expire}</span>`).join('')}</div>` : ''}
    </div>
  </div>`;
}

/* ══════════ ANALYTICS PAGE ══════════ */
function renderAnalytics() {
  const el = document.getElementById('analytics-content');
  if (!el) return;
  const states = [...new Set(tickets.map(t => t.state).filter(Boolean))].sort();
  const dsf = dashStateVal;
  const fTickets = dsf ? tickets.filter(t => t.state === dsf && !isSuperseded(t)) : tickets.filter(t => !isSuperseded(t));
  const fProjects = dsf ? projects.filter(p => p.state === dsf) : projects;
  const projStats = fProjects.filter(p => p.status !== 'Completed').map(p => {
    const ts = fTickets.filter(t => t.projectId === p.id);
    const clearFtP = ts.filter(t => t.status === 'Clear').reduce((s,t) => s+(t.footage||0), 0);
    const openFtP  = ts.filter(t => t.status === 'Open').reduce((s,t)  => s+(t.footage||0), 0);
    const concluidoFt = ts.filter(t => t.status === 'Closed').reduce((s,t) => s+(t.footage||0), 0);
    const damageFt = ts.filter(t => t.status === 'Damage').reduce((s,t) => s+(t.footage||0), 0);
    const ticketFt = ts.reduce((s,t) => s+(t.footage||0), 0);
    const totalFt  = p.totalFeet || ticketFt || 1;
    const locs = [...new Set(ts.map(t => t.location).filter(Boolean).map(l => l.replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()))].join(', ') || '';
    return { name:p.name, id:p.id, count:ts.length, clearFtP, openFtP, concluidoFt, damageFt, ticketFt, totalFt,
      pctClear:totalFt>0?Math.round(clearFtP/totalFt*100):0, pctOpen:totalFt>0?Math.round(openFtP/totalFt*100):0,
      pctConcluido:totalFt>0?Math.round(concluidoFt/totalFt*100):0, pctDamage:totalFt>0?Math.round(damageFt/totalFt*100):0,
      hasTotalFromSheet:!!p.totalFeet, locs, state:p.state||'' };
  }).sort((a,b) => b.count-a.count);

  const stateFilter = `<select class="fi" id="analytics-state-filter" onchange="dashStateVal=this.value;renderAnalytics()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px"><option value="">Todos estados</option>${states.map(s=>`<option value="${s}"${dsf===s?' selected':''}>${s}</option>`).join('')}</select>`;

  el.innerHTML = `
    <div class="page-title">Analytics <span style="font-size:13px;font-weight:400;color:var(--muted);font-family:var(--mono)">${new Date().toLocaleDateString('pt-BR')}</span><span style="margin-left:auto">${stateFilter}</span></div>
    ${renderRiskKPIs(fTickets)}
    ${renderProgressoFootage(fTickets, projStats)}
    ${renderVelocity(fTickets, projStats)}
    ${renderClearedStats(fTickets)}
    ${renderClearTimeMetrics(fTickets)}
    ${renderUtilSummaryHtml()}
    ${renderSyncHealthCard()}
    ${renderWeeklyEvolution(fTickets)}
  `;
}

function renderClearedStats(fTickets){
  var now=Date.now(),day1=now-864e5,day7=now-7*864e5,day30=now-30*864e5;
  var cpf=_clearProjFilter||'';
  var ft2=cpf?fTickets.filter(function(t){return t.projectId===cpf;}):fTickets;
  function getClearEvts(t){
    if(!t.history||!t.history.length)return[];
    return t.history.filter(function(h){var a=(h.action||'').toLowerCase();return a.indexOf('clear')>=0&&(a.indexOf('\u2192 clear')>=0||a.indexOf('auto-clear')>=0||a.indexOf('auto 811')>=0||a.indexOf('status manual')>=0);});
  }
  var c24=[],c7=[],c30=[],byU7={};
  for(var i=0;i<ft2.length;i++){var t=ft2[i];var evts=getClearEvts(t);for(var j=0;j<evts.length;j++){if(evts[j].ts>=day1)c24.push(t);if(evts[j].ts>=day7)c7.push(t);if(evts[j].ts>=day30)c30.push(t);}}
  if(utilCacheLoaded){for(var i=0;i<c7.length;i++){var us=getTicketUtils(String(c7[i].ticket).trim());for(var j=0;j<us.length;j++){if(us[j].status==='Clear'){if(!byU7[us[j].utility_name])byU7[us[j].utility_name]=0;byU7[us[j].utility_name]++;}}}}
  var ft24=0,ft7=0,ft30=0;
  for(var i=0;i<c24.length;i++)ft24+=(c24[i].footage||0);
  for(var i=0;i<c7.length;i++)ft7+=(c7[i].footage||0);
  for(var i=0;i<c30.length;i++)ft30+=(c30[i].footage||0);
  var daily=[];
  for(var i=6;i>=0;i--){var ds=now-(i+1)*864e5,de=now-i*864e5;var lb=new Date(de).toLocaleDateString('pt-BR',{weekday:'short',day:'2-digit'});var cnt=0,dft=0;for(var k=0;k<ft2.length;k++){var evts=getClearEvts(ft2[k]);for(var j=0;j<evts.length;j++){if(evts[j].ts>=ds&&evts[j].ts<de){cnt++;dft+=(ft2[k].footage||0);}}}daily.push({l:lb,c:cnt,f:dft});}
  var mx=1;for(var i=0;i<daily.length;i++)if(daily[i].c>mx)mx=daily[i].c;
  var su7=Object.entries(byU7).sort(function(a,b){return b[1]-a[1];}).slice(0,10);
  if(!c30.length)return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">Tickets Clareados</div><div style="color:var(--muted);font-size:13px">Nenhum ticket clareado nos ultimos 30 dias.</div></div></div>';
  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">';
  h+='<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin-bottom:0">\u2705 Tickets Clareados</div><select class="fi" onchange="_clearProjFilter=this.value;renderDash()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px"><option value="">Todos projetos</option>'+projects.filter(function(p){return p.status!=="Completed";}).map(function(p){return'<option value="'+p.id+'"'+(cpf===p.id?" selected":"")+'>'+projDropLabel(p)+'</option>';}).join("")+'</select></div>';
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
  return`<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin-bottom:0">Utilities 811 — Pendentes por empresa</div><div style="display:flex;gap:12px;align-items:center;font-size:12px;font-family:var(--mono)"><span style="color:var(--red);font-weight:600">${totalPending} pendências</span><span style="color:var(--muted)">${ticketsWithPending.size} tickets afetados</span><button class="btn btn-sm" onclick="exportAllPending()" style="font-size:10px">↓ Excel pendentes</button></div></div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px">${sorted.map(([name,count])=>{const tks=utilTickets[name]||[];return`<div style="background:var(--bg);border:1px solid var(--border);border-radius:var(--r);padding:12px;cursor:pointer" onclick="filterByUtil('${name.replace(/'/g,"\\'")}')"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px"><span style="font-size:13px;font-weight:600;color:var(--text)">${name}</span><span style="font-size:12px;font-weight:700;color:var(--red);font-family:var(--mono);background:var(--red-bg);padding:2px 8px;border-radius:10px">${count}</span></div><div style="display:flex;flex-wrap:wrap;gap:3px">${tks.slice(0,5).map(t=>`<span style="font-size:10px;font-family:var(--mono);padding:1px 6px;border-radius:8px;background:var(--white);border:1px solid var(--border);color:var(--text2);cursor:pointer" onclick="event.stopPropagation();openTicketDetail(${t.id})">${t.ticket}</span>`).join('')}${tks.length>5?`<span style="font-size:10px;color:var(--muted)">+${tks.length-5} mais</span>`:''}</div><div style="display:flex;gap:4px;margin-top:6px"><button class="btn btn-sm" onclick="event.stopPropagation();filterByUtil('${name.replace(/'/g,"\\\\'")}')" style="font-size:10px">Ver tickets</button><button class="btn btn-sm" onclick="event.stopPropagation();exportUtilTickets('${name.replace(/'/g,"\\\\'")}')" style="font-size:10px">↓ Excel</button></div></div>`;}).join('')}</div></div></div>`;
}

function renderClearTimeMetrics(fTickets){
  if(!utilCacheLoaded)return'';
  var mpf=_metricProjFilter||'';
  var ft3=mpf?fTickets.filter(function(t){return t.projectId===mpf;}):fTickets;
  var utilTimes={};
  for(var i=0;i<ft3.length;i++){var t=ft3[i];if(!t.history||!t.history.length)continue;var createdTs=t.history[0].ts;if(!createdTs)continue;var utils=getTicketUtils(String(t.ticket).trim());for(var j=0;j<utils.length;j++){var u=utils[j];if(u.status!=='Clear'||!u.responded_at)continue;var respTs=new Date(u.responded_at).getTime();if(isNaN(respTs)||respTs<createdTs)continue;var days=(respTs-createdTs)/86400000;if(days>90)continue;var name=u.utility_name;if(!utilTimes[name])utilTimes[name]={total:0,count:0};utilTimes[name].total+=days;utilTimes[name].count++;}}
  var utilAvg=[];for(var name in utilTimes){if(utilTimes[name].count>=2){utilAvg.push({name:name,avg:Math.round(utilTimes[name].total/utilTimes[name].count*10)/10,count:utilTimes[name].count});}}
  utilAvg.sort(function(a,b){return b.avg-a.avg;});
  if(!utilAvg.length)return'';
  var projOpts='<option value="">Todos projetos</option>'+projects.filter(function(p){return p.status!=='Completed';}).map(function(p){return'<option value="'+p.id+'"'+(mpf===p.id?' selected':'')+'>'+projDropLabel(p)+'</option>';}).join('');
  var projSel='<select class="fi" onchange="_metricProjFilter=this.value;renderDash()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';
  var globalAvg=utilAvg.reduce(function(s,u){return s+u.avg*u.count;},0)/utilAvg.reduce(function(s,u){return s+u.count;},0);
  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><div class="dash-card-title" style="margin-bottom:0">⏱ Tempo médio para Clear</div><div style="display:flex;align-items:center;gap:12px"><span style="font-size:20px;font-weight:700;font-family:var(--mono);color:'+(globalAvg<=3?'var(--green)':globalAvg<=6?'var(--amber)':'var(--red)')+'">'+globalAvg.toFixed(1)+' dias</span>'+projSel+'</div></div>';
  h+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:6px">';
  for(var i=0;i<Math.min(utilAvg.length,12);i++){var u=utilAvg[i];var color=u.avg<=3?'var(--green)':u.avg<=6?'var(--amber)':'var(--red)';h+='<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)"><span style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:140px" title="'+u.name+'">'+u.name+'</span><span style="font-size:12px;font-weight:700;font-family:var(--mono);color:'+color+'">'+u.avg+'d</span></div>';}
  h+='</div></div></div>';return h;
}
function projDropLabel(p){
  var ts=tickets.filter(function(t){return t.projectId===p.id;});
  var locs=[...new Set(ts.map(function(t){return(t.location||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim();}).filter(Boolean))].join(', ');
  return(locs?locs+' ('+p.name+')':p.name);
}
function filterByUtil(utilName){
  nav('tickets');
  setTimeout(()=>{
    const sel=document.getElementById('tbl-util');
    if(sel){sel.value=utilName;renderTable();}
  },100);
}

function nav(page){if(isSharedView)return;document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));document.querySelectorAll('.snav-item').forEach(t=>t.classList.remove('active'));document.getElementById('pg-'+page).classList.add('active');const btn=document.querySelector('.snav-item[data-page="'+page+'"]');if(btn)btn.classList.add('active');if(page==='map'){setTimeout(()=>{initMap();if(map)map.invalidateSize();},80);}if(page==='proj')renderProjects();if(page==='tickets')renderTable();if(page==='dash')renderDash();if(page==='contacts')renderContacts();if(page==='analytics')renderAnalytics();}
function openModal(id){document.getElementById(id).classList.add('open');}
function closeModal(id){document.getElementById(id).classList.remove('open');}
let _t2;
function toast(msg,type='success'){const bg={success:'#16803d',danger:'#dc2626',warn:'#b45309',info:'#1a6cf0'};const dot={success:'#86efac',danger:'#fca5a5',warn:'#fde68a',info:'#93c5fd'};document.getElementById('toast').style.background=bg[type]||bg.success;document.getElementById('tdot').style.background=dot[type]||dot.success;document.getElementById('tmsg').textContent=msg;document.getElementById('toast').classList.add('show');clearTimeout(_t2);_t2=setTimeout(()=>document.getElementById('toast').classList.remove('show'),4000);}

function toggleSidebar(){const sb=document.getElementById('map-sidebar');const ov=document.getElementById('sb-overlay');sb.classList.toggle('mob-open');ov.classList.toggle('open');}

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

function exportUtilTickets(utilName){
  const openTickets=tickets.filter(t=>(t.status==='Open'||t.status==='Damage'||t.status==='Clear')&&!isSuperseded(t));
  const tks=openTickets.filter(t=>{const pends=getTicketPendingUtils(t.ticket);return pends.some(p=>p.utility_name===utilName);});
  if(!tks.length){toast('Nenhum ticket pendente para '+utilName,'warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[['Ticket #','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Projeto'],...tks.map(t=>[t.ticket,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,projects.find(p=>p.id===t.projectId)?.name||''])];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Pendentes');
  XLSX.writeFile(wb,'OneDrill_'+utilName.replace(/[^a-zA-Z0-9]/g,'_')+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(tks.length+' tickets exportados — '+utilName,'success');
}
function exportAllPending(){
  if(!utilCacheLoaded){toast('Aguarde carregar utilities','warn');return;}
  const openTickets=tickets.filter(t=>(t.status==='Open'||t.status==='Damage'||t.status==='Clear')&&!isSuperseded(t));
  const rows=[];
  for(const t of openTickets){
    const pends=getTicketPendingUtils(t.ticket);
    if(!pends.length)continue;
    for(const p of pends){
      rows.push([t.ticket,p.utility_name,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,projects.find(pr=>pr.id===t.projectId)?.name||'']);
    }
  }
  if(!rows.length){toast('Nenhuma pendência','warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[['Ticket #','Utility Pendente','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Projeto'],...rows];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Todas Pendentes');
  XLSX.writeFile(wb,'OneDrill_Pendentes_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(rows.length+' pendências exportadas','success');
}


async function loadLastSync(){
  try{
    const r=await fetch(`${SUPABASE_URL}/rest/v1/sync_811_log?select=state,finished_at,tickets_checked,status&order=finished_at.desc&limit=20`,{headers:{apikey:SUPABASE_KEY,Authorization:`Bearer ${SUPABASE_KEY}`}});
    if(!r.ok){console.warn('[LastSync] HTTP',r.status);return;}
    const rows=await r.json();
    const byState={};
    for(const row of rows){if(!byState[row.state])byState[row.state]=row;}
    const parts=[];
    for(const state of ['IN','FL']){
      const row=byState[state];if(!row||!row.finished_at){parts.push(state+': —');continue;}
      const d=new Date(row.finished_at);const diffMin=Math.round((Date.now()-d.getTime())/60000);
      let ago;if(diffMin<2)ago='agora';else if(diffMin<60)ago=diffMin+'min atrás';else if(diffMin<120)ago='1h atrás';else if(diffMin<1440)ago=Math.round(diffMin/60)+'h atrás';else ago=Math.round(diffMin/1440)+'d atrás';
      parts.push((row.status==='success'?'🟢':'🔴')+' '+state+': '+ago+' ('+(row.tickets_checked||0)+')');
    }
    const el=document.getElementById('last-sync-status');
    if(el)el.innerHTML=parts.join('<br>');
  }catch(e){console.error('[LastSync]',e);}
}
/* ══════════ WEEKLY EVOLUTION (4 weeks) ══════════ */
function renderWeeklyEvolution(fTickets){
  try{
  const now=Date.now(),week=7*864e5;
  const allF=dashStateVal?tickets.filter(t=>t.state===dashStateVal):tickets;
  function countInRange(start,end,matchFn){return allF.filter(t=>(t.history||[]).some(h=>h.ts>=start&&h.ts<end&&matchFn(h))).length;}
  function isClear(h){const a=(h.action||'').toLowerCase();return a.includes('clear')&&(a.includes('→ clear')||a.includes('auto-clear')||a.includes('auto 811')||a.includes('status manual'));}
  function isOpen(h){const a=(h.action||'').toLowerCase();return a.includes('importado')||a.includes('ticket criado')||(a.includes('→ open')&&!a.includes('auto'));}
  function isClosed(h){const a=(h.action||'').toLowerCase();return a.includes('→ closed')||a.includes('completed');}
  const weeks=[];
  for(let w=0;w<4;w++){const end=now-w*week,start=end-week;const lbl=w===0?'Esta semana':w===1?'Anterior':(w+1)+' sem. atrás';weeks.push({lbl,open:countInRange(start,end,isOpen),clear:countInRange(start,end,isClear),closed:countInRange(start,end,isClosed)});}
  function arrow(curr,prev,greenUp){if(prev===undefined)return'';const diff=curr-prev;if(diff===0)return'<span style="color:var(--muted);font-size:10px;margin-left:3px">—</span>';const up=diff>0;const color=(up===greenUp)?'var(--green)':'var(--red)';return'<span style="color:'+color+';font-size:10px;font-weight:700;margin-left:3px">'+(up?'▲':'▼')+Math.abs(diff)+'</span>';}
  let h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">📊 Evolução Semanal</div><table class="evo-table"><thead><tr><th>Métrica</th>';
  for(const w of weeks)h+='<th>'+w.lbl+'</th>';
  h+='</tr></thead><tbody><tr><td style="color:var(--text2)">Tickets abertos</td>';
  for(let i=0;i<weeks.length;i++)h+='<td>'+weeks[i].open+(i<weeks.length-1?arrow(weeks[i].open,weeks[i+1].open,true):'')+'</td>';
  h+='</tr><tr><td style="color:var(--text2)">Tickets clear</td>';
  for(let i=0;i<weeks.length;i++)h+='<td style="color:var(--green);font-weight:700">'+weeks[i].clear+(i<weeks.length-1?arrow(weeks[i].clear,weeks[i+1].clear,true):'')+'</td>';
  h+='</tr><tr><td style="color:var(--text2)">Concluídos</td>';
  for(let i=0;i<weeks.length;i++)h+='<td>'+weeks[i].closed+(i<weeks.length-1?arrow(weeks[i].closed,weeks[i+1].closed,true):'')+'</td>';
  h+='</tr></tbody></table></div></div>';
  return h;
  }catch(e){console.error('Weekly evolution error:',e);return'';}
}

/* ══════════ PROGRESSO FOOTAGE (aggregated + filter) ══════════ */
function renderProgressoFootage(fTickets,projStats){
  try{
  const pf=window._progProjFilter||'';
  const projOpts='<option value="">Todos (agrupado)</option>'+projStats.map(p=>'<option value="'+p.id+'"'+(pf===p.id?' selected':'')+'>'+(p.locs?p.locs+' ('+p.name+')':p.name)+'</option>').join('');
  const projSel='<select class="fi" onchange="_progProjFilter=this.value;renderDash()" style="width:auto;min-width:160px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';
  function mkGrid(d){return'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:8px"><div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+d.totalFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-top:2px">Total ft</div></div><div style="padding:9px;background:var(--green-bg);border-radius:var(--r);border:1px solid var(--green-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--green)">'+d.clearFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--green);text-transform:uppercase;margin-top:2px">Clear '+d.pctClear+'%</div></div><div style="padding:9px;background:var(--red-bg);border-radius:var(--r);border:1px solid var(--red-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--red)">'+d.openFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--red);text-transform:uppercase;margin-top:2px">Aberto '+d.pctOpen+'%</div></div><div style="padding:9px;background:var(--amber-bg);border-radius:var(--r);border:1px solid var(--amber-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+d.damageFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--amber);text-transform:uppercase;margin-top:2px">Damage '+d.pctDamage+'%</div></div><div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+d.concluidoFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--text2);text-transform:uppercase;margin-top:2px">Concluído '+d.pctConcluido+'%</div></div></div><div class="prog-bar"><div style="width:'+d.pctClear+'%;background:var(--green)"></div><div style="width:'+Math.min(d.pctOpen,100-d.pctClear)+'%;background:var(--red)"></div><div style="width:'+Math.min(d.pctDamage,100-d.pctClear-d.pctOpen)+'%;background:#f59e0b"></div><div style="width:'+Math.min(d.pctConcluido,100-d.pctClear-d.pctOpen-d.pctDamage)+'%;background:var(--text)"></div></div>';}
  let content='';
  if(!pf){
    const tf=projStats.reduce((s,p)=>s+p.totalFt,0)||1;const cf=projStats.reduce((s,p)=>s+p.clearFtP,0);const of2=projStats.reduce((s,p)=>s+p.openFtP,0);const df=projStats.reduce((s,p)=>s+p.damageFt,0);const clf=projStats.reduce((s,p)=>s+p.concluidoFt,0);
    content=mkGrid({totalFt:tf,clearFt:cf,openFt:of2,damageFt:df,concluidoFt:clf,pctClear:Math.round(cf/tf*100),pctOpen:Math.round(of2/tf*100),pctDamage:Math.round(df/tf*100),pctConcluido:Math.round(clf/tf*100)});
    content+='<div class="prog-legend"><span><span class="prog-dot" style="background:var(--green)"></span>Clear</span><span><span class="prog-dot" style="background:var(--red)"></span>Aberto</span><span><span class="prog-dot" style="background:#f59e0b"></span>Damage</span><span><span class="prog-dot" style="background:var(--text)"></span>Concluído</span><span style="margin-left:auto">'+projStats.reduce((s,p)=>s+p.count,0)+' tickets · '+projStats.length+' projetos</span></div>';
  }else{
    const p=projStats.find(x=>x.id===pf);
    if(p){content='<div style="font-size:13px;font-weight:700;color:var(--text);margin-bottom:8px">📍 '+(p.locs||p.state)+' <span style="font-size:11px;color:var(--muted);font-family:var(--mono)">'+p.name+'</span></div>'+mkGrid({totalFt:p.totalFt,clearFt:p.clearFtP,openFt:p.openFtP,damageFt:p.damageFt,concluidoFt:p.concluidoFt,pctClear:p.pctClear,pctOpen:p.pctOpen,pctDamage:p.pctDamage,pctConcluido:p.pctConcluido})+'<div class="prog-legend"><span><span class="prog-dot" style="background:var(--green)"></span>Clear '+p.pctClear+'%</span><span><span class="prog-dot" style="background:var(--red)"></span>Aberto '+p.pctOpen+'%</span><span><span class="prog-dot" style="background:var(--text)"></span>Concluído '+p.pctConcluido+'%</span><span style="margin-left:auto">'+p.count+' tickets</span></div>';}
    else content='<div style="color:var(--muted)">Projeto não encontrado</div>';
  }
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><div class="dash-card-title" style="margin-bottom:0">Progresso por Footage</div>'+projSel+'</div>'+content+'</div></div>';
  }catch(e){console.error('Progresso error:',e);return'';}
}

/* ══════════ GLOBAL SEARCH ══════════ */
function globalSearch(q){
  const dd=document.getElementById('gsearch-dd');if(!dd)return;
  q=(q||'').toLowerCase().trim();
  if(!q||q.length<2){dd.innerHTML='';dd.classList.remove('has-results');return;}
  const results=[];
  for(const t of tickets){if(results.length>=10)break;if(t.ticket.toLowerCase().includes(q)||(t.client||'').toLowerCase().includes(q)||(t.address||'').toLowerCase().includes(q)||(t.prime||'').toLowerCase().includes(q)){results.push({type:'ticket',id:t.id,title:t.ticket,sub:t.client+' · '+t.location+' · '+t.status,status:t.status});}}
  for(const p of projects){if(results.length>=12)break;if(p.name.toLowerCase().includes(q)||(p.client||'').toLowerCase().includes(q)){results.push({type:'project',id:p.id,title:p.name,sub:p.client+' · '+p.state});}}
  if(!results.length){dd.innerHTML='<div style="padding:12px;color:var(--muted);font-size:12px;text-align:center">Nenhum resultado</div>';dd.classList.add('has-results');return;}
  dd.innerHTML=results.map(r=>'<div class="gsr-item" onmousedown="'+(r.type==='ticket'?'openTicketDetail('+r.id+')':'openProjectMap(\''+r.id+'\')')+';document.getElementById(\'gsearch\').value=\'\';document.getElementById(\'gsearch-dd\').classList.remove(\'has-results\')"><div class="gsr-num">'+(r.type==='ticket'?'🎫':'📁')+' '+r.title+(r.status?' <span class="sbadge b-'+r.status.toLowerCase()+'" style="font-size:9px">'+r.status+'</span>':'')+'</div><div class="gsr-sub">'+r.sub+'</div></div>').join('');
  dd.classList.add('has-results');
}

/* ══════════ NOTIFICATIONS ══════════ */
function buildNotifications(){
  try{
  const notifs=[];const now=Date.now(),day3=3*864e5;
  const expiring=tickets.filter(t=>{if(!t.expire||t.expire==='—'||t.status==='Closed'||t.status==='Cancel'||isSuperseded(t))return false;const d=new Date(t.expire);const diff=(d-now)/864e5;return diff>=0&&diff<=5;});
  for(const t of expiring)notifs.push({icon:'⏰',text:t.ticket+' expira '+t.expire,id:t.id,type:'warn'});
  for(const t of tickets){if(!t.history)continue;for(const h of t.history){if(h.ts>=now-day3&&(h.action||'').toLowerCase().includes('→ clear')){notifs.push({icon:'✅',text:t.ticket+' clareado',id:t.id,type:'good'});break;}}}
  const damages=tickets.filter(t=>t.status==='Damage'&&!isSuperseded(t));
  for(const t of damages)notifs.push({icon:'⚠️',text:t.ticket+' com damage',id:t.id,type:'danger'});
  const badge=document.getElementById('notif-badge');
  const urgent=notifs.filter(n=>n.type==='warn'||n.type==='danger').length;
  if(badge){badge.textContent=urgent;badge.style.display=urgent>0?'':'none';}
  window._notifs=notifs.slice(0,30);
  }catch(e){console.error('Notifications error:',e);}
}
function toggleNotifPanel(){
  const panel=document.getElementById('notif-panel');if(!panel)return;
  document.getElementById('info-panel')?.classList.remove('open');
  const open=panel.classList.toggle('open');
  if(open){
    const notifs=window._notifs||[];
    if(!notifs.length){panel.innerHTML='<div style="padding:16px;text-align:center;color:var(--muted);font-size:13px">Nenhuma notificação</div>';return;}
    const byType={warn:[],danger:[],good:[],info:[]};
    for(const n of notifs)(byType[n.type]||byType.info).push(n);
    let h='';
    if(byType.warn.length||byType.danger.length){h+='<div class="notif-section">⚠ Atenção</div>';for(const n of [...byType.danger,...byType.warn])h+='<div class="notif-item" onclick="openTicketDetail('+n.id+');toggleNotifPanel()">'+n.icon+' '+n.text+'</div>';}
    if(byType.good.length){h+='<div class="notif-section">✅ Resolvidos</div>';for(const n of byType.good.slice(0,10))h+='<div class="notif-item" onclick="openTicketDetail('+n.id+');toggleNotifPanel()">'+n.icon+' '+n.text+'</div>';}
    panel.innerHTML=h;
  }
}
function toggleInfoPanel(){
  const panel=document.getElementById('info-panel');if(!panel)return;
  document.getElementById('notif-panel')?.classList.remove('open');
  const open=panel.classList.toggle('open');
  if(open){
    const fTickets=dashStateVal?tickets.filter(t=>t.state===dashStateVal&&!isSuperseded(t)):tickets.filter(t=>!isSuperseded(t));
    const recent=[...fTickets].sort((a,b)=>(b.history?.[b.history.length-1]?.ts||0)-(a.history?.[a.history.length-1]?.ts||0)).slice(0,10);
    let h='<div class="notif-section">📋 Atividade recente</div>';
    for(const t of recent){const last=t.history?.[t.history.length-1];h+='<div class="notif-item" onclick="openTicketDetail('+t.id+');toggleInfoPanel()"><span style="font-family:var(--mono);font-weight:600">'+t.ticket+'</span> <span class="sbadge b-'+t.status.toLowerCase()+'" style="font-size:9px">'+t.status+'</span><div style="font-size:10px;color:var(--muted);margin-top:1px">'+(last?.action||'—')+'</div></div>';}
    h+='<div class="notif-section" style="margin-top:12px">ℹ️ Sistema</div><div style="font-size:12px;color:var(--text2);padding:6px 10px;line-height:1.8">🔵 Dados: Supabase<br>🟢 Sync: Automática<br>🗺 Mapa: Google Hybrid</div>';
    panel.innerHTML=h;
  }
}

/* ══════════ MOBILE NAV ══════════ */
function toggleMobNav(){
  const nav=document.getElementById('sidebar-nav');const ov=document.getElementById('mob-nav-overlay');
  if(!nav)return;
  const open=nav.classList.toggle('mob-open');
  if(ov)ov.classList.toggle('open',open);
}

/* ══════════ CONTACTS ══════════ */
let utilContacts=[],editingContactId=null;
async function loadContacts(){
  try{const r=await fetch(SUPABASE_URL+'/rest/v1/utility_contacts?select=*&order=utility_name',{headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY}});if(r.ok)utilContacts=await r.json();else utilContacts=[];console.log('[Contacts]',utilContacts.length,'utilities com contatos');}catch(e){console.error('Contacts load error:',e);utilContacts=[];}
}
function renderContacts(){
  const grid=document.getElementById('contacts-grid');if(!grid)return;
  const sr=(document.getElementById('contacts-search')?.value||'').toLowerCase();
  const sf=document.getElementById('contacts-state-filter')?.value||'';
  let f=utilContacts.filter(c=>{if(sf&&(c.state||'')!==sf)return false;if(sr&&!(c.utility_name||'').toLowerCase().includes(sr)&&!(c.phone_main||'').includes(sr))return false;return true;});
  if(!f.length){grid.innerHTML='<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center">Nenhum contato encontrado.'+(utilContacts.length===0?' Execute <code>python 811_sync.py --contacts --state FL</code> para importar.':'')+'</div>';return;}
  // Agrupa por utility
  const byUtil={};
  for(const c of f){const key=c.utility_name||'?';if(!byUtil[key])byUtil[key]=[];byUtil[key].push(c);}
  grid.innerHTML=Object.entries(byUtil).map(([util,contacts])=>{
    return'<div class="contact-card"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px"><div class="cc-name">'+util+'</div><span style="font-size:11px;padding:2px 8px;border-radius:10px;background:var(--bg);color:var(--muted);border:1px solid var(--border);font-family:var(--mono)">'+(contacts[0].state||'—')+'</span></div>'+contacts.map(c=>{
      const name=c.contact_name||'';
      const phones=[];
      if(c.phone_main)phones.push('<span class="cc-tag cc-tag-main">Principal</span> <a href="tel:'+c.phone_main+'">'+c.phone_main+'</a>');
      if(c.phone_alt)phones.push('<span class="cc-tag cc-tag-alt">Alt.</span> <a href="tel:'+c.phone_alt+'">'+c.phone_alt+'</a>');
      if(c.phone_emergency)phones.push('<span class="cc-tag cc-tag-emerg">Emerg.</span> <a href="tel:'+c.phone_emergency+'">'+c.phone_emergency+'</a>');
      return'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-top:1px solid var(--border)">'+(name?'<span style="font-size:12px;font-weight:600;color:var(--text2);min-width:120px">'+name+'</span>':'')+'<div class="cc-phones" style="margin:0;gap:10px">'+phones.join(' ')+'</div>'+(isAdmin?'<div style="display:flex;gap:4px;margin-left:8px"><button class="btn btn-sm" onclick="editContact('+c.id+')" style="font-size:10px;padding:2px 6px">✏</button><button class="btn btn-sm btn-danger" onclick="deleteContact('+c.id+')" style="font-size:10px;padding:2px 6px">×</button></div>':'')+'</div>';
    }).join('')+(contacts.some(x=>x.notes)?'<div class="cc-meta" style="margin-top:6px">'+contacts[0].notes+'</div>':'')+'</div>';
  }).join('');
}
function openContactModal(id){
  editingContactId=id||null;
  document.getElementById('contact-modal-title').textContent=id?'Editar contato':'Novo contato';
  if(id){const c=utilContacts.find(x=>x.id===id);if(c){document.getElementById('ct-utility').value=c.utility_name||'';document.getElementById('ct-name').value=c.contact_name||'';document.getElementById('ct-state').value=c.state||'FL';document.getElementById('ct-ticket').value=c.ticket_ref||'';document.getElementById('ct-phone1').value=c.phone_main||'';document.getElementById('ct-phone2').value=c.phone_alt||'';document.getElementById('ct-phone3').value=c.phone_emergency||'';document.getElementById('ct-notes').value=c.notes||'';}}
  else{['ct-utility','ct-name','ct-ticket','ct-phone1','ct-phone2','ct-phone3','ct-notes'].forEach(id=>document.getElementById(id).value='');document.getElementById('ct-state').value='FL';}
  openModal('ov-contact');
}
function editContact(id){openContactModal(id);}
async function saveContact(){
  const name=document.getElementById('ct-utility').value.trim();if(!name){toast('Preencha o nome da utility.','danger');return;}
  const data={utility_name:name,contact_name:document.getElementById('ct-name').value.trim(),state:document.getElementById('ct-state').value,ticket_ref:document.getElementById('ct-ticket').value.trim(),phone_main:document.getElementById('ct-phone1').value.trim(),phone_alt:document.getElementById('ct-phone2').value.trim(),phone_emergency:document.getElementById('ct-phone3').value.trim(),notes:document.getElementById('ct-notes').value.trim()};
  try{let res;if(editingContactId){res=await fetch(SUPABASE_URL+'/rest/v1/utility_contacts?id=eq.'+editingContactId,{method:'PATCH',headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY,'Content-Type':'application/json','Prefer':'return=minimal'},body:JSON.stringify(data)});}else{res=await fetch(SUPABASE_URL+'/rest/v1/utility_contacts',{method:'POST',headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY,'Content-Type':'application/json','Prefer':'return=minimal'},body:JSON.stringify(data)});}
  if(!res.ok)throw new Error('HTTP '+res.status);await loadContacts();renderContacts();closeModal('ov-contact');toast('Contato salvo!','success');}catch(e){toast('Erro ao salvar: '+e.message,'danger');}
}
async function deleteContact(id){
  if(!confirm('Excluir este contato?'))return;
  try{await fetch(SUPABASE_URL+'/rest/v1/utility_contacts?id=eq.'+id,{method:'DELETE',headers:{apikey:SUPABASE_KEY,Authorization:'Bearer '+SUPABASE_KEY}});await loadContacts();renderContacts();toast('Contato excluído','success');}catch(e){toast('Erro: '+e.message,'danger');}
}
function exportContacts(){
  if(!utilContacts.length){toast('Nenhum contato.','warn');return;}
  const wb=XLSX.utils.book_new();const data=[['Utility','Nome Contato','Estado','Tel. Principal','Tel. Alternativo','Tel. Emergência','Ticket Ref','Notas'],...utilContacts.map(c=>[c.utility_name,c.contact_name||'',c.state,c.phone_main||'',c.phone_alt||'',c.phone_emergency||'',c.ticket_ref||'',c.notes||''])];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Contatos');XLSX.writeFile(wb,'OneDrill_Contatos_'+new Date().toISOString().slice(0,10)+'.xlsx');toast('Contatos exportados!','success');
}

window.addEventListener('load',async()=>{
  // Network detection
  window.addEventListener('offline',()=>{toast('⚠ Sem conexão — alterações não serão salvas','danger');setSyncStatus(false,'Offline');});
  window.addEventListener('online',()=>{toast('✅ Conexão restaurada','success');setSyncStatus(true,'Online');});
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
      try{const{data:roleData}=await sb.from('app_roles').select('role').eq('user_id',session.user.id).single();isAdmin=!!(roleData&&roleData.role==='admin');}catch(e){isAdmin=false;}
      role=isAdmin?'admin':'viewer';
      enterApp();
      return;
    }
  }catch(e){console.log('[Auth] No session:',e);}
  document.getElementById('login-screen').style.display='flex';
});

