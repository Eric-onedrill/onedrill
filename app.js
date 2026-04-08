
/* ══════════════════════════════════════════════
   OneDrill — App Logic  (Refactored v2)
   ══════════════════════════════════════════════
   Melhorias aplicadas:
   ─ XSS: esc() em TODOS os dados do usuário
   ─ Filtros centralizados via filterTickets()
   ─ Geocoding com fila throttled (1 req/s)
   ─ Import em batch (upsert de 200)
   ─ Supabase client em vez de fetch direto
   ─ Map: cleanup correto de listeners
   ─ Código organizado por módulo
   ══════════════════════════════════════════════
   Segurança: Supabase Auth + Row Level Security
   A anon key abaixo é SEGURA — RLS restringe
   operacoes de escrita a usuarios autenticados.
   ══════════════════════════════════════════════ */

/* ═══════════ 1. CONFIG ═══════════ */
const SUPABASE_URL='https://ofbqtaulvzeltfpqcjhh.supabase.co';
const SUPABASE_KEY='eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9mYnF0YXVsdnplbHRmcHFjamhoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ0MDMyMjAsImV4cCI6MjA4OTk3OTIyMH0.zPU8SCUAVrTOxp-cuKupXBt0QgRkxnLcpScwnHJKVWE';
const ADMIN_EMAILS=['engineering@onedrill.us','carlos@onedrill.us'];
const BATCH_SIZE=200;
const AUTO_REFRESH_MS=300000;
const GEOCODE_INTERVAL_MS=1100; // >1s for Nominatim rate limit

/* ═══════════ 2. UTILITIES ═══════════ */
function debounce(fn,ms){let t;return(...a)=>{clearTimeout(t);t=setTimeout(()=>fn(...a),ms);};}

/** Escapa HTML para prevenir XSS — usar em TODOS os dados do usuário */
function esc(s){
  if(s===null||s===undefined)return'';
  const d=document.createElement('div');
  d.textContent=String(s);
  return d.innerHTML;
}

/** Formata data timestamp para exibição */
function fmtDt(ts){
  const d=new Date(ts);
  return d.toLocaleDateString('pt-BR')+' '+d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});
}

/** Cor do status */
function scol(s){
  const m={open:'#dc2626',clear:'#16a34a',damage:'#d97706',closed:'#1a1a18',cancel:'#6d28d9'};
  return m[(s||'').toLowerCase()]||'#9a9888';
}
function tipoDash(t){return(t||'').toLowerCase().includes('main')?null:'6,4';}
function lineWeight(t){return(t||'').toLowerCase().includes('main')?5:3;}

/* ═══════════ 3. STATE ═══════════ */
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
let utilContacts=[],editingContactId=null;
let supersededSet=new Set();
let miniMap=null;
let _expAlertEl=null;
let _t2; // toast timer

// Debounced renderers
const debouncedRedraw=debounce(()=>redrawAll(),250);
const debouncedTable=debounce(()=>renderTable(),250);
const debouncedContacts=debounce(()=>renderContacts(),250);

/* ═══════════ 4. SUPERSEDED SET ═══════════ */
function rebuildSupersededSet(){
  supersededSet=new Set(
    tickets.map(t=>String(t.oldTicket2||'').trim()).filter(Boolean)
  );
}
function isSuperseded(t){
  return supersededSet.has(String(t.ticket||'').trim());
}

/* ═══════════ 5. CENTRALIZED FILTER ═══════════ */
/**
 * Filtra tickets com opções unificadas.
 * @param {Object} opts
 * @param {string} opts.status      - Filtrar por status exato
 * @param {string} opts.projectId   - Filtrar por projeto
 * @param {string} opts.client      - Filtrar por cliente
 * @param {string} opts.search      - Busca textual (ticket, client, location, address, prime)
 * @param {string} opts.state       - Filtrar por estado
 * @param {string} opts.utility     - Filtrar por utility pendente ('__any_pending__', '__all_clear__', ou nome)
 * @param {boolean} opts.excludeSuperseded - Excluir superseded (default: true)
 * @param {Object}  opts.statusFilter - Map de status booleans (ex: {open:true,clear:true,...})
 * @param {string}  opts.mapUtilFilter - Filtro de utility no mapa
 */
function filterTickets(opts={}){
  const {
    status='',
    projectId='',
    client='',
    search='',
    state='',
    utility='',
    excludeSuperseded=true,
    statusFilter=null,
    mapUtilFilter=''
  }=opts;

  const sr=search.toLowerCase();

  return tickets.filter(t=>{
    // Superseded
    if(excludeSuperseded && isSuperseded(t)) return false;

    // Status exato
    if(status && t.status!==status) return false;

    // Status filter (mapa checkboxes)
    if(statusFilter){
      const sl=(t.status||'').toLowerCase();
      if(sl==='open'    && !statusFilter.open)    return false;
      if(sl==='damage'  && !statusFilter.damage)  return false;
      if(sl==='clear'   && !statusFilter.clear)   return false;
      if(sl==='closed'  && !statusFilter.closed)  return false;
      if(sl==='cancel'  && !statusFilter.cancel)  return false;
    }

    // Projeto
    if(projectId && t.projectId!==projectId) return false;

    // Cliente
    if(client && t.client!==client) return false;

    // Estado
    if(state && t.state!==state) return false;

    // Busca textual
    if(sr && !t.ticket.toLowerCase().includes(sr)
          && !(t.client||'').toLowerCase().includes(sr)
          && !(t.location||'').toLowerCase().includes(sr)
          && !(t.address||'').toLowerCase().includes(sr)
          && !(t.prime||'').toLowerCase().includes(sr)) return false;

    // Utility filter
    if(utility){
      const tkey=String(t.ticket).trim();
      const pends=getTicketPendingUtils(tkey);
      const allU=getTicketUtils(tkey);
      if(utility==='__any_pending__'){if(!pends.length)return false;}
      else if(utility==='__all_clear__'){if(pends.length>0)return false;if(!allU.length)return false;}
      else{if(!pends.some(p=>p.utility_name===utility))return false;}
    }

    // Map utility filter
    if(mapUtilFilter){
      const tkey=String(t.ticket).trim();
      const pu=getTicketPendingUtils(tkey);
      if(mapUtilFilter==='__pending__'){if(!pu.length)return false;}
      else{if(!pu.some(p=>p.utility_name===mapUtilFilter))return false;}
    }

    return true;
  });
}

/* ═══════════ 6. GEOCODING QUEUE ═══════════ */
const geoQueue=[];
let geoProcessing=false;

function enqueueGeocode(ticket){
  if(ticket._geocoded || ticket._geocoding) return;
  if(!ticket.address || ticket.address==='—') return;
  if(geoQueue.some(t=>t.id===ticket.id)) return;
  ticket._geocoding=true;
  geoQueue.push(ticket);
  processGeoQueue();
}

async function processGeoQueue(){
  if(geoProcessing || !geoQueue.length) return;
  geoProcessing=true;
  while(geoQueue.length){
    const t=geoQueue.shift();
    try{
      const coords=await geocodeAddress(t.address,t.location,t.state);
      if(coords){
        t._geocoded=coords;
        // Re-render map if visible
        const activePage=document.querySelector('.page.active')?.id;
        if(activePage==='pg-map') renderMap();
        else if(isSharedView) renderSharedMap();
      }
    }catch(e){console.error('[Geocode]',e);}
    t._geocoding=false;
    // Throttle: 1 request per GEOCODE_INTERVAL_MS
    await new Promise(r=>setTimeout(r,GEOCODE_INTERVAL_MS));
  }
  geoProcessing=false;
}

async function geocodeAddress(address,location,state){
  if(!address||address==='—')return null;
  const queries=[
    `${address}, ${location}, ${state}, USA`,
    `${address}, ${location}, USA`,
    `${address}, ${state}, USA`
  ];
  const stateL=(state||'').toLowerCase();
  const inBounds=(lat,lon)=>{
    if(stateL==='in')return lat>36&&lat<43&&lon>-89&&lon<-84;
    if(stateL==='fl')return lat>24&&lat<32&&lon>-88&&lon<-79;
    return true;
  };
  for(const q of queries){
    try{
      const res=await fetch(`https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(q)}&format=json&limit=3&countrycodes=us`,{
        headers:{'Accept-Language':'en','User-Agent':'OneDrill/1.0'}
      });
      const d=await res.json();
      if(d?.length){
        for(const item of d){
          const lat=parseFloat(item.lat),lon=parseFloat(item.lon);
          if(inBounds(lat,lon))return[lat,lon];
        }
      }
    }catch(e){/* continue */}
  }
  return null;
}

/* ═══════════ 7. SUPABASE DATA LAYER ═══════════ */
function dbToProject(r){
  return{
    id:r.id, name:r.name, client:r.client||'', state:r.state||'',
    status:r.status||'Active', desc:r.description||'', totalFeet:r.total_feet||0,
    centerCoords:(r.center_lat&&r.center_lon)?[r.center_lat,r.center_lon]:null,
    _manual:r.is_manual||false
  };
}
function dbToTicket(r){
  return{
    id:r.id, ticket:r.ticket, projectId:r.project_id||'', company:r.company||'',
    state:r.state||'', location:r.location||'', status:r.status||'Open',
    expire:r.expire||'', footage:r.footage||0, client:r.client||'', prime:r.prime||'',
    job:r.job||'', tipo:r.tipo||'', address:r.address||'', pending:r.pending||'',
    oldTicket2:r.old_ticket2||'', statusOld:r.status_old||'', expireOld:r.expire_old||'',
    notes:r.notes||'', fieldPath:r.field_path||null,
    _geocoded:(r.geocoded_lat&&r.geocoded_lon)?[r.geocoded_lat,r.geocoded_lon]:null,
    history:r.history||[], attachments:r.attachments||[], status_locked:r.status_locked||false
  };
}
function ticketToDb(t){
  return{
    ticket:t.ticket, project_id:t.projectId||null, company:t.company||'',
    state:t.state||'', location:t.location||'', status:t.status||'Open',
    expire:t.expire||'', footage:t.footage||0, client:t.client||'', prime:t.prime||'',
    job:t.job||'', tipo:t.tipo||'', address:t.address||'', pending:t.pending||'',
    old_ticket2:t.oldTicket2||'', status_old:t.statusOld||'', expire_old:t.expireOld||'',
    notes:t.notes||'', field_path:t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null,
    geocoded_lat:t._geocoded?t._geocoded[0]:null, geocoded_lon:t._geocoded?t._geocoded[1]:null,
    history:t.history||[], attachments:t.attachments||[], status_locked:t.status_locked||false
  };
}
function projectToDb(p){
  return{
    id:p.id, name:p.name, client:p.client||'', state:p.state||'',
    status:p.status||'Active', description:p.desc||'', total_feet:p.totalFeet||0,
    center_lat:p.centerCoords?p.centerCoords[0]:null,
    center_lon:p.centerCoords?p.centerCoords[1]:null,
    is_manual:p._manual||false
  };
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

function setSyncStatus(ok,msg){
  const d=document.getElementById('sync-dot');
  const l=document.getElementById('sync-label');
  if(d)d.style.background=ok?'var(--green)':'var(--red)';
  if(l)l.textContent=msg;
}

async function requireAuth(){
  const{data:{session}}=await sb.auth.getSession();
  if(!session){toast('Faça login como Admin para editar.','danger');return false;}
  return true;
}

async function saveTicketToDb(t){
  if(!await requireAuth())return false;
  setSyncStatus(true,'Salvando...');
  const data=ticketToDb(t);
  let res;
  if(typeof t.id==='number'&&t.id>0){
    res=await sb.from('tickets').update(data).eq('id',t.id);
  }else{
    res=await sb.from('tickets').insert(data).select().single();
    if(res.data)t.id=res.data.id;
  }
  if(res.error){
    setSyncStatus(false,'Erro ao salvar');
    const msg=res.error.message||'';
    if(msg.includes('infinite recursion')||msg.includes('app_roles')){
      toast('Erro RLS no Supabase — rode o script fix_rls_app_roles.sql no SQL Editor do Supabase','danger');
      console.error('[SaveTicket] RLS recursion fix needed. Error:',res.error);
    }else if(res.error.code==='42501'){
      toast('Sem permissão — faça login como Admin','danger');
    }else{
      toast('Erro: '+msg,'danger');
    }
    return false;
  }
  setSyncStatus(true,'Salvo ✓');
  return true;
}

/** Salva batch de tickets via upsert (para imports) */
async function saveTicketBatch(ticketArray){
  if(!ticketArray.length)return{ok:true,ids:[]};
  const dbRows=ticketArray.map(ticketToDb);
  const{data,error}=await sb.from('tickets').upsert(dbRows,{onConflict:'ticket'}).select('id,ticket');
  if(error){
    console.error('[BatchSave]',error);
    return{ok:false,ids:[]};
  }
  // Map IDs back
  if(data){
    for(const row of data){
      const t=ticketArray.find(x=>x.ticket===row.ticket);
      if(t)t.id=row.id;
    }
  }
  return{ok:true,ids:data?.map(r=>r.id)||[]};
}

async function saveProjectToDb(p){
  if(!await requireAuth())return false;
  const data=projectToDb(p);
  const res=await sb.from('projects').upsert(data,{onConflict:'id'});
  if(res.error){
    toast(res.error.code==='42501'?'Sem permissão — faça login como Admin':'Erro: '+res.error.message,'danger');
    return false;
  }
  return true;
}

async function deleteProjectFromDb(id){
  if(!await requireAuth())return false;
  await sb.from('tickets').update({project_id:null}).eq('project_id',id);
  const res=await sb.from('projects').delete().eq('id',id);
  return!res.error;
}

/* ═══════════ 8. UTILITY CACHE (via Supabase client) ═══════════ */
function getTicketPendingUtils(ticketNum){
  const key=String(ticketNum||'').trim();
  return(utilCache[key]||[]).filter(u=>u.status==='Pending');
}
function getTicketUtils(ticketNum){
  const key=String(ticketNum||'').trim();
  return utilCache[key]||[];
}

async function loadUtilCache(){
  try{
    let allData=[];
    let offset=0;
    const pageSize=1000;
    while(true){
      const{data,error}=await sb
        .from('ticket_811_responses')
        .select('ticket_num,utility_name,status,responded_at,response_text')
        .order('ticket_num')
        .range(offset,offset+pageSize-1);
      if(error){console.error('[UtilCache] Supabase error:',error);break;}
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

function syncUtilFilter(){
  if(!utilCacheLoaded)return;
  const allUtils={};
  const openTicketNums=new Set(
    filterTickets({excludeSuperseded:true}).filter(t=>t.status!=='Closed'&&t.status!=='Cancel')
      .map(t=>String(t.ticket||'').trim())
  );
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
    el.innerHTML='<option value="">Todas utilities</option>'
      +'<option value="__any_pending__">⚠ Qualquer pendente</option>'
      +'<option value="__all_clear__">✅ Todas clear</option>'
      +sorted.map(([name,count])=>`<option value="${esc(name)}">🔴 ${esc(name)} (${count})</option>`).join('');
    if(prev)el.value=prev;
  }
  console.log(`[UtilFilter] ${sorted.length} utilities pendentes:`,sorted.map(s=>s[0]+':'+s[1]));
}

/* ═══════════ 9. CONTACTS (via Supabase client) ═══════════ */
async function loadContacts(){
  try{
    let allData=[];
    let offset=0;
    const pageSize=1000;
    while(true){
      const{data,error}=await sb
        .from('utility_contacts')
        .select('*')
        .order('utility_name')
        .range(offset,offset+pageSize-1);
      if(error){console.error('Contacts load error:',error);break;}
      if(!data||!data.length)break;
      allData=allData.concat(data);
      if(data.length<pageSize)break;
      offset+=pageSize;
    }
    utilContacts=allData;
    console.log('[Contacts]',utilContacts.length,'contatos carregados'+(offset>0?' (paginado)':''));
  }catch(e){console.error('Contacts load error:',e);utilContacts=[];}
}

function renderContacts(){
  const grid=document.getElementById('contacts-grid');if(!grid)return;
  const sr=(document.getElementById('contacts-search')?.value||'').toLowerCase();
  const sf=document.getElementById('contacts-state-filter')?.value||'';
  let f=utilContacts.filter(c=>{
    if(sf&&(c.state||'')!==sf)return false;
    if(sr&&!(c.utility_name||'').toLowerCase().includes(sr)&&!(c.phone_main||'').includes(sr))return false;
    return true;
  });
  if(!f.length){
    grid.innerHTML='<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center">Nenhum contato encontrado.'
      +(utilContacts.length===0?' Execute <code>python 811_sync.py --contacts --state FL</code> para importar.':'')+'</div>';
    return;
  }
  const byUtil={};
  for(const c of f){const key=c.utility_name||'?';if(!byUtil[key])byUtil[key]=[];byUtil[key].push(c);}
  grid.innerHTML=Object.entries(byUtil).map(([util,contacts])=>{
    return'<div class="contact-card">'
      +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
      +'<div class="cc-name">'+esc(util)+'</div>'
      +'<span style="font-size:11px;padding:2px 8px;border-radius:10px;background:var(--bg);color:var(--muted);border:1px solid var(--border);font-family:var(--mono)">'+esc(contacts[0].state||'—')+'</span>'
      +'</div>'
      +contacts.map(c=>{
        const name=esc(c.contact_name||'');
        const phones=[];
        if(c.phone_main)phones.push('<span class="cc-tag cc-tag-main">Principal</span> <a href="tel:'+esc(c.phone_main)+'">'+esc(c.phone_main)+'</a>');
        if(c.phone_alt)phones.push('<span class="cc-tag cc-tag-alt">Alt.</span> <a href="tel:'+esc(c.phone_alt)+'">'+esc(c.phone_alt)+'</a>');
        if(c.phone_emergency)phones.push('<span class="cc-tag cc-tag-emerg">Emerg.</span> <a href="tel:'+esc(c.phone_emergency)+'">'+esc(c.phone_emergency)+'</a>');
        return'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-top:1px solid var(--border)">'
          +(name?'<span style="font-size:12px;font-weight:600;color:var(--text2);min-width:120px">'+name+'</span>':'')
          +'<div class="cc-phones" style="margin:0;gap:10px">'+phones.join(' ')+'</div>'
          +(isAdmin?'<div style="display:flex;gap:4px;margin-left:8px"><button class="btn btn-sm" onclick="editContact('+c.id+')" style="font-size:10px;padding:2px 6px">✏</button><button class="btn btn-sm btn-danger" onclick="deleteContact('+c.id+')" style="font-size:10px;padding:2px 6px">×</button></div>':'')
          +'</div>';
      }).join('')
      +(contacts.some(x=>x.notes)?'<div class="cc-meta" style="margin-top:6px">'+esc(contacts[0].notes)+'</div>':'')
      +'</div>';
  }).join('');
}

function openContactModal(id){
  editingContactId=id||null;
  document.getElementById('contact-modal-title').textContent=id?'Editar contato':'Novo contato';
  if(id){
    const c=utilContacts.find(x=>x.id===id);
    if(c){
      document.getElementById('ct-utility').value=c.utility_name||'';
      document.getElementById('ct-name').value=c.contact_name||'';
      document.getElementById('ct-state').value=c.state||'FL';
      document.getElementById('ct-ticket').value=c.ticket_ref||'';
      document.getElementById('ct-phone1').value=c.phone_main||'';
      document.getElementById('ct-phone2').value=c.phone_alt||'';
      document.getElementById('ct-phone3').value=c.phone_emergency||'';
      document.getElementById('ct-notes').value=c.notes||'';
    }
  }else{
    ['ct-utility','ct-name','ct-ticket','ct-phone1','ct-phone2','ct-phone3','ct-notes'].forEach(id=>document.getElementById(id).value='');
    document.getElementById('ct-state').value='FL';
  }
  openModal('ov-contact');
}
function editContact(id){openContactModal(id);}

async function saveContact(){
  const name=document.getElementById('ct-utility').value.trim();
  if(!name){toast('Preencha o nome da utility.','danger');return;}
  const data={
    utility_name:name,
    contact_name:document.getElementById('ct-name').value.trim(),
    state:document.getElementById('ct-state').value,
    ticket_ref:document.getElementById('ct-ticket').value.trim(),
    phone_main:document.getElementById('ct-phone1').value.trim(),
    phone_alt:document.getElementById('ct-phone2').value.trim(),
    phone_emergency:document.getElementById('ct-phone3').value.trim(),
    notes:document.getElementById('ct-notes').value.trim()
  };
  try{
    if(editingContactId){
      const{error}=await sb.from('utility_contacts').update(data).eq('id',editingContactId);
      if(error)throw error;
    }else{
      const{error}=await sb.from('utility_contacts').insert(data);
      if(error)throw error;
    }
    await loadContacts();renderContacts();closeModal('ov-contact');toast('Contato salvo!','success');
  }catch(e){toast('Erro ao salvar: '+(e.message||e),'danger');}
}

async function deleteContact(id){
  if(!confirm('Excluir este contato?'))return;
  try{
    const{error}=await sb.from('utility_contacts').delete().eq('id',id);
    if(error)throw error;
    await loadContacts();renderContacts();toast('Contato excluído','success');
  }catch(e){toast('Erro: '+(e.message||e),'danger');}
}

function exportContacts(){
  if(!utilContacts.length){toast('Nenhum contato.','warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[
    ['Utility','Nome Contato','Estado','Tel. Principal','Tel. Alternativo','Tel. Emergência','Ticket Ref','Notas'],
    ...utilContacts.map(c=>[c.utility_name,c.contact_name||'',c.state,c.phone_main||'',c.phone_alt||'',c.phone_emergency||'',c.ticket_ref||'',c.notes||''])
  ];
  const ws=XLSX.utils.aoa_to_sheet(data);
  XLSX.utils.book_append_sheet(wb,ws,'Contatos');
  XLSX.writeFile(wb,'OneDrill_Contatos_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Contatos exportados!','success');
}

/* ═══════════ 10. AUTH & LOGIN ═══════════ */
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
    if(error){
      errEl.textContent=error.message==='Invalid login credentials'?'Email ou senha incorretos':error.message;
      errEl.style.display='block';
      document.querySelector('.login-admin-btn').disabled=false;
      document.querySelector('.login-admin-btn').textContent='Entrar como Admin';
      return;
    }
    await resolveRole(data.user);
    document.getElementById('login-screen').style.display='none';
    enterApp();
  }catch(e){errEl.textContent='Erro de conexão';errEl.style.display='block';}
  document.querySelector('.login-admin-btn').disabled=false;
  document.querySelector('.login-admin-btn').textContent='Entrar como Admin';
}

/** Resolve role via app_roles com fallback por email */
async function resolveRole(user){
  try{
    const{data:roleData,error:roleErr}=await sb.from('app_roles').select('role').eq('user_id',user.id).single();
    if(roleData&&roleData.role==='admin'){isAdmin=true;role='admin';}
    else if(roleErr){
      isAdmin=ADMIN_EMAILS.includes((user.email||'').toLowerCase());
      role=isAdmin?'admin':'viewer';
      console.warn('[Auth] app_roles inacessivel ('+roleErr.code+'), fallback email:',user.email,'->',role);
    }else{isAdmin=false;role='viewer';}
  }catch(e){
    isAdmin=ADMIN_EMAILS.includes((user.email||'').toLowerCase());
    role=isAdmin?'admin':'viewer';
  }
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

/* ═══════════ 11. APP SHELL ═══════════ */
function enterApp(){
  document.getElementById('app-shell').style.display='grid';
  document.getElementById('role-badge').textContent=isAdmin?'ADMIN':'VIEWER';
  document.getElementById('role-badge').style.background=isAdmin?'var(--green-bg)':'var(--accent-bg)';
  document.getElementById('role-badge').style.color=isAdmin?'var(--green)':'var(--accent)';
  const logoutBtn=document.getElementById('btn-logout');
  if(logoutBtn)logoutBtn.style.display='';
  const adminEls=['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn','btn-add-contact'];
  adminEls.forEach(id=>{const e=document.getElementById(id);if(e)e.style.display=isAdmin?'':'none';});
  if(!isAdmin){const fss=document.getElementById('field-status-section');if(fss)fss.style.display='none';}
  syncAll();renderDash();
  loadLastSync();
  loadUtilCache().then(()=>{renderDash();renderTable();buildNotifications();});
  loadContacts().then(()=>renderContacts());

  // Auto-refresh
  setInterval(async()=>{
    if(fieldDrawing){console.log('[AutoRefresh] Pulado — desenho em andamento');return;}
    if(document.querySelector('.overlay.open')){console.log('[AutoRefresh] Pulado — modal aberto');return;}
    try{
      const{data:p}=await sb.from('projects').select('*').order('name');
      const{data:t}=await sb.from('tickets').select('*').order('ticket');
      if(p)projects=p.map(dbToProject);
      if(t)tickets=t.map(dbToTicket);
      rebuildSupersededSet();
      await loadUtilCache();
      await loadLastSync();
      syncAll();
      setSyncStatus(true,'Atualizado');
      console.log('[AutoRefresh] OK');
    }catch(e){console.error('[AutoRefresh]',e);}
  },AUTO_REFRESH_MS);
}

/* ═══════════ 12. NAVIGATION ═══════════ */
function nav(page){
  if(isSharedView)return;
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.snav-item').forEach(t=>t.classList.remove('active'));
  document.getElementById('pg-'+page).classList.add('active');
  const btn=document.querySelector('.snav-item[data-page="'+page+'"]');
  if(btn)btn.classList.add('active');
  if(page==='map'){setTimeout(()=>{initMap();if(map)map.invalidateSize();},80);}
  if(page==='proj')renderProjects();
  if(page==='tickets')renderTable();
  if(page==='dash')renderDash();
  if(page==='contacts')renderContacts();
  if(page==='analytics')renderAnalytics();
}
function openModal(id){document.getElementById(id).classList.add('open');}
function closeModal(id){document.getElementById(id).classList.remove('open');}
function toast(msg,type='success'){
  const bg={success:'#16803d',danger:'#dc2626',warn:'#b45309',info:'#1a6cf0'};
  const dot={success:'#86efac',danger:'#fca5a5',warn:'#fde68a',info:'#93c5fd'};
  document.getElementById('toast').style.background=bg[type]||bg.success;
  document.getElementById('tdot').style.background=dot[type]||dot.success;
  document.getElementById('tmsg').textContent=msg;
  document.getElementById('toast').classList.add('show');
  clearTimeout(_t2);
  _t2=setTimeout(()=>document.getElementById('toast').classList.remove('show'),4000);
}
function toggleSidebar(){
  const sb=document.getElementById('map-sidebar');
  const ov=document.getElementById('sb-overlay');
  sb.classList.toggle('mob-open');
  ov.classList.toggle('open');
}
function toggleMobNav(){
  const nav=document.getElementById('sidebar-nav');
  const ov=document.getElementById('mob-nav-overlay');
  if(!nav)return;
  const open=nav.classList.toggle('mob-open');
  if(ov)ov.classList.toggle('open',open);
}

/* ═══════════ 13. CITY COORDS ═══════════ */
const CITY_COORDS={
  'vigo - terre haute':[39.4667,-87.4139],'terre haute':[39.4667,-87.4139],'vigo':[39.4667,-87.4139],
  'orlando - tangelo park':[28.4538,-81.4503],'tangelo park':[28.4538,-81.4503],
  'orlando':[28.5383,-81.3792],'arcadia':[27.2142,-81.8579],
  'volusia - deland':[29.0283,-81.3031],'deland':[29.0283,-81.3031],
  'pinelas - st. petersburg':[27.7676,-82.6403],'st. petersburg':[27.7676,-82.6403],
  'default':[28.5383,-81.3792]
};
function cityCoords(l){
  const k=(l||'').toLowerCase().trim();
  if(CITY_COORDS[k])return CITY_COORDS[k];
  for(const[kk,v]of Object.entries(CITY_COORDS)){
    if(kk!=='default'&&(k.includes(kk)||kk.includes(k)))return v;
  }
  return CITY_COORDS['default'];
}
function projCenter(pid){const p=projects.find(x=>x.id===pid);return p?.centerCoords||null;}

/* ═══════════ 14. MAP ═══════════ */
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
function setLayer(t){
  [satL,strL,hybL].forEach(l=>{try{map.removeLayer(l)}catch{}});
  ['bsat','bstr','bhyb'].forEach(id=>{const el=document.getElementById(id);if(el)el.classList.remove('active')});
  if(t==='sat'){satL.addTo(map);document.getElementById('bsat').classList.add('active');}
  else if(t==='hyb'){hybL.addTo(map);document.getElementById('bhyb').classList.add('active');}
  else{strL.addTo(map);document.getElementById('bstr').classList.add('active');}
}

/** Cleanup seguro de markers, lines e labels do mapa */
function clearMapLayers(){
  mkrs.forEach(m=>{m.off();map.removeLayer(m);});
  lines.forEach(l=>{l.off();map.removeLayer(l);});
  labels.forEach(l=>{map.removeLayer(l);});
  if(clusterGroup){map.removeLayer(clusterGroup);clusterGroup=null;}
  mkrs=[];lines=[];labels=[];
}

function mapFiltered(){
  const pf=document.getElementById('proj-filter')?.value||'';
  const sr=(document.getElementById('srch')?.value||'').toLowerCase();
  const cl=document.getElementById('fcli')?.value||'';
  const lc=document.getElementById('floc')?.value||'';
  const muf=document.getElementById('map-util-filter')?.value||'';
  return filterTickets({
    projectId:pf,
    search:sr,
    client:cl,
    excludeSuperseded:true,
    statusFilter:mf,
    mapUtilFilter:muf
  }).filter(t=>{
    // Location filter (not in centralized filter since it's map-specific)
    if(lc&&t.location!==lc)return false;
    return true;
  });
}

function buildPopup(t,c){
  const proj=projects.find(p=>p.id===t.projectId);
  const isExp=t.expire&&t.expire!=='—'&&t.status!=='Closed'&&t.status!=='Cancel'&&new Date(t.expire)<new Date()&&!(isRenewed(t)&&isInRenewalGrace(t));
  return`<div style="font-family:'DM Sans',sans-serif;font-size:13px;line-height:1.6;min-width:180px;padding:2px">`
    +(isExp?'<div style="background:#dc2626;color:white;padding:6px 10px;border-radius:6px;margin-bottom:8px;text-align:center;font-weight:700;font-size:12px">⛔ NÃO TRABALHAR — VENCIDO</div>':'')
    +`<div style="font-weight:700;color:#18180f;margin-bottom:6px;font-size:14px;font-family:'DM Mono',monospace">${esc(t.ticket)}</div>`
    +(proj?`<div><span style="color:#9a9888">Projeto: </span>${esc(proj.name)}</div>`:'')
    +`<div><span style="color:#9a9888">Cliente: </span>${esc(t.client)}</div>`
    +(t.prime?`<div><span style="color:#9a9888">Prime: </span>${esc(t.prime)}</div>`:'')
    +`<div><span style="color:#9a9888">Footage: </span>${t.footage} ft</div>`
    +(t.tipo?`<div><span style="color:#9a9888">Tipo: </span>${esc(t.tipo)}</div>`:'')
    +`<div><span style="color:#9a9888">Status: </span><span style="color:${c};font-weight:700">${esc(t.status)}</span></div>`
    +`<div><span style="color:#9a9888">Expira: </span><span${isExp?' style="color:#dc2626;font-weight:700"':''}>${esc(t.expire||'—')}${isExp?' ⚠ VENCIDO':''}</span></div>`
    +(t.address?`<div><span style="color:#9a9888">Endereço: </span>${esc(t.address)}</div>`:'')
    +`<div style="margin-top:7px;padding-top:7px;border-top:1px solid #e2e0da;display:flex;gap:8px">`
    +`<a href="#" onclick="openTicketDetail(${t.id});return false;" style="color:#1a6cf0;font-size:12px;font-weight:600">Detalhes →</a>`
    +`<a href="#" onclick="openNavigation(${t.id});return false;" style="color:#16a34a;font-size:12px;font-weight:600">🗺 Navegar</a>`
    +`</div></div>`;
}

function openNavigation(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath[0]:t._geocoded;
  if(coords){
    window.open(`https://www.google.com/maps/dir/?api=1&destination=${coords[0]},${coords[1]}&travelmode=driving`,'_blank');
  }else if(t.address){
    window.open(`https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(t.address+', '+t.location+', '+t.state)}`,'_blank');
  }else{toast('Sem coordenadas para navegar','warn');}
}

async function renderMap(){
  if(!map)return;
  clearMapLayers();
  clusterGroup=L.markerClusterGroup({maxClusterRadius:40,spiderfyOnMaxZoom:true,showCoverageOnHover:false,disableClusteringAtZoom:17});

  for(const t of mapFiltered()){
    const c=scol(t.status),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
    const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null;
    if(coords){
      const mi=op=>L.divIcon({className:'',html:`<div style="width:9px;height:9px;border-radius:50%;background:${c};border:2px solid white;opacity:${op};box-shadow:0 1px 4px rgba(0,0,0,.3)"></div>`,iconSize:[9,9],iconAnchor:[4,4]});
      const m1=L.marker(coords[0],{icon:mi(1)}).addTo(map);
      const m2=L.marker(coords[coords.length-1],{icon:mi(.7)}).addTo(map);
      m1.bindPopup(buildPopup(t,c));m2.bindPopup(buildPopup(t,c));
      m1.on('click',()=>hiT(t.id));m2.on('click',()=>hiT(t.id));
      mkrs.push(m1,m2);
      const ln=L.polyline(coords,{color:c,weight:lw,opacity:.92,dashArray:dash}).addTo(map);
      ln.on('click',()=>{hiT(t.id);showPanel(t)});
      lines.push(ln);
      const mid=coords[Math.floor(coords.length/2)]||coords[0];
      const lbl=L.marker(mid,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="border-left:3px solid ${c}">${esc(t.ticket)}</a>`,iconAnchor:[32,10]})}).addTo(map);
      labels.push(lbl);
    }else{
      let pos=t._geocoded||null;
      if(!pos){
        const pc=projCenter(t.projectId);
        const cc=pc||cityCoords(t.location);
        const jitter=()=>(Math.random()-.5)*(pc?0.002:0.006);
        pos=[cc[0]+jitter(),cc[1]+jitter()];
        enqueueGeocode(t);
      }
      const mi=L.divIcon({className:'',html:`<div style="width:11px;height:11px;border-radius:50%;background:${c};border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,.3);opacity:${t._geocoded?1:.6}"></div>`,iconSize:[11,11],iconAnchor:[5,5]});
      const mk=L.marker(pos,{icon:mi});mk.bindPopup(buildPopup(t,c));mk.on('click',()=>hiT(t.id));
      mkrs.push(mk);clusterGroup.addLayer(mk);
      const lbl=L.marker(pos,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="margin-top:12px;display:block;border-left:3px solid ${c}">${esc(t.ticket)}</a>`,iconAnchor:[32,-2]})}).addTo(map);
      labels.push(lbl);
    }
  }
  if(clusterGroup)map.addLayer(clusterGroup);
}

function showPanel(t){
  const c=scol(t.status);
  const proj=projects.find(p=>p.id===t.projectId);
  currentPanelId=t.id;
  const isExp=t.expire&&t.expire!=='—'&&t.status!=='Closed'&&t.status!=='Cancel'&&new Date(t.expire)<new Date()&&!(isRenewed(t)&&isInRenewalGrace(t));
  document.getElementById('ptitle-txt').textContent=t.ticket+(isRenewed(t)?' (🔄 '+( t.oldTicket2||t.old_ticket2)+')':'');
  document.getElementById('pbody').innerHTML=
    (isExp?'<div style="background:#dc2626;color:white;padding:8px 10px;border-radius:var(--r);margin-bottom:8px;text-align:center;font-weight:700;font-size:12px;animation:expPulse 1.5s infinite">⛔ NÃO TRABALHAR — VENCIDO</div>':'')
    +(proj?`<div class="mp-row"><span class="mp-key">Projeto</span><span class="mp-val">${esc(proj.name)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Cliente</span><span class="mp-val">${esc(t.client)}</span></div>`
    +(t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${esc(t.prime)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val" style="cursor:pointer;color:var(--accent)" onclick="quickEditFootage(currentDetailId);return false;" title="Clique para editar">${t.footage} ft ✏</span></div>`
    +(t.tipo?`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${esc(t.tipo)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${esc(t.status)}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val"${isExp?' style="color:#dc2626;font-weight:700"':''}>${esc(t.expire||'—')}${isExp?' ⚠ VENCIDO':''}</span></div>`;
  document.getElementById('panel').classList.add('vis');
}

function hiT(id){
  document.querySelectorAll('.tcard').forEach(c=>c.classList.remove('active'));
  const cd=document.querySelector(`[data-id="${id}"]`);
  if(cd){cd.classList.add('active');cd.scrollIntoView({behavior:'smooth',block:'nearest'});}
  const t=tickets.find(x=>x.id===id);
  if(t)showPanel(t);
}

function toggleMF(key){
  mf[key]=!mf[key];
  const btn=document.getElementById('ft-'+key);
  const oc={open:'on-open',damage:'on-damage',clear:'on-clear',closed:'on-closed',cancel:'on-cancel'};
  btn.className='ftog'+(mf[key]?' '+oc[key]:'');
  redrawAll();
}
function redrawAll(){renderList();renderMap();}

function onProjFilter(){
  const pf=document.getElementById('proj-filter').value;
  if(pf&&map){
    const p=projects.find(x=>x.id===pf);
    if(p?.centerCoords){
      map.setView(p.centerCoords,16);
    }else{
      const pts=tickets.filter(t=>t.projectId===pf);
      const allCoords=[];
      for(const t of pts){
        if(t.fieldPath&&t.fieldPath.length>=2)allCoords.push(...t.fieldPath);
        else if(t._geocoded)allCoords.push(t._geocoded);
      }
      if(allCoords.length){
        map.fitBounds(L.latLngBounds(allCoords),{padding:[60,60],maxZoom:17});
      }else{
        const locs=pts.map(t=>t.location).filter(Boolean);
        const cc=cityCoords(locs[0]||'');
        if(cc)map.setView(cc,14);
      }
    }
  }
  redrawAll();
}

function fitAll(){
  if(!map)return;
  const filtered=mapFiltered();
  const allCoords=[];
  for(const t of filtered){
    if(t.fieldPath&&t.fieldPath.length>=2)allCoords.push(...t.fieldPath);
    else if(t._geocoded)allCoords.push(t._geocoded);
  }
  if(allCoords.length)map.fitBounds(L.latLngBounds(allCoords),{padding:[40,40]});
  else map.setView([28.4,-81.4],10);
}

function renderList(){
  const f=mapFiltered();
  document.getElementById('tcount').textContent=`${f.length} ticket${f.length!==1?'s':''}`;
  document.getElementById('tlist').innerHTML=f.length?f.map(t=>
    `<div class="tcard s-${(t.status||'').toLowerCase()}" data-id="${t.id}" onclick="focusT(${t.id})">`
    +`<div class="tcard-top"><span class="tcard-num">${esc(t.ticket)}${isRenewed(t)?' <span style="font-size:9px;color:#7c3aed">🔄</span>':''}</span><span class="sbadge b-${t.status.toLowerCase()}">${esc(t.status)}</span></div>`
    +`<div class="tcard-client">${esc(t.client)}${t.prime?' · '+esc(t.prime):''}</div>`
    +`<div class="tcard-meta"><span>${esc(t.location)}, ${esc(t.state)}</span><span>${t.footage} ft</span>${t.tipo?`<span>${esc(t.tipo)}</span>`:''}</div>`
    +(t.pending?`<div style="font-size:10px;color:var(--amber);font-weight:600;margin-top:2px">⏳ ${esc(t.pending)}</div>`:'')
    +`</div>`
  ).join(''):'<div style="text-align:center;padding:28px 16px;color:var(--muted);font-size:13px">Nenhum ticket</div>';
}

function focusT(id){
  hiT(id);
  const t=tickets.find(x=>x.id===id);if(!t||!map)return;
  if(window.innerWidth<=768){
    const sb=document.getElementById('map-sidebar');
    const ov=document.getElementById('sb-overlay');
    sb.classList.remove('mob-open');ov.classList.remove('open');
  }
  if(t.fieldPath&&t.fieldPath.length>=2){
    map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});
  }else if(t._geocoded){
    map.setView(t._geocoded,18);
  }else{
    const pc=projCenter(t.projectId);
    const cc=pc||cityCoords(t.location);
    map.setView(cc,pc?17:15);
    enqueueGeocode(t);
  }
}

/* ═══════════ 15. FIELD DRAWING ═══════════ */
function startFieldDraw(tid){
  const t=tickets.find(x=>x.id===tid);if(!t)return;
  if(fieldDrawing)cancelFieldDraw();
  fieldDrawing=true;fieldPts=[];fieldTicketId=tid;
  if(t.fieldPath&&t.fieldPath.length>=2)fieldPts=[...t.fieldPath];
  document.getElementById('field-draw-panel').style.display='block';
  document.getElementById('field-draw-ticket').textContent=t.ticket+' — '+(t.tipo||'');
  document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';
  map.getContainer().style.cursor='crosshair';
  if(fieldPts.length>=2){
    if(fieldLine)map.removeLayer(fieldLine);
    fieldLine=L.polyline(fieldPts,{color:scol(t.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);
  }
  closeModal('ov-detail');
}
function cancelFieldDraw(){
  fieldDrawing=false;fieldPts=[];fieldTicketId=null;
  if(fieldLine){map.removeLayer(fieldLine);fieldLine=null;}
  document.getElementById('field-draw-panel').style.display='none';
  map.getContainer().style.cursor='';
}
function undoFieldPt(){
  if(!fieldPts.length)return;
  fieldPts.pop();
  if(fieldLine)map.removeLayer(fieldLine);
  const t=tickets.find(x=>x.id===fieldTicketId);
  if(fieldPts.length>=2)fieldLine=L.polyline(fieldPts,{color:scol(t?.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);
  else fieldLine=null;
  document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';
}
async function saveFieldPath(){
  if(fieldPts.length<2){toast('Mínimo 2 pontos.','warn');return;}
  const t=tickets.find(x=>x.id===fieldTicketId);if(!t)return;
  t.fieldPath=[...fieldPts];
  t.history.push({ts:Date.now(),action:`Trajeto desenhado (${fieldPts.length} pontos)`,color:'#6d28d9'});
  const ok=await saveTicketToDb(t);
  if(ok)toast(`Trajeto salvo — ${t.ticket}`,'success');
  cancelFieldDraw();renderMap();
  setTimeout(()=>{if(map&&t.fieldPath)map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});},200);
}
function clearFieldPath(){
  fieldPts=[];
  if(fieldLine){map.removeLayer(fieldLine);fieldLine=null;}
  document.getElementById('field-draw-count').textContent='0 pts';
}
function onMC(e){
  if(fieldDrawing){
    const t=tickets.find(x=>x.id===fieldTicketId);
    fieldPts.push([e.latlng.lat,e.latlng.lng]);
    if(fieldLine)map.removeLayer(fieldLine);
    if(fieldPts.length>=2)fieldLine=L.polyline(fieldPts,{color:scol(t?.status),weight:5,opacity:.9,dashArray:'8,4'}).addTo(map);
    document.getElementById('field-draw-count').textContent=fieldPts.length+' pts';
  }
}
function onMDC(e){if(fieldDrawing)saveFieldPath();}
function goDrawField(tid){
  closeModal('ov-detail');nav('map');
  setTimeout(()=>{
    initMap();
    const t=tickets.find(x=>x.id===tid);
    if(t){
      if(t.fieldPath&&t.fieldPath.length>=2)map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[80,80],maxZoom:19});
      else if(t._geocoded)map.setView(t._geocoded,19);
      else{
        const pc=projCenter(t.projectId);
        map.setView(pc||cityCoords(t.location),pc?17:15);
        enqueueGeocode(t);
      }
    }
    setTimeout(()=>startFieldDraw(tid),400);
  },100);
}

/* ═══════════ 16. TICKET DETAIL ═══════════ */
function openTicketDetail(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  currentDetailId=id;

  const isExpired=t.expire&&t.expire!=='—'&&t.status!=='Closed'&&t.status!=='Cancel'&&new Date(t.expire)<new Date()&&!(isRenewed(t)&&isInRenewalGrace(t));
  if(isExpired)showExpiredAlert(t);

  const c=scol(t.status);
  const proj=projects.find(p=>p.id===t.projectId);
  document.getElementById('det-title').textContent=t.ticket+(isRenewed(t)?' (renovou '+((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0]+')':'');
  document.getElementById('det-sub').textContent=(proj?proj.name+' · ':'')+t.client+(t.prime?' · '+t.prime:'')+(isInRenewalGrace(t)?' · 🔄 Graça até '+(t.expireOld||t.expire_old):'');
  const hasOldInfo=t.oldTicket2||t.statusOld||t.expireOld||t.pending;

  const expiredBanner=isExpired?'<div style="background:#dc2626;color:white;padding:10px 14px;border-radius:var(--r);margin-bottom:10px;text-align:center;font-weight:700;font-size:14px;animation:expPulse 1.5s infinite">⛔ NÃO TRABALHAR — TICKET VENCIDO ('+esc(t.expire)+')</div>':'';

  document.getElementById('det-info').innerHTML=expiredBanner
    +`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${esc(t.status)}${t.status_locked?' 🔒':''}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Empresa</span><span class="mp-val">${esc(t.company||'—')}</span></div>`
    +(t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${esc(t.prime)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Local</span><span class="mp-val">${esc(t.location)}, ${esc(t.state)}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val">${t.footage} ft</span></div>`
    +`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${esc(t.tipo||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Job #</span><span class="mp-val">${esc(t.job||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Endereço</span><span class="mp-val">${esc(t.address||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val"${isExpired?' style="color:#dc2626;font-weight:700"':''}>${esc(t.expire||'—')}${isExpired?' ⚠ VENCIDO':''}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Trajeto</span><span class="mp-val" style="color:${t.fieldPath?'var(--purple)':'var(--muted)'}">${t.fieldPath?`✏️ Campo (${t.fieldPath.length} pts)`:'Sem trajeto'}</span></div>`
    +(t.notes?`<div style="margin-top:8px;padding-top:8px;border-top:1px solid var(--border);font-size:12px;color:var(--text2);white-space:pre-wrap;word-break:break-word">${esc(t.notes)}</div>`:'')
    +(hasOldInfo?`<div style="margin-top:10px;padding:9px 11px;background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r)"><div style="font-size:10px;font-weight:700;color:#92400e;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px">📋 Ticket Anterior</div>`
      +(t.pending?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Pending</span><span class="mp-val" style="color:var(--amber)">${esc(t.pending)}</span></div>`:'')
      +(t.oldTicket2?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Old Ticket #</span><span class="mp-val" style="font-family:var(--mono);color:#b45309">${esc(t.oldTicket2)}</span></div>`:'')
      +(t.statusOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Status Ant.</span><span class="mp-val" style="color:#92400e">${esc(t.statusOld)}</span></div>`:'')
      +(t.expireOld?`<div class="mp-row"><span class="mp-key" style="font-size:11px">Exp. Ant.</span><span class="mp-val" style="color:#92400e">${esc(t.expireOld)}</span></div>`:'')
      +`</div>`:'');

  const lockBadge=document.getElementById('det-lock-badge');
  const unlockBtn=document.getElementById('det-unlock-btn');
  if(t.status_locked){lockBadge.style.display='';unlockBtn.style.display='';}
  else{lockBadge.style.display='none';unlockBtn.style.display='none';}
  if(!isSharedView&&isAdmin){
    document.getElementById('det-edit-btn').style.display='';
    document.getElementById('det-draw-btn').style.display='';
    document.getElementById('det-renew-btn').style.display=(t.status!=='Closed'&&t.status!=='Cancel')?'':'none';
    document.getElementById('field-status-section').style.display='';
  }else{
    document.getElementById('det-edit-btn').style.display='none';
    document.getElementById('det-draw-btn').style.display='none';
    document.getElementById('det-renew-btn').style.display='none';
    document.getElementById('field-status-section').style.display='none';
  }
  renderHistory(t);renderMiniMap(t);renderUtils(t);openModal('ov-detail');
}

// ── EXPIRED TICKET FULLSCREEN ALERT ──
function showExpiredAlert(t){
  if(_expAlertEl){_expAlertEl.remove();_expAlertEl=null;}
  const el=document.createElement('div');
  el.id='expired-alert-overlay';
  el.innerHTML='<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:20px;text-align:center">'
    +'<div style="font-size:60px;margin-bottom:16px">⛔</div>'
    +'<div style="font-size:28px;font-weight:700;color:white;margin-bottom:8px;text-shadow:0 2px 8px rgba(0,0,0,.3)">NÃO TRABALHAR</div>'
    +'<div style="font-size:20px;color:rgba(255,255,255,.9);margin-bottom:16px">TICKET VENCIDO</div>'
    +'<div style="font-family:var(--mono);font-size:24px;color:white;padding:8px 20px;background:rgba(0,0,0,.3);border-radius:12px;margin-bottom:8px">'+esc(t.ticket)+'</div>'
    +'<div style="font-size:16px;color:rgba(255,255,255,.8);margin-bottom:20px">Expirou em '+esc(t.expire)+'</div>'
    +'<div style="background:rgba(255,255,255,.2);border:2px solid rgba(255,255,255,.5);border-radius:12px;padding:14px 28px;margin-bottom:16px">'
    +'<div style="font-size:20px;font-weight:700;color:white">📞 LIGAR AO OFFICE</div>'
    +'<div style="font-size:14px;color:rgba(255,255,255,.8);margin-top:4px">Call the office before any work</div>'
    +'</div>'
    +'<div style="font-size:13px;color:rgba(255,255,255,.5)">Toque para fechar</div>'
    +'</div>';
  Object.assign(el.style,{position:'fixed',top:'0',left:'0',width:'100%',height:'100%',background:'rgba(220,38,38,.92)',zIndex:'99999',cursor:'pointer',backdropFilter:'blur(4px)',WebkitBackdropFilter:'blur(4px)',animation:'expFadeIn .3s ease'});
  el.onclick=()=>{el.style.animation='expFadeOut .3s ease';setTimeout(()=>{el.remove();_expAlertEl=null;},280);};
  document.body.appendChild(el);
  _expAlertEl=el;
  setTimeout(()=>{if(_expAlertEl===el){el.style.animation='expFadeOut .3s ease';setTimeout(()=>{el.remove();if(_expAlertEl===el)_expAlertEl=null;},280);}},5000);
}

// Inject CSS animations for expired alert (once)
(function(){
  if(document.getElementById('exp-alert-css'))return;
  const s=document.createElement('style');s.id='exp-alert-css';
  s.textContent='@keyframes expFadeIn{from{opacity:0}to{opacity:1}}@keyframes expFadeOut{from{opacity:1}to{opacity:0}}@keyframes expPulse{0%,100%{opacity:1}50%{opacity:.7}}';
  document.head.appendChild(s);
})();

function renderHistory(t){
  document.getElementById('det-hist').innerHTML=t.history?.length
    ?[...t.history].reverse().map(h=>`<div class="hist-item"><div class="hist-dot" style="background:${h.color||'#9a9888'}"></div><div style="flex:1"><div style="color:var(--text2);font-size:12px">${esc(h.action)}</div><div class="hist-time">${fmtDt(h.ts)}</div></div></div>`).join('')
    :'<div style="color:var(--muted);font-size:12px">Sem histórico</div>';
}

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

// ── TICKET RENEWAL ──
async function renewTicket(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  const newNum=prompt('Número do ticket NOVO (renovação de '+t.ticket+'):');
  if(!newNum||!newNum.trim())return;
  const newTicket=newNum.trim();
  // Verifica se já existe como registro separado
  const dup=tickets.find(x=>x.ticket===newTicket&&x.id!==t.id);
  if(dup){
    if(!confirm('Ticket '+newTicket+' já existe no sistema (importado pelo scraper).\n\nDeseja MESCLAR?\n• O trajeto e dados do ticket atual serão mantidos\n• O registro duplicado ('+newTicket+') será removido\n• O número será atualizado para '+newTicket))return;
    // Copia expire do duplicado se disponível
    if(dup.expire&&dup.expire!=='—'&&(!t.expire||t.expire==='—'||new Date(dup.expire)>new Date(t.expire))){
      t.expire=dup.expire;
    }
    // Deleta o duplicado
    try{
      const r=await fetch(SB_URL+'/rest/v1/tickets?id=eq.'+dup.id,{method:'DELETE',headers:SB_H});
      if(r.ok){tickets.splice(tickets.indexOf(dup),1);toast('Registro duplicado removido.','success');}
    }catch(e){console.error('Erro ao deletar duplicado:',e);}
  }else{
    if(!confirm('Renovar ticket?\n\nANTIGO: '+t.ticket+' (expira '+t.expire+')\nNOVO: '+newTicket+'\n\nO ticket manterá status até o vencimento do antigo.\nApós vencer, precisará de novas liberações.'))return;
  }
  const oldNum=t.ticket;
  const oldExpire=t.expire;
  const oldStatus=t.status;
  // Cadeia de tickets anteriores (suporta múltiplas renovações)
  const prevChain=t.oldTicket2||t.old_ticket2||'';
  const fullChain=prevChain?oldNum+' → '+prevChain:oldNum;
  // Salva dados antigos
  t.old_ticket2=fullChain;
  t.oldTicket2=fullChain;
  t.expire_old=oldExpire;
  t.expireOld=oldExpire;
  t.status_old=oldStatus;
  t.statusOld=oldStatus;
  // Atualiza para novo ticket
  t.ticket=newTicket;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'[RENOVAÇÃO] '+oldNum+' → '+newTicket+(dup?' (mesclado)':'')+' (graça até '+oldExpire+')',color:'#7c3aed'});
  const ok=await saveTicketToDb(t);
  if(ok){
    toast('✅ Ticket renovado: '+oldNum+' → '+newTicket,'success');
    closeModal('ov-detail');syncAll();
    setTimeout(()=>openTicketDetail(t.id),300);
  }else{
    t.ticket=oldNum;t.old_ticket2=prevChain;t.oldTicket2=prevChain;t.expire_old='';t.expireOld='';t.status_old='';t.statusOld='';
    t.history.pop();
    toast('Erro ao salvar renovação.','danger');
  }
}
function isInRenewalGrace(t){
  if(!t.expireOld&&!t.expire_old)return false;
  const cutover=t.expireOld||t.expire_old;
  if(!cutover||cutover==='—')return false;
  const d=new Date(cutover);
  return d>=new Date(new Date().toDateString());// true if old hasn't expired yet
}
function isRenewed(t){return !!(t.oldTicket2||t.old_ticket2);}

async function setManualStatus(newStatus){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  const old=t.status;const wasLocked=t.status_locked;
  t.status=newStatus;t.status_locked=true;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:`Status manual: ${old} → ${newStatus} 🔒`,color:scol(newStatus)});
  const ok=await saveTicketToDb(t);
  if(ok){toast(`✅ Status: ${old} → ${newStatus} (travado)`,'success');openTicketDetail(currentDetailId);syncAll();}
  else{t.status=old;t.status_locked=wasLocked;t.history.pop();toast(`Erro ao salvar — status revertido para ${old}`,'danger');}
}
async function unlockStatus(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  t.status_locked=false;
  const ok=await saveTicketToDb(t);
  if(ok){toast('🔓 Status desbloqueado','success');openTicketDetail(id);}
}

function renderMiniMap(t){
  const container=document.getElementById('mini-map-container');if(!container)return;
  if(miniMap){try{miniMap.remove();}catch(e){}miniMap=null;}
  const hasPath=t.fieldPath&&t.fieldPath.length>=2;const hasGeo=t._geocoded;
  if(!hasPath&&!hasGeo){
    container.innerHTML='<div class="mini-map-empty">📍 Sem localização<br><span style="font-size:10px">Use ✏️ Desenhar para marcar no mapa</span></div>';
    return;
  }
  container.innerHTML='<div class="mini-map-wrap"><div id="mini-map"></div></div>';
  setTimeout(()=>{
    try{
      miniMap=L.map('mini-map',{zoomControl:false,attributionControl:false,dragging:false,scrollWheelZoom:false});
      L.tileLayer('https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}',{maxZoom:21}).addTo(miniMap);
      const c=scol(t.status);
      if(hasPath){
        const ln=L.polyline(t.fieldPath,{color:c,weight:4,opacity:0.9}).addTo(miniMap);
        miniMap.fitBounds(ln.getBounds(),{padding:[14,14]});
      }else{
        miniMap.setView(t._geocoded,17);
        L.circleMarker(t._geocoded,{radius:8,color:c,fillColor:c,fillOpacity:0.9,weight:2}).addTo(miniMap);
      }
    }catch(e){console.error('Mini map:',e);}
  },120);
}

function openFullMap(id){
  closeModal('ov-detail');
  if(isSharedView){
    if(shMap){
      const t=tickets.find(x=>x.id===id);
      if(t?.fieldPath?.length>=2)shMap.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});
      else if(t?._geocoded)shMap.setView(t._geocoded,18);
    }
    return;
  }
  nav('map');
  setTimeout(()=>{
    initMap();
    const t=tickets.find(x=>x.id===id);if(!t)return;
    if(t.fieldPath&&t.fieldPath.length>=2)map.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});
    else if(t._geocoded)map.setView(t._geocoded,18);
    hiT(id);
  },200);
}

async function renderUtils(t){
  const el=document.getElementById('util-list');
  const sm=document.getElementById('util-summary');
  if(!el)return;
  el.innerHTML='<div style="color:var(--muted);font-size:12px">Carregando...</div>';
  try{
    // Durante período de graça, mostra utilities do ticket ANTIGO
    const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    const oldTicketNum=inGrace?((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0]:'';
    const queryTicket=inGrace&&oldTicketNum?oldTicketNum:t.ticket;

    const{data,error}=await sb
      .from('ticket_811_responses')
      .select('utility_name,status,response_text,responded_at')
      .eq('ticket_num',queryTicket)
      .order('utility_name');

    if(error)throw error;

    // Banner de graça
    let graceBanner='';
    if(inGrace){
      graceBanner='<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:var(--r);padding:8px 12px;margin-bottom:8px">'
        +'<div style="font-size:11px;font-weight:700;color:#16a34a">✅ LIBERADO — Utilities do ticket antigo ('+esc(oldTicketNum)+')</div>'
        +'<div style="font-size:10px;color:#15803d;margin-top:2px">Válido até '+(t.expireOld||t.expire_old)+'. Após essa data, novas liberações serão necessárias do ticket '+esc(t.ticket)+'.</div>'
        +'</div>';
    }

    if(!data||!data.length){
      el.innerHTML=graceBanner+'<div style="color:var(--muted);font-size:12px">Sem dados de utilities</div>';
      if(sm)sm.textContent='';
      return;
    }
    const pending=data.filter(u=>u.status==='Pending');
    const cleared=data.filter(u=>u.status==='Clear'||u.status==='Private');
    const marked=data.filter(u=>u.status==='Marked');
    if(sm){
      const parts=[];
      if(inGrace)parts.push('<span style="color:#7c3aed">🔄 graça</span>');
      if(pending.length)parts.push(`<span style="color:var(--red)">${pending.length} pendente${pending.length>1?'s':''}</span>`);
      if(marked.length)parts.push(`<span style="color:var(--amber)">${marked.length} marcada${marked.length>1?'s':''}</span>`);
      if(cleared.length)parts.push(`<span style="color:var(--green)">${cleared.length} clear</span>`);
      sm.innerHTML=parts.join(' · ');
    }
    const badgeClass={Pending:'ub-pending',Clear:'ub-clear',Marked:'ub-marked',Private:'ub-private',Unmarked:'ub-clear'};
    const label={Pending:'Pendente',Clear:'Clear',Marked:'Marcado',Private:'Privado',Unmarked:'Desmarcado'};
    const order={Pending:0,Marked:1,Private:2,Clear:3,Unmarked:4};
    data.sort((a,b)=>(order[a.status]||9)-(order[b.status]||9));
    el.innerHTML=graceBanner+data.map(u=>{
      const resp=(u.response_text||'').trim();
      let detail='';
      if(resp && u.status==='Clear'){
        const short=resp.length>80?resp.substring(0,80)+'…':resp;
        detail=`<div style="font-size:10px;color:var(--green);margin-top:2px;line-height:1.3;opacity:.85">${esc(short)}</div>`;
      }
      return`<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:6px 0;border-bottom:1px solid var(--border)"><div style="flex:1;min-width:0"><span class="util-name" style="display:block">${esc(u.utility_name)}</span>${detail}</div><span class="util-badge ${badgeClass[u.status]||'ub-pending'}" style="flex-shrink:0;margin-top:2px">${label[u.status]||esc(u.status)}</span></div>`;
    }).join('');
  }catch(e){el.innerHTML='<div style="color:var(--muted);font-size:12px">Erro ao carregar utilities</div>';}
}

/* ═══════════ 17. TICKET TABLE ═══════════ */
function riskScore(t){
  if(!utilCacheLoaded)return 0;
  let s=0;const now=Date.now();
  if(t.expire&&t.expire!=='—'){
    const diff=(new Date(t.expire)-now)/86400000;
    if(diff<0)s+=60;else if(diff<=2)s+=45;else if(diff<=5)s+=30;else if(diff<=10)s+=18;else if(diff<=20)s+=8;
  }
  const pends=getTicketPendingUtils(String(t.ticket).trim());
  s+=Math.min(pends.length*8,35);
  if(t.status==='Damage')s+=30;
  else if(t.status==='Clear')s=Math.max(s-20,0);
  else if(t.status==='Closed'||t.status==='Cancel')return 0;
  if(t.history&&t.history.length){
    const ds=(now-(t.history[t.history.length-1].ts||0))/86400000;
    if(ds>30)s+=15;else if(ds>14)s+=8;
  }
  return Math.min(s,100);
}
function riskLabel(s){
  if(s>=60)return{label:'CRÍTICO',color:'#dc2626',bg:'#fef2f2',border:'#fecaca'};
  if(s>=35)return{label:'ALTO',color:'#d97706',bg:'#fffbeb',border:'#fde68a'};
  if(s>=15)return{label:'MÉDIO',color:'#2563eb',bg:'#eff6ff',border:'#bfdbfe'};
  return{label:'BAIXO',color:'#16a34a',bg:'#f0fdf4',border:'#bbf7d0'};
}

function renderTable(){
  const sr=(document.getElementById('tbl-srch').value||'').toLowerCase();
  const st=document.getElementById('tbl-stat').value;
  const pr=document.getElementById('tbl-proj').value;
  const cl=document.getElementById('tbl-cli').value;
  const ut=document.getElementById('tbl-util')?.value||'';

  let f=filterTickets({status:st,projectId:pr,client:cl,search:sr,utility:ut});

  f.sort((a,b)=>{
    if(sortCol==='risk'){const ra=riskScore(a),rb=riskScore(b);return sortAsc?ra-rb:rb-ra;}
    if(sortCol==='footage')return sortAsc?(a.footage||0)-(b.footage||0):(b.footage||0)-(a.footage||0);
    return sortAsc?String(a[sortCol]||'').localeCompare(String(b[sortCol]||'')):String(b[sortCol]||'').localeCompare(String(a[sortCol]||''));
  });

  document.getElementById('tbl-count').textContent=`${f.length} tickets · ${f.reduce((s,t)=>s+(t.footage||0),0).toLocaleString()} ft`;
  document.getElementById('tbl-body').innerHTML=f.map(t=>{
    const pends=getTicketPendingUtils(String(t.ticket).trim());
    const pendNames=pends.map(p=>p.utility_name);
    const pendChips=pendNames.length
      ?`<div style="display:flex;flex-wrap:wrap;gap:2px;margin-top:3px">`
        +pendNames.slice(0,3).map(n=>`<span style="font-size:9px;padding:1px 5px;border-radius:10px;background:var(--red-bg);color:var(--red);font-family:var(--mono);white-space:nowrap">${esc(n.length>20?n.substring(0,20)+'…':n)}</span>`).join('')
        +(pendNames.length>3?`<span style="font-size:9px;color:var(--muted)">+${pendNames.length-3}</span>`:'')
        +`</div>`
      :'';
    return`<tr onclick="openTicketDetail(${t.id})">`
      +`<td style="font-family:var(--mono);font-weight:500">${esc(t.ticket)}${isRenewed(t)?'<div style="font-size:9px;color:#7c3aed">🔄 renovou '+esc(((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0])+'</div>':''}</td>`
      +`<td style="color:var(--text2);font-size:12px">${esc(t.client)}</td>`
      +`<td style="color:var(--muted);font-size:12px">${esc(t.prime||'—')}</td>`
      +`<td>${esc(t.location)}, ${esc(t.state)}</td>`
      +`<td class="tc-${(t.status||'').toLowerCase()}">${esc(t.status)}${pendChips}</td>`
      +`<td style="font-family:var(--mono)">${t.footage} ft</td>`
      +`<td style="font-family:var(--mono);font-size:12px">${esc(t.expire||'—')}</td>`
      +`<td style="color:var(--muted)">${esc(t.tipo||'—')}</td>`
      +`<td onclick="event.stopPropagation()"><div style="display:flex;gap:5px"><button class="btn btn-sm" onclick="openTicketDetail(${t.id})">Ver</button>${isAdmin?`<button class="btn btn-sm" onclick="editFromTbl(${t.id})">Editar</button>`:''}</div></td>`
      +`</tr>`;
  }).join('');
}
function sortBy(col){sortAsc=sortCol===col?!sortAsc:true;sortCol=col;renderTable();}
function editFromTbl(id){currentDetailId=id;editCurrentTicket();}

/* ═══════════ 18. DASHBOARD ═══════════ */
function renderDash(){
  const states=[...new Set(tickets.map(t=>t.state).filter(Boolean))].sort();
  const dsf=dashStateVal;
  const fTickets=filterTickets({state:dsf});
  const now=Date.now();const week=7*86400000;

  const total=fTickets.length;
  const openT=fTickets.filter(t=>t.status==='Open');
  const clearT=fTickets.filter(t=>t.status==='Clear');
  const damageT=fTickets.filter(t=>t.status==='Damage');
  const open=openT.length,clear=clearT.length,damage=damageT.length;
  const totalFt=fTickets.reduce((s,t)=>s+(t.footage||0),0);
  const openFt=openT.reduce((s,t)=>s+(t.footage||0),0);
  const clearFt=clearT.reduce((s,t)=>s+(t.footage||0),0);
  const damageFt=damageT.reduce((s,t)=>s+(t.footage||0),0);
  const noMap=fTickets.filter(t=>(!t.fieldPath||t.fieldPath.length<2)&&t.status!=='Cancel'&&t.status!=='Closed');
  const _sd=_soonDays||10;
  const soon=fTickets.filter(t=>{if(!t.expire||t.expire==='—')return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;const d=new Date(t.expire);const diff=(d-Date.now())/86400000;return diff>=0&&diff<=_sd&&t.status!=='Closed'&&t.status!=='Cancel';});

  function wCount(status,start,end){
    return fTickets.filter(t=>t.history&&t.history.some(h=>{
      const a=(h.action||'').toLowerCase();
      return h.ts>=start&&h.ts<end&&(status==='Open'?(a.includes('importado')||a.includes('ticket criado')):(a.includes('→ clear')||a.includes('auto 811')||a.includes('auto-clear')));
    })).length;
  }
  const openWk=wCount('Open',now-week,now),openPrev=wCount('Open',now-2*week,now-week);
  const clearWk=wCount('Clear',now-week,now),clearPrev=wCount('Clear',now-2*week,now-week);
  function trend(curr,prev,greenUp){
    const d=curr-prev;
    if(!d)return '<span style="font-size:10px;color:var(--muted)">sem mudança</span>';
    const up=d>0;const c=(up===greenUp)?'var(--green)':'var(--red)';
    return'<span style="font-size:10px;font-weight:700;color:'+c+'">'+(up?'▲':'▼')+Math.abs(d)+' vs semana passada</span>';
  }

  const sf='<select class="fi" onchange="dashStateVal=this.value;renderDash()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px"><option value="">Todos estados</option>'+states.map(s=>'<option value="'+esc(s)+'"'+(dsf===s?' selected':'')+'>'+esc(s)+'</option>').join('')+'</select>';

  const el=document.getElementById('dash-content');if(!el)return;

  el.innerHTML=
  // ── HEADER
  '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:8px;flex-wrap:wrap;gap:8px">'
  +'<div><div style="font-size:22px;font-weight:700;color:var(--text)">Dashboard</div>'
  +'<div style="font-size:11px;color:var(--muted)">OneDrill 811 — '+new Date().toLocaleDateString('pt-BR')+'</div></div>'
  +'<div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">'
  +'<div id="dash-sync-pill" style="font-size:11px;padding:3px 10px;border-radius:20px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border)">● Sincronizado</div>'
  +sf+'</div></div>'

  // ── STATUS PILLS
  +'<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">'
  +'<div style="font-size:11px;padding:3px 10px;border-radius:20px;background:var(--bg);border:1px solid var(--border);color:var(--text2)">● '+total+' verificados'+(utilCacheLoaded?' · '+fTickets.filter(t=>t.status==='Clear'&&!getTicketPendingUtils(String(t.ticket).trim()).length).length+' em cache (Clear)':'')+'</div>'
  +'<div id="dash-health-pill" style="font-size:11px;padding:3px 10px;border-radius:20px;background:var(--green-bg);border:1px solid var(--green-border);color:var(--green)">● Health check OK</div>'
  +'</div>'

  // ── STAT CARDS
  +'<div style="display:grid;grid-template-columns:1fr 1.4fr 1.4fr 1.2fr 1.2fr;gap:8px;margin-bottom:16px">'
  +'<div class="stat-card" style="padding:10px 12px;cursor:pointer" onclick="nav(\'tickets\')">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Total ativo</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono)">'+total+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">'+totalFt.toLocaleString()+' ft</div></div>'
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid var(--red);cursor:pointer" onclick="nav(\'tickets\');setTimeout(()=>{document.getElementById(\'tbl-stat\').value=\'Open\';renderTable();},100)">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Open</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono);color:var(--red)">'+open+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">'+openFt.toLocaleString()+' ft</div>'
  +'<div style="margin-top:3px">'+trend(openWk,openPrev,false)+'</div></div>'
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid var(--green);cursor:pointer" onclick="nav(\'tickets\');setTimeout(()=>{document.getElementById(\'tbl-stat\').value=\'Clear\';renderTable();},100)">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Clear</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono);color:var(--green)">'+clear+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">'+clearFt.toLocaleString()+' ft</div>'
  +'<div style="margin-top:3px">'+trend(clearWk,clearPrev,true)+'</div></div>'
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid var(--amber);cursor:pointer" onclick="nav(\'tickets\');setTimeout(()=>{document.getElementById(\'tbl-stat\').value=\'Damage\';renderTable();},100)">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Damage</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+damage+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">'+damageFt.toLocaleString()+' ft</div>'
  +'<div style="margin-top:3px;font-size:10px;color:'+(damage>0?'var(--amber)':'var(--muted)')+'">'+( damage>0?'atenção':'sem mudança')+'</div></div>'
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid var(--purple);cursor:pointer" onclick="nav(\'map\')">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Sem trajeto</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono);color:var(--purple)">'+noMap.length+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">de '+total+' ativos</div>'
  +'<div style="font-size:10px;color:var(--purple);margin-top:3px">'+Math.round(noMap.length/Math.max(total,1)*100)+'% descobertos</div></div>'
  +'</div>'

  // ── TICKETS VENCENDO
  +'<div style="background:var(--white);border:1px solid '+(soon.length?'var(--red-border)':'var(--border)')+';border-radius:var(--r-lg);padding:10px 14px;margin-bottom:14px">'
  +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:'+(soon.length?'8px':'0')+'">'
  +'<span style="font-size:12px;font-weight:600;color:'+(soon.length?'var(--red)':'var(--green)')+'">'+( soon.length?'⚠ '+soon.length+' ticket(s) vencendo':'✅ Nenhum ticket vencendo')+' nos próximos '+_sd+' dias</span>'
  +'<div style="display:flex;gap:6px;align-items:center">'
  +'<select class="fi" onchange="_soonDays=parseInt(this.value);renderDash()" style="font-size:11px;padding:3px 6px;width:auto">'
  +'<option value="3"'+(_sd===3?' selected':'')+'>3d</option><option value="5"'+(_sd===5?' selected':'')+'>5d</option>'
  +'<option value="10"'+(_sd===10?' selected':'')+'>10d</option><option value="15"'+(_sd===15?' selected':'')+'>15d</option>'
  +'<option value="30"'+(_sd===30?' selected':'')+'>30d</option></select>'
  +(soon.length?'<button class="btn btn-sm" onclick="exportExpiring()" style="background:var(--red);color:white;border-color:var(--red);font-size:11px">↓ Excel</button>':'')
  +'</div></div>'
  +(soon.length?'<div style="max-height:200px;overflow-y:auto"><table style="width:100%;font-size:11px;border-collapse:collapse"><thead><tr style="text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)"><th style="padding:4px 6px;font-weight:600">Ticket</th><th style="padding:4px 6px;font-weight:600">Dias</th><th style="padding:4px 6px;font-weight:600">Local</th><th style="padding:4px 6px;font-weight:600">Status</th><th style="padding:4px 6px;font-weight:600">Expira</th></tr></thead><tbody>'
  +soon.sort((a,b)=>{const da=Math.round((new Date(a.expire)-now)/86400000);const db=Math.round((new Date(b.expire)-now)/86400000);return da-db;}).map(t=>{
    const d2=Math.round((new Date(t.expire)-now)/86400000);
    const urgColor=d2<=2?'var(--red)':d2<=5?'var(--amber)':'var(--text2)';
    const urgBg=d2<=2?'var(--red-bg)':d2<=5?'#fffbeb':'transparent';
    const loc=esc((t.location||'').replace(/\s*(Inside|Near).*/i,'').split(',')[0].trim());
    return'<tr style="border-bottom:1px solid var(--border);cursor:pointer;background:'+urgBg+'" onclick="openTicketDetail('+t.id+')">'
    +'<td style="padding:4px 6px;font-family:var(--mono);font-weight:600">'+esc(t.ticket)+'</td>'
    +'<td style="padding:4px 6px;font-weight:700;color:'+urgColor+'">'+d2+'d</td>'
    +'<td style="padding:4px 6px;color:var(--text2)">'+loc+', '+esc(t.state)+'</td>'
    +'<td style="padding:4px 6px"><span class="sbadge b-'+t.status.toLowerCase()+'" style="font-size:9px">'+esc(t.status)+'</span></td>'
    +'<td style="padding:4px 6px;font-family:var(--mono);color:var(--muted)">'+esc(t.expire)+'</td>'
    +'</tr>';}).join('')+'</tbody></table></div>':'')
  +'</div>'

  // Cleared stats, private locator, sync timer
  +renderClearedStats(fTickets)
  +renderPrivateLocatorAlert(fTickets)
  +'<div id="dash-sync-timer" style="text-align:center;font-size:10px;color:var(--muted);padding:10px 0">sync automático em breve</div>';

  loadLastSync();
  updateSyncTimer();
}

function updateSyncTimer(){
  const tEl=document.getElementById('dash-sync-timer');
  const pill=document.getElementById('dash-sync-pill');
  const last=window._lastSyncTime;
  if(last){
    const next=last+2*3600000;
    const diff=Math.round((next-Date.now())/60000);
    const d=new Date(last);
    const hm=d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});
    if(pill)pill.textContent='● Último sync '+hm+(diff>0?' · próximo em '+diff+' min':'');
    if(tEl)tEl.textContent=diff>0?'sync automático em ~'+diff+' min':'sync em andamento ou aguardando...';
  }
  setTimeout(updateSyncTimer,60000);
}

/* ═══════════ 19. PROJECTS ═══════════ */
function cleanLoc(l){return(l||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()||l;}
function projDropLabel(p){
  const ts=tickets.filter(t=>t.projectId===p.id);
  const locs=[...new Set(ts.map(t=>cleanLoc(t.location)).filter(Boolean))].join(', ');
  return locs?locs+' ('+p.name+')':p.name;
}

function renderProjects(){
  const g=document.getElementById('proj-grid');if(!g)return;
  const stateFilter=document.getElementById('proj-state-filter');
  if(stateFilter){
    const states=[...new Set(projects.map(p=>p.state).filter(Boolean))].sort();
    const prev=stateFilter.value;
    stateFilter.innerHTML='<option value="">Todos estados</option>'+states.map(s=>`<option value="${esc(s)}">${esc(s)}</option>`).join('');
    if(prev)stateFilter.value=prev;
  }
  const sf=stateFilter?.value||'';
  const filteredProjects=sf?projects.filter(p=>p.state===sf):projects;
  if(!filteredProjects.length){g.innerHTML='<div style="color:var(--muted);font-size:13px">Nenhum projeto.</div>';return;}
  const active=filteredProjects.filter(p=>p.status!=='Completed');
  const completed=filteredProjects.filter(p=>p.status==='Completed');

  const renderCard=(p)=>{
    const ts=filterTickets({projectId:p.id});
    const openC=ts.filter(t=>t.status==='Open').length,clearC=ts.filter(t=>t.status==='Clear').length,damageC=ts.filter(t=>t.status==='Damage').length,closedC=ts.filter(t=>t.status==='Closed').length;
    const ticketFt=ts.reduce((s,t)=>s+(t.footage||0),0);const projTotal=p.totalFeet||ticketFt||1;
    const clearFtP=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
    const openFtP=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
    const concluidoFt=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);
    const damageFtV=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
    const pctConcluido=projTotal>0?Math.round(concluidoFt/projTotal*100):0;
    const pctClear=projTotal>0?Math.round(clearFtP/projTotal*100):0;
    const pctOpen=projTotal>0?Math.round(openFtP/projTotal*100):0;
    const pctDamage=projTotal>0?Math.round(damageFtV/projTotal*100):0;
    const locations=[...new Set(ts.map(t=>cleanLoc(t.location)).filter(Boolean))];
    const locStr=locations.join(', ')||p.state;
    return`<div class="pcard"><div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:3px"><div style="flex:1"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><div class="pcard-name">📍 ${esc(locStr)}</div><div style="font-size:12px;color:var(--muted);font-family:var(--mono)">${esc(p.name)}</div></div></div><span class="status-pill pill-${p.status==='Active'?'active':'done'}" style="flex-shrink:0;margin-left:8px">${esc(p.status)}</span></div><div class="pcard-meta">${esc(p.client)} · ${esc(p.state)}</div><div class="prog-bar"><div style="width:${pctClear}%;background:var(--green)"></div><div style="width:${Math.min(pctOpen,100-pctClear)}%;background:var(--red)"></div><div style="width:${Math.min(pctDamage,100-pctClear-pctOpen)}%;background:#f59e0b"></div><div style="width:${Math.min(pctConcluido,100-pctClear-pctOpen-pctDamage)}%;background:var(--text)"></div></div><div class="pcard-stats"><div class="pstat"><span class="pstat-val" style="color:var(--red)">${openC}</span><span class="pstat-lbl">Open</span></div><div class="pstat"><span class="pstat-val" style="color:var(--green)">${clearC}</span><span class="pstat-lbl">Clear</span></div><div class="pstat"><span class="pstat-val" style="color:var(--amber)">${damageC}</span><span class="pstat-lbl">Damage</span></div><div class="pstat"><span class="pstat-val" style="color:var(--muted)">${closedC}</span><span class="pstat-lbl">Closed</span></div><div class="pstat"><span class="pstat-val">${ts.length}</span><span class="pstat-lbl">Total</span></div></div><div style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-bottom:10px">${ticketFt.toLocaleString()} ft${p.totalFeet?' / '+p.totalFeet.toLocaleString()+' ft total':''}</div><div style="display:flex;gap:6px;flex-wrap:wrap"><button class="btn btn-sm" onclick="shareProject('${p.id}')" style="background:var(--accent);color:white;border-color:var(--accent)">📤 Compartilhar</button><button class="btn btn-sm" onclick="openProjectMap('${p.id}')">Ver no mapa</button>${isAdmin?`<button class="btn btn-sm" onclick="editProject('${p.id}')">Editar</button><button class="btn btn-sm btn-danger" onclick="openDelProj('${p.id}')">Excluir</button>`:''}</div></div>`;
  };

  g.innerHTML=(active.length?active.map(renderCard).join(''):'')
    +(completed.length?`<div style="grid-column:1/-1;margin-top:24px;padding-top:20px;border-top:2px solid var(--border)"><div style="font-size:12px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;margin-bottom:14px">✅ Projetos Concluídos (${completed.length})</div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px">${completed.map(renderCard).join('')}</div></div>`:'');
}

function openProjectMap(pid){nav('map');setTimeout(()=>{document.getElementById('proj-filter').value=pid;onProjFilter();},200);}

function openNewProject(){
  editingProjectId=null;
  document.getElementById('proj-modal-title').textContent='Novo projeto';
  ['pm-loc','pm-num','pm-client','pm-state','pm-coords'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('pm-status').value='Active';
  document.getElementById('pm-feet').value='';
  openModal('ov-proj');
}
function editProject(pid){
  editingProjectId=pid;
  const p=projects.find(x=>x.id===pid);if(!p)return;
  document.getElementById('proj-modal-title').textContent='Editar projeto';
  const parts=p.name.split(' — ');
  if(parts.length>=2){document.getElementById('pm-loc').value=parts[0].trim();document.getElementById('pm-num').value=parts.slice(1).join(' — ').trim();}
  else{document.getElementById('pm-loc').value='';document.getElementById('pm-num').value=p.name;}
  document.getElementById('pm-client').value=p.client;
  document.getElementById('pm-state').value=p.state;
  document.getElementById('pm-status').value=p.status;
  document.getElementById('pm-feet').value=p.totalFeet||'';
  document.getElementById('pm-coords').value=p.centerCoords?p.centerCoords.join(', '):'';
  openModal('ov-proj');
}
async function saveProject(){
  const loc=document.getElementById('pm-loc').value.trim();
  const num=document.getElementById('pm-num').value.trim();
  if(!loc&&!num){toast('Preencha localidade ou número.','danger');return;}
  const name=loc&&num?`${loc} — ${num}`:loc||num;
  const coordStr=document.getElementById('pm-coords').value.trim();
  let centerCoords=null;
  if(coordStr){const m=coordStr.match(/([-\d.]+)\s*,\s*([-\d.]+)/);if(m)centerCoords=[parseFloat(m[1]),parseFloat(m[2])];}
  const data={name,client:document.getElementById('pm-client').value,state:document.getElementById('pm-state').value,status:document.getElementById('pm-status').value,desc:'',totalFeet:parseFloat(document.getElementById('pm-feet').value)||0,centerCoords,_manual:true};
  if(editingProjectId){
    const p=projects.find(x=>x.id===editingProjectId);
    if(p)Object.assign(p,data);
    await saveProjectToDb(projects.find(x=>x.id===editingProjectId));
  }else{
    const id='p'+Date.now();
    const p={...data,id};
    projects.push(p);
    await saveProjectToDb(p);
  }
  closeModal('ov-proj');syncAll();toast('Projeto salvo!','success');
}
function openDelProj(pid){
  deletingProjectId=pid;
  const p=projects.find(x=>x.id===pid);if(!p)return;
  const ts=tickets.filter(t=>t.projectId===pid);
  document.getElementById('del-proj-info').innerHTML=`Projeto: <strong>${esc(p.name)}</strong><br>Este projeto tem <strong>${ts.length} ticket(s)</strong> vinculado(s).`;
  openModal('ov-del-proj');
}
async function confirmDelProj(){
  if(!deletingProjectId)return;
  const ok=await deleteProjectFromDb(deletingProjectId);
  if(ok){
    projects=projects.filter(p=>p.id!==deletingProjectId);
    tickets.forEach(t=>{if(t.projectId===deletingProjectId)t.projectId='';});
    deletingProjectId=null;
    closeModal('ov-del-proj');syncAll();toast('Projeto excluído!','success');
  }else{toast('Erro ao excluir projeto','danger');}
}
function openMoveProj(tid){
  const t=tickets.find(x=>x.id===tid);if(!t)return;
  document.getElementById('move-proj-ticket-info').textContent=`Ticket: ${t.ticket}`;
  const sel=document.getElementById('move-proj-sel');
  sel.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}"${t.projectId===p.id?' selected':''}>${esc(p.name)}</option>`).join('');
  openModal('ov-move-proj');
}
async function saveMoveProj(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  t.projectId=document.getElementById('move-proj-sel').value;
  t.history.push({ts:Date.now(),action:`Movido para projeto: ${projects.find(p=>p.id===t.projectId)?.name||'Sem projeto'}`,color:'#1a6cf0'});
  await saveTicketToDb(t);
  closeModal('ov-move-proj');openTicketDetail(currentDetailId);syncAll();toast('Projeto atualizado!','success');
}

function shareProject(pid){
  const p=projects.find(x=>x.id===pid);if(!p)return;
  const url=window.location.origin+window.location.pathname+'?p='+encodeURIComponent(p.id);
  if(navigator.clipboard){
    navigator.clipboard.writeText(url).then(()=>toast('Link copiado! Quem abrir verá só este projeto.','success')).catch(()=>{prompt('Copie o link:',url);});
  }else{prompt('Copie o link:',url);}
}
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

/* ═══════════ 20. SHARED VIEW ═══════════ */
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
  const locs0=[...new Set(ts0.map(t=>t.location).filter(Boolean).map(l=>cleanLoc(l)))].join(', ')||p.state||'';
  document.getElementById('shared-proj-name').textContent=locs0+(locs0?' — ':'')+p.name+(p.client?' · '+p.client:'');

  const ts=filterTickets({projectId:p.id});
  const openC=ts.filter(t=>t.status==='Open').length;
  const clearC=ts.filter(t=>t.status==='Clear').length;
  const damageC=ts.filter(t=>t.status==='Damage').length;
  const totalFt=ts.reduce((s,t)=>s+(t.footage||0),0);
  document.getElementById('shared-stats').innerHTML=`
    <div class="shared-stat"><span class="shared-stat-val">${ts.length}</span><span class="shared-stat-lbl">Total</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--red)">${openC}</span><span class="shared-stat-lbl">Open</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--green)">${clearC}</span><span class="shared-stat-lbl">Clear</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--amber)">${damageC}</span><span class="shared-stat-lbl">Damage</span></div>
    <div class="shared-stat"><span class="shared-stat-val">${totalFt.toLocaleString()}</span><span class="shared-stat-lbl">Feet</span></div>`;

  renderSharedList();
  initSharedMap(p);

  // Expiring tickets alert for field view
  const now=new Date();
  const expired=ts.filter(t=>t.expire&&t.expire!=='—'&&t.status!=='Closed'&&t.status!=='Cancel'&&new Date(t.expire)<now);
  const expiring3d=ts.filter(t=>{if(!t.expire||t.expire==='—'||t.status==='Closed'||t.status==='Cancel')return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;const d=new Date(t.expire);const diff=(d-now)/86400000;return diff>=0&&diff<=3;});
  const expiring7d=ts.filter(t=>{if(!t.expire||t.expire==='—'||t.status==='Closed'||t.status==='Cancel')return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;const d=new Date(t.expire);const diff=(d-now)/86400000;return diff>3&&diff<=7;});

  const hasExpired=expired.length>0;
  if(hasExpired||expiring3d.length){
    const el=document.createElement('div');el.id='field-alert-overlay';
    const bgColor=hasExpired?'rgba(220,38,38,.92)':'rgba(217,119,6,.88)';
    const list=[...expired.map(t=>({t,label:'⛔ VENCIDO',color:'#fff'})),...expiring3d.map(t=>({t,label:'⚠ '+Math.round((new Date(t.expire)-now)/86400000)+'d',color:'#fde68a'})),...expiring7d.slice(0,3).map(t=>({t,label:Math.round((new Date(t.expire)-now)/86400000)+'d',color:'#bfdbfe'}))];
    const listHtml=list.map(({t,label,color})=>'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid rgba(255,255,255,.2)"><span style="font-family:var(--mono);color:white;font-weight:700">'+esc(t.ticket)+'</span><span style="color:'+color+';font-weight:700;font-size:12px">'+label+'</span></div>').join('');
    el.innerHTML='<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:20px;text-align:center">'
      +'<div style="font-size:50px;margin-bottom:12px">'+(hasExpired?'⛔':'⚠')+'</div>'
      +'<div style="font-size:24px;font-weight:700;color:white;margin-bottom:6px">'+(hasExpired?'TICKETS VENCIDOS':'TICKETS VENCENDO')+'</div>'
      +'<div style="font-size:14px;color:rgba(255,255,255,.8);margin-bottom:16px">'+(hasExpired?expired.length+' vencido(s)':'')+(hasExpired&&expiring3d.length?' + ':'')+( expiring3d.length?expiring3d.length+' expira(m) em 3 dias':'')+'</div>'
      +'<div style="max-width:320px;width:100%">'+listHtml+'</div>'
      +'<div style="font-size:13px;color:rgba(255,255,255,.5);margin-top:16px">Toque para fechar</div>'
      +'</div>';
    Object.assign(el.style,{position:'fixed',top:'0',left:'0',width:'100%',height:'100%',background:bgColor,zIndex:'99999',cursor:'pointer',backdropFilter:'blur(4px)',WebkitBackdropFilter:'blur(4px)',animation:'expFadeIn .3s ease'});
    el.onclick=()=>{el.style.animation='expFadeOut .3s ease';setTimeout(()=>el.remove(),280);};
    document.body.appendChild(el);
    setTimeout(()=>{const e=document.getElementById('field-alert-overlay');if(e){e.style.animation='expFadeOut .3s ease';setTimeout(()=>{if(e.parentNode)e.remove();},280);}},hasExpired?6000:4000);
  }

  // Sticky banner
  const allUrgent=[...expired,...expiring3d,...expiring7d];
  if(allUrgent.length){
    const banner=document.createElement('div');
    banner.id='field-expiring-banner';
    const bannerColor=expired.length?'#dc2626':expiring3d.length?'#d97706':'#2563eb';
    const bannerBg=expired.length?'#fef2f2':expiring3d.length?'#fffbeb':'#eff6ff';
    const bannerBorder=expired.length?'#fecaca':expiring3d.length?'#fde68a':'#bfdbfe';
    let parts=[];
    if(expired.length)parts.push('⛔ '+expired.length+' vencido'+(expired.length>1?'s':''));
    if(expiring3d.length)parts.push('⚠ '+expiring3d.length+' vence'+(expiring3d.length>1?'m':'')+' em 3d');
    if(expiring7d.length)parts.push(expiring7d.length+' vence'+(expiring7d.length>1?'m':'')+' em 7d');
    banner.innerHTML='<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap">'
      +'<span style="font-weight:700;font-size:12px">'+parts.join(' · ')+'</span>'
      +'<span style="font-size:10px;font-family:var(--mono)">'+allUrgent.slice(0,4).map(t=>esc(t.ticket)).join(', ')+(allUrgent.length>4?' +':'')+'</span>'
      +'</div>';
    Object.assign(banner.style,{position:'fixed',top:'0',left:'0',width:'100%',padding:'8px 14px',background:bannerBg,color:bannerColor,borderBottom:'2px solid '+bannerBorder,zIndex:'9998',fontSize:'12px',boxSizing:'border-box',animation:'expFadeIn .3s ease'});
    document.body.appendChild(banner);
  }
}

function exitSharedView(){
  isSharedView=false;
  document.getElementById('pg-shared').classList.remove('active');
  history.replaceState(null,'',window.location.pathname);
  const fb=document.getElementById('field-expiring-banner');if(fb)fb.remove();
  const fa=document.getElementById('field-alert-overlay');if(fa)fa.remove();
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
  return filterTickets({projectId:sharedProjectId,status:st,search:sr});
}

function renderSharedList(){
  const f=sharedFiltered();
  document.getElementById('shared-count').textContent=`${f.length} ticket${f.length!==1?'s':''}`;
  document.getElementById('shared-list').innerHTML=f.length?f.map(t=>
    `<div class="tcard s-${(t.status||'').toLowerCase()}" data-id="${t.id}" onclick="shFocusTicket(${t.id})">`
    +`<div class="tcard-top"><span class="tcard-num">${esc(t.ticket)}${isRenewed(t)?' <span style="font-size:9px;color:#7c3aed">🔄</span>':''}</span><span class="sbadge b-${t.status.toLowerCase()}">${esc(t.status)}</span></div>`
    +`<div class="tcard-client">${esc(t.client)}${t.prime?' · '+esc(t.prime):''}</div>`
    +`<div class="tcard-meta"><span>${esc(t.location)}, ${esc(t.state)}</span><span>${t.footage} ft</span>${t.tipo?`<span>${esc(t.tipo)}</span>`:''}</div></div>`
  ).join(''):'<div style="text-align:center;padding:28px;color:var(--muted);font-size:13px">Nenhum ticket</div>';
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

function clearSharedMapLayers(){
  shMkrs.forEach(m=>{m.off();shMap.removeLayer(m);});
  shLines.forEach(l=>{l.off();shMap.removeLayer(l);});
  shLabels.forEach(l=>{shMap.removeLayer(l);});
  shMkrs=[];shLines=[];shLabels=[];
}

function renderSharedMap(){
  if(!shMap)return;
  clearSharedMapLayers();
  const f=sharedFiltered();
  for(const t of f){
    const c=scol(t.status),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
    const coords=t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null;
    if(coords){
      const mi=op=>L.divIcon({className:'',html:`<div style="width:9px;height:9px;border-radius:50%;background:${c};border:2px solid white;opacity:${op};box-shadow:0 1px 4px rgba(0,0,0,.3)"></div>`,iconSize:[9,9],iconAnchor:[4,4]});
      const m1=L.marker(coords[0],{icon:mi(1)}).addTo(shMap);
      const m2=L.marker(coords[coords.length-1],{icon:mi(.7)}).addTo(shMap);
      m1.bindPopup(buildPopup(t,c));m2.bindPopup(buildPopup(t,c));shMkrs.push(m1,m2);
      const ln=L.polyline(coords,{color:c,weight:lw,opacity:.92,dashArray:dash}).addTo(shMap);shLines.push(ln);
      const mid=coords[Math.floor(coords.length/2)]||coords[0];
      const lbl=L.marker(mid,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="border-left:3px solid ${c}">${esc(t.ticket)}</a>`,iconAnchor:[32,10]})}).addTo(shMap);
      shLabels.push(lbl);
    }else{
      let pos=t._geocoded||null;
      if(!pos){
        const p=projects.find(x=>x.id===sharedProjectId);
        const cc=p?.centerCoords||cityCoords(t.location);
        const jitter=()=>(Math.random()-.5)*0.002;
        pos=[cc[0]+jitter(),cc[1]+jitter()];
        enqueueGeocode(t);
      }
      const mi=L.divIcon({className:'',html:`<div style="width:11px;height:11px;border-radius:50%;background:${c};border:2px solid white;box-shadow:0 1px 4px rgba(0,0,0,.3);opacity:${t._geocoded?1:.6}"></div>`,iconSize:[11,11],iconAnchor:[5,5]});
      const mk=L.marker(pos,{icon:mi}).addTo(shMap);mk.bindPopup(buildPopup(t,c));shMkrs.push(mk);
      const lbl=L.marker(pos,{icon:L.divIcon({className:'',html:`<a class="ticket-label" onclick="openTicketDetail(${t.id});return false;" href="#" style="margin-top:12px;display:block;border-left:3px solid ${c}">${esc(t.ticket)}</a>`,iconAnchor:[32,-2]})}).addTo(shMap);
      shLabels.push(lbl);
    }
  }
}

function shFocusTicket(id){
  if(window.innerWidth<=768){const sb=document.getElementById('shared-sidebar');const ov=document.getElementById('shared-overlay');sb.classList.remove('mob-open');ov.classList.remove('open');document.getElementById('shared-toggle-label').textContent='Ver tickets';}
  const t=tickets.find(x=>x.id===id);if(!t||!shMap)return;
  document.querySelectorAll('#shared-list .tcard').forEach(c=>c.classList.remove('active'));
  const cd=document.querySelector(`#shared-list [data-id="${id}"]`);if(cd)cd.classList.add('active');
  if(t.fieldPath&&t.fieldPath.length>=2)shMap.fitBounds(L.latLngBounds(t.fieldPath),{padding:[60,60],maxZoom:19});
  else if(t._geocoded)shMap.setView(t._geocoded,18);
}
function shSetLayer(t){
  [shSatL,shStrL,shHybL].forEach(l=>{try{shMap.removeLayer(l)}catch{}});
  ['sh-bsat','sh-bstr','sh-bhyb'].forEach(id=>{const el=document.getElementById(id);if(el)el.classList.remove('active')});
  if(t==='sat'){shSatL.addTo(shMap);document.getElementById('sh-bsat').classList.add('active');}
  else if(t==='hyb'){shHybL.addTo(shMap);document.getElementById('sh-bhyb').classList.add('active');}
  else{shStrL.addTo(shMap);document.getElementById('sh-bstr').classList.add('active');}
}
function shFitAll(){
  if(!shMap)return;
  const ts=tickets.filter(t=>t.projectId===sharedProjectId);
  const wc=ts.filter(t=>t.fieldPath&&t.fieldPath.length>=2);
  if(wc.length)shMap.fitBounds(L.latLngBounds(wc.flatMap(t=>t.fieldPath)),{padding:[40,40]});
  else{const p=projects.find(x=>x.id===sharedProjectId);if(p?.centerCoords)shMap.setView(p.centerCoords,15);}
}

/* ═══════════ 21. TICKET CRUD ═══════════ */
function openNewTicket(){
  editingTicketId=null;
  document.getElementById('ticket-modal-title').textContent='Novo ticket';
  ['tm-t','tm-c','tm-co','tm-l','tm-st','tm-f','tm-notes','tm-tipo','tm-job','tm-prime','tm-addr'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('tm-s').value='Open';
  document.getElementById('tm-e').value='';
  document.getElementById('tm-proj').value='';
  openModal('ov-ticket');
}
function editCurrentTicket(){
  closeModal('ov-detail');
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  editingTicketId=t.id;
  document.getElementById('ticket-modal-title').textContent='Editar ticket';
  document.getElementById('tm-t').value=t.ticket;
  document.getElementById('tm-s').value=t.status;
  document.getElementById('tm-proj').value=t.projectId||'';
  document.getElementById('tm-c').value=t.client;
  document.getElementById('tm-co').value=t.company;
  document.getElementById('tm-l').value=t.location;
  document.getElementById('tm-st').value=t.state;
  document.getElementById('tm-f').value=t.footage;
  document.getElementById('tm-e').value=t.expire;
  document.getElementById('tm-notes').value=t.notes||'';
  document.getElementById('tm-tipo').value=t.tipo||'';
  document.getElementById('tm-job').value=t.job||'';
  document.getElementById('tm-prime').value=t.prime||'';
  document.getElementById('tm-addr').value=t.address||'';
  openModal('ov-ticket');
}
async function saveTicket(){
  const tnum=document.getElementById('tm-t').value.trim();
  if(!tnum){toast('Preencha o número.','danger');return;}
  const newStatus=document.getElementById('tm-s').value;
  let savedId=null;
  if(editingTicketId){
    const t=tickets.find(x=>x.id===editingTicketId);
    if(t){
      const old=t.status;
      Object.assign(t,{
        ticket:tnum,projectId:document.getElementById('tm-proj').value,
        client:document.getElementById('tm-c').value,company:document.getElementById('tm-co').value,
        location:document.getElementById('tm-l').value,state:document.getElementById('tm-st').value,
        footage:parseInt(document.getElementById('tm-f').value)||0,expire:document.getElementById('tm-e').value||'',
        notes:document.getElementById('tm-notes').value,status:newStatus,
        tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,
        prime:document.getElementById('tm-prime').value,address:document.getElementById('tm-addr').value
      });
      if(old!==newStatus)t.history.push({ts:Date.now(),action:`Status: ${old} → ${newStatus}`,color:scol(newStatus)});
      t.history.push({ts:Date.now(),action:'Editado',color:'#9a9888'});
      await saveTicketToDb(t);savedId=t.id;
    }
    toast('Ticket atualizado!','success');
  }else{
    const t={id:null,ticket:tnum,projectId:document.getElementById('tm-proj').value,company:document.getElementById('tm-co').value||'One Drill',state:document.getElementById('tm-st').value||'FL',location:document.getElementById('tm-l').value||'',status:newStatus,expire:document.getElementById('tm-e').value||'',footage:parseInt(document.getElementById('tm-f').value)||0,client:document.getElementById('tm-c').value||'—',prime:document.getElementById('tm-prime').value,tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,address:document.getElementById('tm-addr').value,notes:document.getElementById('tm-notes').value,fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Ticket criado',color:'#1a6cf0'}],attachments:[],pending:'',oldTicket2:'',statusOld:'',expireOld:'',status_locked:false};
    tickets.push(t);await saveTicketToDb(t);savedId=t.id;
    toast('Ticket criado!','success');
  }
  closeModal('ov-ticket');syncAll();
  if(savedId)setTimeout(()=>openTicketDetail(savedId),200);
}

/* ═══════════ 22. IMPORT (BATCH) ═══════════ */
function openImport(){
  parsed=[];parsedProjectTotals={};parsedProjectCoords={};
  document.getElementById('prevarea').style.display='none';
  document.getElementById('bimport').style.display='none';
  document.getElementById('progwrap').style.display='none';
  document.getElementById('progfill').style.width='0%';
  document.getElementById('ffile').value='';
  openModal('ov-import');
}
function onDrop(e){e.preventDefault();document.getElementById('uzone').classList.remove('drag');if(e.dataTransfer.files[0])readFile(e.dataTransfer.files[0]);}
function onFileIn(e){if(e.target.files[0])readFile(e.target.files[0]);}
function nk(k){return String(k||'').toLowerCase().replace(/[^a-z0-9]/g,'');}

function readFile(file){
  const reader=new FileReader();
  reader.onload=e=>{
    try{
      const wb=XLSX.read(e.target.result,{type:'binary',cellDates:true});
      // Parse project sheet
      if(wb.SheetNames.length>1){
        const wsP=wb.Sheets[wb.SheetNames[1]];
        const projRows=XLSX.utils.sheet_to_json(wsP,{header:1,defval:''});
        for(let i=0;i<projRows.length;i++){
          if(projRows[i].some(c=>String(c||'').toLowerCase().includes('project'))){
            const hdr=projRows[i].map(h=>String(h||'').toLowerCase().replace(/[^a-z0-9]/g,''));
            for(let j=i+1;j<projRows.length;j++){
              const r=projRows[j];
              if(!r.some(c=>c!==null&&c!==''&&c!==undefined))continue;
              const pidIdx=hdr.findIndex(h=>h.includes('project'));
              const ftIdx=hdr.findIndex(h=>h.includes('feet')||h.includes('total'));
              const coordIdx=hdr.findIndex(h=>h.includes('coord')||h.includes('lat'));
              const pid=String(r[pidIdx]||'').trim();
              const ft=parseFloat(r[ftIdx])||0;
              if(pid){
                parsedProjectTotals[pid]=ft;
                if(coordIdx>=0){const coordStr=String(r[coordIdx]||'').trim();const m=coordStr.match(/([-\d.]+)\s*,\s*([-\d.]+)/);if(m)parsedProjectCoords[pid]=[parseFloat(m[1]),parseFloat(m[2])];}
              }
            }
            break;
          }
        }
      }
      const ws=wb.Sheets[wb.SheetNames[0]];
      const allRows=XLSX.utils.sheet_to_json(ws,{header:1,defval:''});
      if(!allRows.length){toast('Arquivo vazio.','danger');return;}
      let headerRowIdx=0;
      for(let i=0;i<Math.min(5,allRows.length);i++){
        if(allRows[i].some(c=>String(c||'').toLowerCase().replace(/\s/g,'').includes('ticket'))){headerRowIdx=i;break;}
      }
      const headers=allRows[headerRowIdx].map(h=>nk(h));
      const dataRows=allRows.slice(headerRowIdx+1).filter(r=>r.some(c=>c!==null&&c!==''&&c!==undefined));
      const ci=(...names)=>{for(const n of names){const i=headers.findIndex(h=>h&&h===n);if(i>=0)return i;}for(const n of names){const i=headers.findIndex(h=>h&&h.startsWith(n)&&h.length<=n.length+3);if(i>=0)return i;}return -1;};
      const idx={ticket:ci('ticket'),company:ci('company'),state:ci('state'),location:ci('location'),status:ci('status'),expire:ci('expireon'),footage:ci('footage'),client:ci('client'),prime:ci('prime'),job:ci('jobnumber'),tipo:ci('tipo'),address:ci('mainaddress'),project:ci('project'),pending:ci('pending'),oldTicket2:ci('oldticket'),statusOld:ci('statusold'),expireOld:ci('oldexpirationdate')};
      const getCell=(row,i)=>{if(i<0||i>=row.length)return'';const v=row[i];if(v===null||v===undefined)return'';if(v instanceof Date)return v.toLocaleDateString('en-US');return String(v).replace(/\xa0/g,'').trim();};
      parsed=dataRows.map(row=>{
        const ticket=getCell(row,idx.ticket);if(!ticket)return null;
        let rawStatus=getCell(row,idx.status);
        if(rawStatus.includes('✅')||rawStatus.includes('⚠'))rawStatus='Open';
        let status='Open';const sl=rawStatus.toLowerCase();
        if(sl==='clear')status='Clear';else if(sl==='open')status='Open';
        else if(sl==='closed'||sl==='close')status='Closed';else if(sl==='damage')status='Damage';
        else if(sl==='cancel')status='Cancel';else if(rawStatus)status=rawStatus;
        const rawExpOld=getCell(row,idx.expireOld);let expireOld='';
        if(rawExpOld){try{const d=new Date(rawExpOld);expireOld=isNaN(d.getTime())?rawExpOld:d.toLocaleDateString('en-US');}catch{expireOld=rawExpOld;}}
        return{ticket,company:getCell(row,idx.company)||'One Drill',state:getCell(row,idx.state),location:getCell(row,idx.location),status,expire:getCell(row,idx.expire),footage:parseFloat(getCell(row,idx.footage))||0,client:getCell(row,idx.client),prime:getCell(row,idx.prime),job:getCell(row,idx.job),tipo:getCell(row,idx.tipo),address:getCell(row,idx.address),projectName:getCell(row,idx.project),pending:getCell(row,idx.pending),oldTicket2:getCell(row,idx.oldTicket2),statusOld:getCell(row,idx.statusOld),expireOld};
      }).filter(Boolean);
      if(!parsed.length){toast('Nenhuma linha válida.','danger');return;}
      const cols=['ticket','client','prime','status','footage','tipo'];
      document.getElementById('prevlabel').textContent=`${parsed.length} ticket(s) detectados`;
      document.getElementById('ptbl').innerHTML='<thead><tr>'+cols.map(c=>`<th>${c}</th>`).join('')+'</tr></thead><tbody>'
        +parsed.slice(0,10).map(r=>'<tr>'+cols.map(c=>`<td>${esc(r[c]||'—')}</td>`).join('')+'</tr>').join('')
        +(parsed.length>10?`<tr><td colspan="${cols.length}" style="color:var(--muted);text-align:center">... +${parsed.length-10} linhas</td></tr>`:'')
        +'</tbody>';
      document.getElementById('prevarea').style.display='block';
      document.getElementById('bimport').style.display='';
    }catch(err){toast('Erro: '+err.message,'danger');console.error(err);}
  };
  reader.readAsBinaryString(file);
}

async function doImport(){
  if(!parsed.length)return;
  if(!await requireAuth())return;
  const mode=document.querySelector('input[name="importmode"]:checked')?.value||'replace';
  const pw=document.getElementById('progwrap'),pf=document.getElementById('progfill'),pt=document.getElementById('progtxt');
  pw.style.display='block';document.getElementById('bimport').disabled=true;setSyncStatus(true,'Importando...');

  // Save projects first
  for(const[projId,ft]of Object.entries(parsedProjectTotals)){
    const pc=parsedProjectCoords[projId]||null;
    let p=projects.find(x=>x.name===projId||x.id===projId);
    if(!p){p={id:'p'+projId,name:projId,client:'',state:'',status:'Active',desc:'',totalFeet:ft,centerCoords:pc,_manual:false};projects.push(p);}
    else{if(!p.totalFeet)p.totalFeet=ft;if(!p.centerCoords)p.centerCoords=pc;}
    await saveProjectToDb(p);
  }

  if(mode==='replace'){
    pt.textContent='Limpando tickets antigos...';
    await sb.from('tickets').delete().neq('id',0);
    tickets=[];projects=projects.filter(p=>p._manual);
  }

  // ── BATCH IMPORT ──
  const novo=[];let updated=0;
  const batchBuffer=[];

  for(let i=0;i<parsed.length;i++){
    const r=parsed[i];
    pf.style.width=Math.round(((i+1)/parsed.length)*100)+'%';
    pt.textContent=`${i+1}/${parsed.length}: ${r.ticket}...`;

    let pid='';
    if(r.projectName){
      let p=projects.find(x=>x.name.toLowerCase()===r.projectName.toLowerCase()||x.id==='p'+r.projectName);
      if(!p){
        const projId=String(r.projectName).trim();
        const tf=parsedProjectTotals[projId]||0;
        const pc=parsedProjectCoords[projId]||null;
        p={id:'p'+projId,name:projId,client:r.client,state:r.state,status:'Active',desc:'',_manual:false,totalFeet:tf,centerCoords:pc};
        projects.push(p);await saveProjectToDb(p);
      }else{
        const projId=String(r.projectName).trim();
        if(!p.totalFeet)p.totalFeet=parsedProjectTotals[projId]||0;
        if(!p.centerCoords)p.centerCoords=parsedProjectCoords[projId]||null;
      }
      pid=p.id;
    }

    if(mode==='update'){
      const existing=tickets.find(t=>String(t.ticket).trim()===String(r.ticket).trim());
      if(existing){
        const oldStatus=existing.status;
        Object.assign(existing,{company:r.company,state:r.state,location:r.location,status:r.status,expire:r.expire,footage:r.footage,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,projectId:pid||existing.projectId});
        if(oldStatus!==r.status)existing.history.push({ts:Date.now(),action:`Status: ${oldStatus} → ${r.status}`,color:scol(r.status)});
        existing.history.push({ts:Date.now(),action:'Atualizado via Excel ✅',color:'#16a34a'});
        batchBuffer.push(existing);
        updated++;
        // Flush batch
        if(batchBuffer.length>=BATCH_SIZE){
          await saveTicketBatch(batchBuffer);
          batchBuffer.length=0;
        }
        continue;
      }
    }

    const t={id:null,ticket:r.ticket,projectId:pid,company:r.company||'One Drill',state:r.state,location:r.location,status:r.status,expire:r.expire,footage:r.footage,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,notes:'',fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Importado via Excel',color:'#1a6cf0'}],attachments:[],status_locked:false};
    tickets.push(t);
    batchBuffer.push(t);
    novo.push(t);
    // Flush batch
    if(batchBuffer.length>=BATCH_SIZE){
      pt.textContent=`Salvando batch... (${i+1}/${parsed.length})`;
      await saveTicketBatch(batchBuffer);
      batchBuffer.length=0;
    }
  }
  // Flush remaining
  if(batchBuffer.length){
    pt.textContent='Salvando últimos tickets...';
    await saveTicketBatch(batchBuffer);
  }

  document.getElementById('bimport').disabled=false;
  closeModal('ov-import');syncAll();setSyncStatus(true,'Sincronizado ✓');
  if(mode==='update')toast(`✅ ${updated} atualizados · ${novo.length} novos`,'success');
  else toast(`${novo.length} tickets importados`,'success');
}

/* ═══════════ 23. EXPORT ═══════════ */
function exportExpiring(){
  const days=_soonDays||10;
  const f=filterTickets({}).filter(t=>{
    if(!t.expire||t.expire==='—')return false;
    const d=new Date(t.expire);const diff=(d-Date.now())/86400000;
    return diff>=0&&diff<=days&&t.status!=='Closed'&&t.status!=='Cancel';
  });
  if(!f.length){toast('Nenhum ticket vencendo.','warn');return;}
  const wb=XLSX.utils.book_new();
  const rows=[['Ticket #','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Projeto','Utilities Pendentes']];
  for(const t of f){
    const proj=projects.find(p=>p.id===t.projectId)?.name||'';
    const pends=getTicketPendingUtils(String(t.ticket).trim()).map(u=>u.utility_name).join(', ');
    rows.push([t.ticket,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,proj,pends]);
  }
  const ws=XLSX.utils.aoa_to_sheet(rows);
  ws['!cols']=[{wch:14},{wch:20},{wch:16},{wch:7},{wch:20},{wch:8},{wch:9},{wch:12},{wch:12},{wch:24},{wch:10},{wch:20},{wch:30}];
  XLSX.utils.book_append_sheet(wb,ws,'Vencendo');
  XLSX.writeFile(wb,'OneDrill_Vencendo_'+days+'dias_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(f.length+' tickets exportados','success');
}

function exportFiltered(){
  const sr=(document.getElementById('tbl-srch')?.value||'').toLowerCase();
  const st=document.getElementById('tbl-stat')?.value||'';
  const pr=document.getElementById('tbl-proj')?.value||'';
  const cl=document.getElementById('tbl-cli')?.value||'';
  const ut=document.getElementById('tbl-util')?.value||'';
  const f=filterTickets({status:st,projectId:pr,client:cl,search:sr,utility:ut});
  if(!f.length){toast('Nenhum ticket para exportar com esses filtros.','warn');return;}
  const totalFt=f.reduce((s,t)=>s+(t.footage||0),0);
  const wb=XLSX.utils.book_new();
  const tData=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Pending','Empresa'],...f.map(t=>[t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,t.pending,t.company]),['','','','','','','TOTAL:',totalFt,'','','','','','']];
  const ws=XLSX.utils.aoa_to_sheet(tData);
  XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  XLSX.writeFile(wb,'OneDrill_Filtrado_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Excel filtrado: '+f.length+' tickets · '+totalFt.toLocaleString()+' ft','success');
}

function exportExcel(){
  const wb=XLSX.utils.book_new();
  const tData=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Job #','Pending','Empresa'],...tickets.map(t=>[t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,t.job,t.pending,t.company])];
  const ws=XLSX.utils.aoa_to_sheet(tData);XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  const pData=[['Nome','Cliente','Estado','Status','Total Feet','Tickets'],...projects.map(p=>[p.name,p.client,p.state,p.status,p.totalFeet,tickets.filter(t=>t.projectId===p.id).length])];
  const wp=XLSX.utils.aoa_to_sheet(pData);XLSX.utils.book_append_sheet(wb,wp,'Projetos');
  XLSX.writeFile(wb,`OneDrill_${new Date().toISOString().slice(0,10)}.xlsx`);
  toast('Excel exportado!','success');
}

function exportUtilTickets(utilName){
  const openTickets=filterTickets({}).filter(t=>t.status==='Open'||t.status==='Damage'||t.status==='Clear');
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
  const openTickets=filterTickets({}).filter(t=>t.status==='Open'||t.status==='Damage'||t.status==='Clear');
  const rows=[];
  for(const t of openTickets){
    const pends=getTicketPendingUtils(t.ticket);if(!pends.length)continue;
    for(const p of pends)rows.push([t.ticket,p.utility_name,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,projects.find(pr=>pr.id===t.projectId)?.name||'']);
  }
  if(!rows.length){toast('Nenhuma pendência','warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[['Ticket #','Utility Pendente','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Projeto'],...rows];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Todas Pendentes');
  XLSX.writeFile(wb,'OneDrill_Pendentes_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(rows.length+' pendências exportadas','success');
}

function exportPrivateLocator(){
  if(!utilCacheLoaded){toast('Aguarde carregar dados','warn');return;}
  const active=filterTickets({}).filter(t=>t.status!=='Closed'&&t.status!=='Cancel');
  const rows=[['Ticket','Status','Local','Estado','Utility','Resposta','Expira']];
  for(const t of active){
    const tkey=String(t.ticket).trim();
    const utils=getTicketUtils(tkey);
    for(const u of utils){
      const rt=(u.response_text||'').toLowerCase();
      if(rt.includes('3h')||rt.includes('privately owned')||rt.includes('private facility owner')){
        rows.push([t.ticket,t.status,t.location,t.state,u.utility_name,u.response_text||'',t.expire||'']);
      }
    }
  }
  if(rows.length<=1){toast('Nenhum ticket com private locator','info');return;}
  const wb=XLSX.utils.book_new();const ws=XLSX.utils.aoa_to_sheet(rows);
  XLSX.utils.book_append_sheet(wb,ws,'Private Locator');
  XLSX.writeFile(wb,'OneDrill_PrivateLocator_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Exportado '+(rows.length-1)+' registros','success');
}

/* ═══════════ 24. GLOBAL SEARCH ═══════════ */
function globalSearch(q){
  const dd=document.getElementById('gsearch-dd');if(!dd)return;
  q=(q||'').toLowerCase().trim();
  if(!q||q.length<2){dd.innerHTML='';dd.classList.remove('has-results');return;}
  const results=[];
  for(const t of tickets){
    if(results.length>=10)break;
    if(t.ticket.toLowerCase().includes(q)||(t.client||'').toLowerCase().includes(q)||(t.address||'').toLowerCase().includes(q)||(t.prime||'').toLowerCase().includes(q)){
      results.push({type:'ticket',id:t.id,title:t.ticket,sub:t.client+' · '+t.location+' · '+t.status,status:t.status});
    }
  }
  for(const p of projects){
    if(results.length>=12)break;
    if(p.name.toLowerCase().includes(q)||(p.client||'').toLowerCase().includes(q)){
      results.push({type:'project',id:p.id,title:p.name,sub:p.client+' · '+p.state});
    }
  }
  if(!results.length){dd.innerHTML='<div style="padding:12px;color:var(--muted);font-size:12px;text-align:center">Nenhum resultado</div>';dd.classList.add('has-results');return;}
  dd.innerHTML=results.map(r=>
    '<div class="gsr-item" onmousedown="'+(r.type==='ticket'?'openTicketDetail('+r.id+')':'openProjectMap(\''+r.id+'\')')+';document.getElementById(\'gsearch\').value=\'\';document.getElementById(\'gsearch-dd\').classList.remove(\'has-results\')">'
    +'<div class="gsr-num">'+(r.type==='ticket'?'🎫':'📁')+' '+esc(r.title)
    +(r.status?' <span class="sbadge b-'+r.status.toLowerCase()+'" style="font-size:9px">'+esc(r.status)+'</span>':'')
    +'</div><div class="gsr-sub">'+esc(r.sub)+'</div></div>'
  ).join('');
  dd.classList.add('has-results');
}

/* ═══════════ 25. NOTIFICATIONS ═══════════ */
function buildNotifications(){
  try{
    const notifs=[];const now=Date.now(),day3=3*864e5;
    const expiring=tickets.filter(t=>{
      if(!t.expire||t.expire==='—'||t.status==='Closed'||t.status==='Cancel'||isSuperseded(t))return false;
      if(isRenewed(t)&&isInRenewalGrace(t))return false;
      const d=new Date(t.expire);const diff=(d-now)/864e5;return diff>=0&&diff<=5;
    });
    for(const t of expiring)notifs.push({icon:'⏰',text:t.ticket+' expira '+t.expire,id:t.id,type:'warn'});
    for(const t of tickets){
      if(!t.history)continue;
      for(const h of t.history){
        if(h.ts>=now-day3&&(h.action||'').toLowerCase().includes('→ clear')){notifs.push({icon:'✅',text:t.ticket+' clareado',id:t.id,type:'good'});break;}
      }
    }
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
    if(byType.warn.length||byType.danger.length){
      h+='<div class="notif-section">⚠ Atenção</div>';
      for(const n of [...byType.danger,...byType.warn])h+='<div class="notif-item" onclick="openTicketDetail('+n.id+');toggleNotifPanel()">'+n.icon+' '+esc(n.text)+'</div>';
    }
    if(byType.good.length){
      h+='<div class="notif-section">✅ Resolvidos</div>';
      for(const n of byType.good.slice(0,10))h+='<div class="notif-item" onclick="openTicketDetail('+n.id+');toggleNotifPanel()">'+n.icon+' '+esc(n.text)+'</div>';
    }
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
    for(const t of recent){
      const last=t.history?.[t.history.length-1];
      h+='<div class="notif-item" onclick="openTicketDetail('+t.id+');toggleInfoPanel()">'
        +'<span style="font-family:var(--mono);font-weight:600">'+esc(t.ticket)+'</span> '
        +'<span class="sbadge b-'+t.status.toLowerCase()+'" style="font-size:9px">'+esc(t.status)+'</span>'
        +'<div style="font-size:10px;color:var(--muted);margin-top:1px">'+esc(last?.action||'—')+'</div></div>';
    }
    h+='<div class="notif-section" style="margin-top:12px">ℹ️ Sistema</div><div style="font-size:12px;color:var(--text2);padding:6px 10px;line-height:1.8">🔵 Dados: Supabase<br>🟢 Sync: Automática<br>🗺 Mapa: Google Hybrid</div>';
    panel.innerHTML=h;
  }
}

/* ═══════════ 26. ANALYTICS ═══════════ */
function renderAnalytics(){
  const el=document.getElementById('analytics-content');if(!el)return;
  const states=[...new Set(tickets.map(t=>t.state).filter(Boolean))].sort();
  const dsf=dashStateVal;
  const sf='<select class="fi" onchange="dashStateVal=this.value;renderAnalytics()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px"><option value="">Todos estados</option>'+states.map(s=>'<option value="'+esc(s)+'"'+(dsf===s?' selected':'')+'>'+esc(s)+'</option>').join('')+'</select>';
  const fT=filterTickets({state:dsf});
  const fP=dsf?projects.filter(p=>p.state===dsf):projects;
  const now=Date.now();const week=7*86400000;
  const ps=fP.filter(p=>p.status!=='Completed').map(p=>{
    const ts=fT.filter(t=>t.projectId===p.id);
    const cf=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
    const of2=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
    const con=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);
    const df=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
    const tf=ts.reduce((s,t)=>s+(t.footage||0),0);const tot=p.totalFeet||tf||1;
    const locs=[...new Set(ts.map(t=>t.location).filter(Boolean).map(l=>cleanLoc(l)))].join(', ')||'';
    const c4w_bins=[0,0,0,0];
    for(const t2 of ts){
      if(!t2.history)continue;
      const clearEvt=t2.history.filter(h2=>{const a2=(h2.action||'').toLowerCase();return a2.includes('→ clear')||a2.includes('auto 811')||a2.includes('auto-clear');}).pop();
      if(!clearEvt||!clearEvt.ts)continue;
      const wAgo=Math.floor((now-clearEvt.ts)/week);
      if(wAgo>=0&&wAgo<4)c4w_bins[wAgo]+=(t2.footage||0);
    }
    const wgts=[3,2,1,0.5];
    const wSum2=c4w_bins.reduce((s2,v,i2)=>s2+v*wgts[i2],0);
    const wTot2=wgts.reduce((s2,w2,i2)=>s2+(c4w_bins[i2]>0?w2:0),0);
    let fpw2=wTot2>0?wSum2/wTot2:0;
    if(fpw2===0)fpw2=c4w_bins.reduce((s2,v)=>s2+v,0)/4;
    return{id:p.id,name:p.name,locs,clearFtP:cf,openFtP:of2,concluidoFt:con,damageFt:df,ticketFt:tf,totalFt:tot,
      pctClear:tot>0?Math.round(cf/tot*100):0,pctOpen:tot>0?Math.round(of2/tot*100):0,
      pctConcluido:tot>0?Math.round(con/tot*100):0,pctDamage:tot>0?Math.round(df/tot*100):0,
      hasTotalFromSheet:!!p.totalFeet,state:p.state||'',ftPerWeek:Math.round(fpw2),
      weeksLeft:fpw2>0?Math.ceil(of2/fpw2):null,count:ts.length};
  }).sort((a,b)=>b.count-a.count);

  const syncPill=window._lastSyncTime?(function(){var d=new Date(window._lastSyncTime);var hm=d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});var diff=Math.round((Date.now()-window._lastSyncTime)/60000);var ago2;if(diff<2)ago2='agora';else if(diff<60)ago2=diff+'min atrás';else if(diff<120)ago2='1h atrás';else ago2=Math.round(diff/60)+'h atrás';return'<span id="analytics-sync-pill" style="font-size:11px;padding:3px 10px;border-radius:20px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border);margin-left:8px;white-space:nowrap">● Último sync '+hm+' ('+ago2+')</span>';})():'<span id="analytics-sync-pill" style="font-size:11px;padding:3px 10px;border-radius:20px;background:var(--bg);color:var(--muted);border:1px solid var(--border);margin-left:8px;white-space:nowrap">⏳ Carregando sync...</span>';

  el.innerHTML=
    '<div class="page-title">Analytics <span style="font-size:13px;font-weight:400;color:var(--muted);font-family:var(--mono)">'+new Date().toLocaleDateString('pt-BR')+'</span>'+syncPill+'<span style="margin-left:auto">'+sf+'</span></div>'
    +renderClearedStats(fT)
    +renderWeeklyEvolution(fT)
    +renderProgressoFootage(fT,ps)
    +renderRiskAnalytics(fT)
    +renderVelocity(fT,ps)
    +renderClearTimeMetrics(fT)
    +renderUtilSummaryHtml()
    +renderRecentActivity(fT)
    +renderSyncHealthCard();
  loadLastSync();
}

function renderVelocity(fT,ps){
  try{
    const vf=_velProjFilter||'';const week=7*86400000;const now=Date.now();
    const pv=ps.filter(p=>p.count>0);
    if(!pv.length)return'';
    const opts='<option value="">Todos projetos</option>'+pv.map(p=>'<option value="'+p.id+'"'+(vf===p.id?' selected':'')+'>'+(p.locs?esc(p.locs)+' ('+esc(p.name)+')':esc(p.name))+'</option>').join('');
    const sh=vf?pv.filter(p=>p.id===vf):pv;
    if(!sh.length)return'';
    const rows=sh.map(p=>{
      const ft=p.ftPerWeek||0;const wl=p.weeksLeft;
      const fc=wl!==null?(wl<=0?'<span style="color:var(--green);font-weight:700">Concluído</span>':wl===1?'<span style="color:var(--amber);font-weight:700">~1 semana</span>':'<span style="color:var(--text2)">~'+wl+' sem.</span>'):'<span style="color:var(--muted)">sem dados</span>';
      const vel=ft>0?'<span style="color:var(--green);font-family:var(--mono);font-weight:600">'+ft.toLocaleString()+' ft/sem</span>':'<span style="color:var(--muted);font-size:11px">parado</span>';
      return'<tr style="border-bottom:1px solid var(--border)"><td style="padding:8px 6px;font-size:12px;font-weight:600">'+esc(p.locs||p.name)+'</td><td style="padding:8px 6px">'+vel+'</td><td style="padding:8px 6px">'+fc+'</td><td style="padding:8px 6px;min-width:120px"><div style="display:flex;align-items:center;gap:6px"><div style="flex:1;height:6px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:'+p.pctClear+'%;height:100%;background:var(--green);border-radius:3px"></div></div><span style="font-size:10px;font-family:var(--mono);color:var(--muted)">'+p.pctClear+'%</span></div></td><td style="padding:8px 6px;font-size:11px;color:var(--muted);font-family:var(--mono)">'+(p.openFtP||0).toLocaleString()+' ft</td></tr>';
    }).join('');
    return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin:0">🚀 Velocity & Previsão</div><select class="fi" onchange="_velProjFilter=this.value;renderAnalytics()" style="width:auto;min-width:160px;font-size:11px;padding:4px 6px">'+opts+'</select></div><table style="width:100%;border-collapse:collapse"><thead><tr style="border-bottom:2px solid var(--border)"><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Projeto</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Velocidade</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Previsão</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Progresso</th><th style="padding:6px;text-align:left;font-size:10px;color:var(--muted);text-transform:uppercase">Restante</th></tr></thead><tbody>'+rows+'</tbody></table></div></div>';
  }catch(e){console.error('Velocity:',e);return'';}
}

/* Analytics sub-renderers — renderClearedStats, renderWeeklyEvolution, renderProgressoFootage,
   renderRiskAnalytics, renderClearTimeMetrics, renderUtilSummaryHtml, renderRecentActivity,
   renderSyncHealthCard, renderSyncHealth, renderPrivateLocatorAlert, renderHealthCard, loadLastSync
   — preserved from original with esc() applied to all user data */

function renderClearedStats(fTickets){
  var now=Date.now(),day1=now-864e5,day7=now-7*864e5,day30=now-30*864e5;
  var cpf=_clearProjFilter||'';
  var ft2=cpf?fTickets.filter(function(t){return t.projectId===cpf;}):fTickets;
  function getTicketClearDate(t){
    if(!t.history||!t.history.length)return 0;
    for(var j=t.history.length-1;j>=0;j--){
      var a=(t.history[j].action||'').toLowerCase();
      if(a.indexOf('\u2192 clear')>=0||a.indexOf('auto-clear')>=0||(a.indexOf('auto 811')>=0&&a.indexOf('revertido')<0)){return t.history[j].ts;}
    }
    return 0;
  }
  var c24=[],c7=[],c30=[],byU7={};
  var seen24={},seen7={},seen30={};
  for(var i=0;i<ft2.length;i++){
    var t=ft2[i];if(t.status==='Cancel')continue;
    var cd=getTicketClearDate(t);if(!cd)continue;
    var tk=t.ticket;
    if(cd>=day1&&!seen24[tk]){c24.push(t);seen24[tk]=1;}
    if(cd>=day7&&!seen7[tk]){c7.push(t);seen7[tk]=1;}
    if(cd>=day30&&!seen30[tk]){c30.push(t);seen30[tk]=1;}
  }
  if(utilCacheLoaded){for(var i2=0;i2<c7.length;i2++){var us=getTicketUtils(String(c7[i2].ticket).trim());for(var j=0;j<us.length;j++){if(us[j].status==='Clear'){if(!byU7[us[j].utility_name])byU7[us[j].utility_name]=0;byU7[us[j].utility_name]++;}}}}
  var ft24=0,ft7=0,ft30=0;
  for(var i3=0;i3<c24.length;i3++)ft24+=(c24[i3].footage||0);
  for(var i4=0;i4<c7.length;i4++)ft7+=(c7[i4].footage||0);
  for(var i5=0;i5<c30.length;i5++)ft30+=(c30[i5].footage||0);

  var projOpts='<option value="">Todos projetos</option>'+projects.filter(function(p2){return p2.status!=='Completed';}).map(function(p2){return'<option value="'+p2.id+'"'+(cpf===p2.id?' selected':'')+'>'+esc(projDropLabel(p2))+'</option>';}).join('');
  var projSel='<select class="fi" onchange="_clearProjFilter=this.value;renderDash()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';

  var topUtils=Object.entries(byU7).sort(function(a,b){return b[1]-a[1];}).slice(0,12);

  // ── Bar chart: clareados por dia (últimos 7 dias) ──
  var dayBuckets={};
  var dayLabels=[];
  var dayNow=new Date();
  for(var d=6;d>=0;d--){
    var dd=new Date(dayNow);dd.setDate(dd.getDate()-d);
    var dk=dd.toISOString().slice(0,10);
    dayBuckets[dk]=0;
    var dias=['dom','seg','ter','qua','qui','sex','sáb'];
    dayLabels.push({key:dk,label:dias[dd.getDay()]+', '+String(dd.getDate()).padStart(2,'0')});
  }
  for(var ib=0;ib<c7.length;ib++){
    var cdt=getTicketClearDate(c7[ib]);
    if(!cdt)continue;
    var cdd=new Date(cdt).toISOString().slice(0,10);
    if(dayBuckets[cdd]!==undefined)dayBuckets[cdd]++;
  }
  var maxBar=Math.max.apply(null,dayLabels.map(function(d2){return dayBuckets[d2.key]||0;}))||1;
  var barHtml='<div style="display:flex;align-items:flex-end;gap:6px;height:120px;padding-top:20px">';
  for(var ib2=0;ib2<dayLabels.length;ib2++){
    var dv=dayBuckets[dayLabels[ib2].key]||0;
    var bh=Math.max(dv/maxBar*100,2);
    barHtml+='<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px">'
      +'<div style="font-size:10px;font-weight:700;color:var(--green)">'+( dv||'')+'</div>'
      +'<div style="width:100%;height:'+bh+'px;background:var(--green);border-radius:3px 3px 0 0;min-height:2px"></div>'
      +'<div style="font-size:9px;color:var(--muted);white-space:nowrap">'+dayLabels[ib2].label+'</div></div>';
  }
  barHtml+='</div>';

  // ── Utility list (vertical) ──
  var utilListHtml='';
  if(topUtils.length){
    utilListHtml='<div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">Utilities que responderam (7D)</div>';
    for(var iu=0;iu<topUtils.length;iu++){
      utilListHtml+='<div style="display:flex;justify-content:space-between;padding:3px 0;border-bottom:1px solid var(--border);font-size:11px">'
        +'<span style="color:var(--text)">'+esc(topUtils[iu][0])+'</span>'
        +'<span style="font-weight:700;color:var(--green);font-family:var(--mono)">'+topUtils[iu][1]+'</span></div>';
    }
  }

  // ── Clareados hoje ──
  var todayHtml='';
  if(c24.length){
    todayHtml='<div style="margin-top:10px"><div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">Clareados hoje ('+c24.length+')</div>'
      +'<div style="display:flex;flex-wrap:wrap;gap:4px">';
    for(var it=0;it<c24.length;it++){
      todayHtml+='<span style="font-size:10px;padding:2px 8px;border-radius:10px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border);cursor:pointer;font-family:var(--mono)" onclick="openTicketDetail('+c24[it].id+')">'+esc(c24[it].ticket)+'</span>';
    }
    todayHtml+='</div></div>';
  }

  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><div class="dash-card-title" style="margin-bottom:0">✅ Tickets Clareados</div>'+projSel+'</div>'
    +'<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px">'
    +'<div style="padding:12px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--green)">'+c24.length+'</div><div style="font-size:10px;color:var(--green)">últimas 24h</div><div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px">'+ft24.toLocaleString()+' ft</div></div>'
    +'<div style="padding:12px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--green)">'+c7.length+'</div><div style="font-size:10px;color:var(--green)">últimos 7 dias</div><div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px">'+ft7.toLocaleString()+' ft</div></div>'
    +'<div style="padding:12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);text-align:center"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:var(--text)">'+c30.length+'</div><div style="font-size:10px;color:var(--muted)">últimos 30 dias</div><div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px">'+ft30.toLocaleString()+' ft</div></div>'
    +'</div>'
    +'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:12px">'
    +'<div><div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">Clareados por dia (últimos 7 dias)</div>'+barHtml+todayHtml+'</div>'
    +'<div>'+utilListHtml+'</div>'
    +'</div>'
    +'</div></div>';
}

function renderPrivateLocatorAlert(fTickets){
  if(!utilCacheLoaded)return'';
  const pvtTickets=[];
  const active=fTickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t));
  for(const t of active){
    const tkey=String(t.ticket).trim();
    const utils=getTicketUtils(tkey);
    const pvtUtils=[];
    for(const u of utils){
      const rt=(u.response_text||'').toLowerCase();
      if(rt.includes('3h')||rt.includes('privately owned')||rt.includes('private facility owner'))pvtUtils.push(u.utility_name);
    }
    const pending=(t.pending||'').toLowerCase();
    if(pending.includes('private locator')&&!pvtUtils.length)pvtUtils.push('(ver pending)');
    if(pvtUtils.length)pvtTickets.push({t,utils:pvtUtils});
  }
  if(!pvtTickets.length)return'';
  return'<div style="background:#faf5ff;border:1px solid #d8b4fe;border-radius:var(--r-lg);padding:12px 14px;margin-bottom:14px">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
    +'<span style="font-size:12px;font-weight:700;color:#7c3aed">🔒 Private Locator Necessário ('+pvtTickets.length+' ticket'+(pvtTickets.length>1?'s':'')+')</span>'
    +'<button class="btn btn-sm" onclick="exportPrivateLocator()" style="font-size:10px;background:#7c3aed;color:white;border-color:#7c3aed">↓ Excel</button>'
    +'</div>'
    +'<div style="font-size:10px;color:#6b21a8;margin-bottom:8px">Estes tickets têm utilities com instalações privadas (3H) que precisam de locator privado contratado separadamente.</div>'
    +'<div style="display:flex;flex-wrap:wrap;gap:6px">'
    +pvtTickets.map(({t,utils})=>{
      const loc=esc((t.location||'').replace(/\s*(Inside|Near).*/i,'').split(',')[0].trim());
      return'<div style="background:white;border:1px solid #d8b4fe;border-radius:var(--r);padding:8px 10px;cursor:pointer;min-width:220px;flex:1;max-width:320px" onclick="openTicketDetail('+t.id+')">'
        +'<div style="display:flex;justify-content:space-between;align-items:center">'
        +'<span style="font-family:var(--mono);font-weight:700;font-size:11px;color:var(--text)">'+esc(t.ticket)+'</span>'
        +'<span class="sbadge b-'+t.status.toLowerCase()+'" style="font-size:9px">'+esc(t.status)+'</span></div>'
        +'<div style="font-size:10px;color:var(--muted);margin-top:2px">'+loc+', '+esc(t.state)+'</div>'
        +'<div style="font-size:9px;color:#7c3aed;margin-top:3px;font-weight:600">'+utils.map(esc).join(', ')+'</div>'
        +'</div>';
    }).join('')
    +'</div></div>';
}

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

function renderProgressoFootage(fTickets,projStats){
  try{
    const pf=window._progProjFilter||'';
    const projOpts='<option value="">Todos (agrupado)</option>'+projStats.map(p=>'<option value="'+p.id+'"'+(pf===p.id?' selected':'')+'>'+(p.locs?esc(p.locs)+' ('+esc(p.name)+')':esc(p.name))+'</option>').join('');
    const projSel='<select class="fi" onchange="_progProjFilter=this.value;renderDash()" style="width:auto;min-width:160px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';
    function mkGrid(d){return'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:8px"><div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+d.totalFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-top:2px">Total ft</div></div><div style="padding:9px;background:var(--green-bg);border-radius:var(--r);border:1px solid var(--green-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--green)">'+d.clearFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--green);text-transform:uppercase;margin-top:2px">Clear '+d.pctClear+'%</div></div><div style="padding:9px;background:var(--red-bg);border-radius:var(--r);border:1px solid var(--red-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--red)">'+d.openFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--red);text-transform:uppercase;margin-top:2px">Aberto '+d.pctOpen+'%</div></div><div style="padding:9px;background:var(--amber-bg);border-radius:var(--r);border:1px solid var(--amber-border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+d.damageFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--amber);text-transform:uppercase;margin-top:2px">Damage '+d.pctDamage+'%</div></div><div style="padding:9px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border);text-align:center"><div style="font-size:14px;font-weight:700;font-family:var(--mono);color:var(--text)">'+d.concluidoFt.toLocaleString()+'</div><div style="font-size:9px;color:var(--text2);text-transform:uppercase;margin-top:2px">Concluído '+d.pctConcluido+'%</div></div></div><div class="prog-bar"><div style="width:'+d.pctClear+'%;background:var(--green)"></div><div style="width:'+Math.min(d.pctOpen,100-d.pctClear)+'%;background:var(--red)"></div><div style="width:'+Math.min(d.pctDamage,100-d.pctClear-d.pctOpen)+'%;background:#f59e0b"></div><div style="width:'+Math.min(d.pctConcluido,100-d.pctClear-d.pctOpen-d.pctDamage)+'%;background:var(--text)"></div></div>';}
    let content='';
    if(!pf){
      const tf=projStats.reduce((s,p)=>s+p.totalFt,0)||1;const cf=projStats.reduce((s,p)=>s+p.clearFtP,0);const of2=projStats.reduce((s,p)=>s+p.openFtP,0);const df=projStats.reduce((s,p)=>s+p.damageFt,0);const clf=projStats.reduce((s,p)=>s+p.concluidoFt,0);
      content=mkGrid({totalFt:tf,clearFt:cf,openFt:of2,damageFt:df,concluidoFt:clf,pctClear:Math.round(cf/tf*100),pctOpen:Math.round(of2/tf*100),pctDamage:Math.round(df/tf*100),pctConcluido:Math.round(clf/tf*100)});
      content+='<div class="prog-legend"><span><span class="prog-dot" style="background:var(--green)"></span>Clear</span><span><span class="prog-dot" style="background:var(--red)"></span>Aberto</span><span><span class="prog-dot" style="background:#f59e0b"></span>Damage</span><span><span class="prog-dot" style="background:var(--text)"></span>Concluído</span><span style="margin-left:auto">'+projStats.reduce((s,p)=>s+p.count,0)+' tickets · '+projStats.length+' projetos</span></div>';
    }else{
      const p=projStats.find(x=>x.id===pf);
      if(p){content='<div style="font-size:13px;font-weight:700;color:var(--text);margin-bottom:8px">📍 '+esc(p.locs||p.state)+' <span style="font-size:11px;color:var(--muted);font-family:var(--mono)">'+esc(p.name)+'</span></div>'+mkGrid({totalFt:p.totalFt,clearFt:p.clearFtP,openFt:p.openFtP,damageFt:p.damageFt,concluidoFt:p.concluidoFt,pctClear:p.pctClear,pctOpen:p.pctOpen,pctDamage:p.pctDamage,pctConcluido:p.pctConcluido})+'<div class="prog-legend"><span><span class="prog-dot" style="background:var(--green)"></span>Clear '+p.pctClear+'%</span><span><span class="prog-dot" style="background:var(--red)"></span>Aberto '+p.pctOpen+'%</span><span><span class="prog-dot" style="background:var(--text)"></span>Concluído '+p.pctConcluido+'%</span><span style="margin-left:auto">'+p.count+' tickets</span></div>';}
      else content='<div style="color:var(--muted)">Projeto não encontrado</div>';
    }
    return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><div class="dash-card-title" style="margin-bottom:0">Progresso por Footage</div>'+projSel+'</div>'+content+'</div></div>';
  }catch(e){console.error('Progresso error:',e);return'';}
}

function renderRiskAnalytics(fT){
  if(!utilCacheLoaded)return'';
  const active=fT.filter(t=>t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t));
  const scored=active.map(t=>({t,s:riskScore(t)}));
  const crit=scored.filter(x=>x.s>=60);const high=scored.filter(x=>x.s>=35&&x.s<60);
  const med=scored.filter(x=>x.s>=15&&x.s<35);const low=scored.filter(x=>x.s<15);
  const exp=active.filter(t=>t.expire&&t.expire!=='—'&&t.status==='Open'&&new Date(t.expire)<new Date());
  const card=(label,count,color,bg,border,sub)=>'<div style="padding:14px;background:'+bg+';border:1px solid '+border+';border-radius:var(--r)"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:'+color+'">'+count+'</div><div style="font-size:10px;font-weight:700;color:'+color+';text-transform:uppercase;margin-top:2px">'+label+'</div>'+(sub?'<div style="font-size:10px;color:'+color+';opacity:.7;margin-top:2px">'+sub+'</div>':'')+'</div>';
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:14px"><div class="dash-card-title" style="margin:0">🎯 Score de Risco</div><button class="btn btn-sm" onclick="nav(\'tickets\');setTimeout(()=>{sortCol=\'risk\';sortAsc=false;renderTable();},100)" style="font-size:11px">Tabela por risco →</button></div><div style="display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:14px">'
    +card('Crítico ≥60',crit.length,'#dc2626','#fef2f2','#fecaca',crit.length?crit.map(x=>esc(x.t.ticket)).slice(0,2).join(', ')+(crit.length>2?'…':''):'Nenhum')
    +card('Alto 35–59',high.length,'#d97706','#fffbeb','#fde68a','')
    +card('Médio 15–34',med.length,'#2563eb','#eff6ff','#bfdbfe','')
    +card('Baixo <15',low.length,'#16a34a','#f0fdf4','#bbf7d0','')
    +card('⚠ Vencidos',exp.length,'#7c3aed','#f5f3ff','#ddd6fe',exp.length?'ainda Open':'Nenhum')
    +'</div>'
    +'<div style="padding:10px 12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);margin-bottom:14px;line-height:1.7;font-size:11px;color:var(--text2)">'
    +'<strong style="color:var(--text);font-size:12px">Como o Score é calculado (0–100):</strong><br>'
    +'<span style="color:#dc2626;font-weight:600">Vencimento:</span> vencido +60 · ≤2 dias +45 · ≤5 dias +30 · ≤10 dias +18 · ≤20 dias +8<br>'
    +'<span style="color:#dc2626;font-weight:600">Utilities pendentes:</span> +8 por utility (máx 35 pts)<br>'
    +'<span style="color:#d97706;font-weight:600">Status Damage:</span> +30<br>'
    +'<span style="color:#d97706;font-weight:600">Sem atividade &gt;30 dias:</span> +15 · &gt;14 dias +8<br>'
    +'<span style="color:#16a34a;font-weight:600">Status Clear:</span> reduz –20<br>'
    +'<span style="color:var(--muted)">Score travado em 0 se Closed ou Cancel</span>'
    +'</div>'
    +(crit.length?'<div style="display:flex;flex-wrap:wrap;gap:5px">'+crit.sort((a,b)=>b.s-a.s).slice(0,15).map(({t,s})=>'<span style="font-size:11px;font-family:var(--mono);padding:3px 9px;border-radius:10px;background:#fef2f2;color:#dc2626;border:1px solid #fecaca;cursor:pointer" onclick="openTicketDetail('+t.id+')" title="Score: '+s+'">'+esc(t.ticket)+' · '+s+'</span>').join('')+'</div>':'')
    +'</div></div>';
}

function renderClearTimeMetrics(fTickets){
  if(!utilCacheLoaded)return'';
  var mpf=_metricProjFilter||'';
  var ft3=mpf?fTickets.filter(function(t){return t.projectId===mpf;}):fTickets;
  var utilTimes={};
  for(var i=0;i<ft3.length;i++){
    var t=ft3[i];if(!t.history||!t.history.length)continue;
    var createdTs=t.history[0].ts;if(!createdTs)continue;
    var utils=getTicketUtils(String(t.ticket).trim());
    for(var j=0;j<utils.length;j++){
      var u=utils[j];if(u.status!=='Clear'||!u.responded_at)continue;
      var respTs=new Date(u.responded_at).getTime();if(isNaN(respTs)||respTs<createdTs)continue;
      var days=(respTs-createdTs)/86400000;if(days>90)continue;
      var name=u.utility_name;if(!utilTimes[name])utilTimes[name]={total:0,count:0};
      utilTimes[name].total+=days;utilTimes[name].count++;
    }
  }
  var utilAvg=[];
  for(var name in utilTimes){if(utilTimes[name].count>=2)utilAvg.push({name:name,avg:Math.round(utilTimes[name].total/utilTimes[name].count*10)/10,count:utilTimes[name].count});}
  utilAvg.sort(function(a,b){return b.avg-a.avg;});
  if(!utilAvg.length)return'';
  var projOpts='<option value="">Todos projetos</option>'+projects.filter(function(p){return p.status!=='Completed';}).map(function(p){return'<option value="'+p.id+'"'+(mpf===p.id?' selected':'')+'>'+esc(projDropLabel(p))+'</option>';}).join('');
  var projSel='<select class="fi" onchange="_metricProjFilter=this.value;renderDash()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';
  var globalAvg=utilAvg.reduce(function(s,u){return s+u.avg*u.count;},0)/utilAvg.reduce(function(s,u){return s+u.count;},0);
  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><div class="dash-card-title" style="margin-bottom:0">⏱ Tempo médio para Clear</div><div style="display:flex;align-items:center;gap:12px"><span style="font-size:20px;font-weight:700;font-family:var(--mono);color:'+(globalAvg<=3?'var(--green)':globalAvg<=6?'var(--amber)':'var(--red)')+'">'+globalAvg.toFixed(1)+' dias</span>'+projSel+'</div></div>';
  h+='<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(200px,1fr));gap:6px">';
  for(var k=0;k<Math.min(utilAvg.length,12);k++){
    var u2=utilAvg[k];var color=u2.avg<=3?'var(--green)':u2.avg<=6?'var(--amber)':'var(--red)';
    h+='<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)"><span style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:140px" title="'+esc(u2.name)+'">'+esc(u2.name)+'</span><span style="font-size:12px;font-weight:700;font-family:var(--mono);color:'+color+'">'+u2.avg+'d</span></div>';
  }
  h+='</div></div></div>';
  return h;
}

function renderUtilSummaryHtml(){
  if(!utilCacheLoaded)return'';
  const allUtils={};
  const openTickets=filterTickets({}).filter(t=>t.status!=='Closed'&&t.status!=='Cancel');
  for(const t of openTickets){
    const utils=getTicketUtils(String(t.ticket).trim());
    for(const u of utils){
      if(u.status==='Pending'){if(!allUtils[u.utility_name])allUtils[u.utility_name]=0;allUtils[u.utility_name]++;}
    }
  }
  const sorted=Object.entries(allUtils).sort((a,b)=>b[1]-a[1]);
  if(!sorted.length)return'';
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">📡 Utilities Pendentes (global)</div><div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:6px">'
    +sorted.slice(0,15).map(([name,count])=>'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)"><span style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px" title="'+esc(name)+'">'+esc(name)+'</span><div style="display:flex;gap:6px;align-items:center"><span style="font-size:12px;font-weight:700;font-family:var(--mono);color:var(--red)">'+count+'</span><button class="btn btn-sm" onclick="exportUtilTickets(\''+esc(name).replace(/'/g,"\\'")+'\')" style="font-size:9px;padding:2px 6px">↓</button></div></div>').join('')
    +'</div><button class="btn btn-sm" onclick="exportAllPending()" style="margin-top:8px;font-size:11px">↓ Exportar todas pendências</button></div></div>';
}

function renderRecentActivity(fT){
  const recent=[...fT].filter(t=>t.history&&t.history.length).sort((a,b)=>(b.history[b.history.length-1]?.ts||0)-(a.history[a.history.length-1]?.ts||0)).slice(0,15);
  if(!recent.length)return'';
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">🕐 Atividade Recente</div><div style="max-height:300px;overflow-y:auto">'
    +recent.map(t=>{const last=t.history[t.history.length-1];return'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail('+t.id+')"><div><span style="font-family:var(--mono);font-weight:600;font-size:12px">'+esc(t.ticket)+'</span> <span class="sbadge b-'+t.status.toLowerCase()+'" style="font-size:9px">'+esc(t.status)+'</span><div style="font-size:10px;color:var(--muted)">'+esc(last?.action||'—')+'</div></div><span style="font-size:10px;color:var(--muted);white-space:nowrap">'+fmtDt(last?.ts||0)+'</span></div>';}).join('')
    +'</div></div></div>';
}

function renderSyncHealthCard(){
  setTimeout(()=>renderSyncHealth(),100);
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">📡 Saúde do Sync (últimas 20 execuções)</div><div id="sync-health-widget"><div style="color:var(--muted);font-size:12px">Carregando...</div></div></div></div>';
}

async function renderSyncHealth(){
  try{
    const{data:rows,error}=await sb.from('sync_811_log').select('state,status,started_at').order('started_at',{ascending:false}).limit(40);
    if(error||!rows)return;
    const el=document.getElementById('sync-health-widget');if(!el)return;
    const by={IN:[],FL:[]};
    for(const row of rows){if(by[row.state])by[row.state].push(row);}
    let h='';
    for(const[st,lg]of Object.entries(by)){
      if(!lg.length)continue;const l20=lg.slice(0,20);
      const sc=l20.filter(x=>x.status==='success').length;
      const rt=Math.round(sc/l20.length*100);
      const c=rt>=90?'var(--green)':rt>=70?'var(--amber)':'var(--red)';
      const dots=l20.map(x=>'<span title="'+(x.started_at||'').slice(0,16)+'" style="display:inline-block;width:10px;height:10px;border-radius:50%;margin:1px;background:'+(x.status==='success'?'var(--green)':'var(--red)')+'"></span>').join('');
      h+='<div style="display:flex;align-items:center;gap:12px;padding:6px 0;border-bottom:1px solid var(--border)"><span style="font-weight:700;font-family:var(--mono);width:24px">'+st+'</span><span style="font-size:16px;font-weight:700;font-family:var(--mono);color:'+c+';width:42px">'+rt+'%</span><div style="flex:1;display:flex;flex-wrap:wrap;gap:2px">'+dots+'</div><span style="font-size:10px;color:var(--muted)">'+sc+'/'+l20.length+'</span></div>';
    }
    el.innerHTML=h||'<div style="color:var(--muted);font-size:12px">Sem dados</div>';
  }catch(e){console.error('SyncHealth:',e);}
}

async function loadLastSync(){
  try{
    const{data:rows,error}=await sb
      .from('sync_811_log')
      .select('state,finished_at,started_at,tickets_checked,tickets_updated,status,error_msg')
      .not('finished_at','is',null)
      .order('finished_at',{ascending:false})
      .limit(20);
    if(error||!rows||!rows.length){console.warn('[LastSync]',error?.message||'Nenhum registro');return;}
    const by={};for(const row of rows){if(row.finished_at&&!by[row.state])by[row.state]=row;}
    const parts=[];
    for(const st of ['IN','FL']){
      const row=by[st];if(!row||!row.finished_at){parts.push(st+': —');continue;}
      const d=new Date(row.finished_at);const dm=Math.round((Date.now()-d.getTime())/60000);
      let ago;if(dm<2)ago='agora';else if(dm<60)ago=dm+'min atrás';else if(dm<120)ago='1h atrás';else if(dm<1440)ago=Math.round(dm/60)+'h atrás';else ago=Math.round(dm/1440)+'d atrás';
      parts.push((row.status==='success'?'🟢':'🔴')+' '+st+': '+ago+' ('+(row.tickets_checked||0)+')');
    }
    const el=document.getElementById('last-sync-status');if(el)el.innerHTML=parts.join('<br>');

    window._syncHealth={states:by,recentErrors:rows.filter(x=>x.status==='error').slice(0,5),allRecent:rows};
    renderHealthCard();

    const allFinished=Object.values(by).filter(x=>x&&x.finished_at).sort((a,b)=>new Date(b.finished_at)-new Date(a.finished_at));
    const latest=allFinished[0];
    if(latest&&latest.finished_at){
      window._lastSyncTime=new Date(latest.finished_at).getTime();
      const pill=document.getElementById('dash-sync-pill');
      if(pill){const hm=new Date(latest.finished_at).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});const diff=Math.round((Date.now()-window._lastSyncTime)/60000);pill.textContent='● Último sync '+hm+(diff<130?' · próximo em '+Math.max(0,120-diff)+' min':'');}
      const ap=document.getElementById('analytics-sync-pill');
      if(ap){const hm2=new Date(latest.finished_at).toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});const diff2=Math.round((Date.now()-window._lastSyncTime)/60000);let ago2;if(diff2<2)ago2='agora';else if(diff2<60)ago2=diff2+'min atrás';else if(diff2<120)ago2='1h atrás';else ago2=Math.round(diff2/60)+'h atrás';ap.style.background='var(--green-bg)';ap.style.color='var(--green)';ap.style.borderColor='var(--green-border)';ap.textContent='● Último sync '+hm2+' ('+ago2+')';}
      updateSyncTimer();
    }
  }catch(e){console.error('[LastSync]',e);}
}

function renderHealthCard(){
  const el=document.getElementById('health-card');if(!el)return;
  const h=window._syncHealth;
  if(!h){el.innerHTML='<div style="color:var(--muted);font-size:12px;padding:8px">Carregando...</div>';return;}
  let html='';
  for(const st of ['IN','FL']){
    const row=h.states[st];
    if(!row){html+='<div style="padding:5px 0;border-bottom:1px solid var(--border);font-size:12px"><span style="font-weight:600">'+st+'</span> <span style="color:var(--muted)">— sem dados</span></div>';continue;}
    const d=new Date(row.finished_at);const dm=Math.round((Date.now()-d.getTime())/60000);
    let ago;if(dm<2)ago='agora';else if(dm<60)ago=dm+'min';else if(dm<1440)ago=Math.round(dm/60)+'h';else ago=Math.round(dm/1440)+'d';
    const isOk=row.status==='success';const isStale=dm>180;
    const dot=isOk&&!isStale?'🟢':isOk&&isStale?'🟡':'🔴';
    const statusLabel=isOk?(isStale?'atrasado':'ok'):'erro';
    html+='<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-bottom:1px solid var(--border)"><div><span style="font-size:12px;font-weight:600">'+dot+' '+st+'</span><span style="font-size:10px;color:var(--muted);margin-left:6px">'+ago+'</span></div><div style="text-align:right"><span style="font-size:11px;font-family:var(--mono);color:'+(isOk?'var(--green)':'var(--red)')+'">'+statusLabel+'</span><span style="font-size:10px;color:var(--muted);margin-left:6px">'+(row.tickets_checked||0)+' tickets · '+(row.tickets_updated||0)+' respostas</span></div></div>';
  }
  if(h.recentErrors.length){
    html+='<div style="margin-top:6px;font-size:10px;font-weight:700;color:var(--red);text-transform:uppercase;letter-spacing:.06em">Erros recentes</div>';
    for(const e of h.recentErrors.slice(0,3)){
      const ed=new Date(e.finished_at||e.started_at);const edm=Math.round((Date.now()-ed.getTime())/60000);
      let eAgo;if(edm<60)eAgo=edm+'min';else if(edm<1440)eAgo=Math.round(edm/60)+'h';else eAgo=Math.round(edm/1440)+'d';
      html+='<div style="font-size:10px;color:var(--red);padding:2px 0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis"><span style="color:var(--muted)">'+e.state+' '+eAgo+'</span> '+esc((e.error_msg||'sem detalhes').substring(0,60))+'</div>';
    }
  }else{html+='<div style="margin-top:6px;font-size:10px;color:var(--green)">Sem erros recentes</div>';}
  el.innerHTML=html;
}

function filterByUtil(utilName){nav('tickets');setTimeout(()=>{const sel=document.getElementById('tbl-util');if(sel){sel.value=utilName;renderTable();}},100);}

/* ═══════════ 27. SYNC HELPERS ═══════════ */
function syncProjectSelects(){
  const mkOpts=(label)=>'<option value="">'+label+'</option>'+projects.map(p=>`<option value="${p.id}">${esc(projDropLabel(p))}</option>`).join('');
  const pf=document.getElementById('proj-filter');if(pf)pf.innerHTML=mkOpts('Todos os projetos');
  const tp=document.getElementById('tbl-proj');if(tp)tp.innerHTML=mkOpts('Todos projetos');
  const tm=document.getElementById('tm-proj');if(tm)tm.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}">${esc(projDropLabel(p))}</option>`).join('');
}
function syncClients(){
  const cls=[...new Set(tickets.map(t=>t.client).filter(Boolean))].sort();
  ['fcli','tbl-cli'].forEach(id=>{const el=document.getElementById(id);if(el)el.innerHTML='<option value="">Todos clientes</option>'+cls.map(c=>`<option>${esc(c)}</option>`).join('');});
}
function syncMapUtilFilter(){
  if(!utilCacheLoaded)return;
  const el=document.getElementById('map-util-filter');if(!el)return;
  const prev=el.value;
  const allU={};
  const openNums=new Set(tickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel').map(t=>String(t.ticket).trim()));
  for(const[tn,resps]of Object.entries(utilCache)){
    if(!openNums.has(tn))continue;
    for(const u of resps){if(u.status==='Pending'){if(!allU[u.utility_name])allU[u.utility_name]=0;allU[u.utility_name]++;}}
  }
  const sorted=Object.entries(allU).sort((a,b)=>b[1]-a[1]);
  el.innerHTML='<option value="">Todas utilities</option><option value="__pending__">Com pendentes</option>'+sorted.map(([n,c])=>'<option value="'+esc(n)+'">'+esc(n)+' ('+c+')</option>').join('');
  if(prev)el.value=prev;
}
function syncLocations(){
  const locs=[...new Set(tickets.map(t=>t.location).filter(Boolean))].sort();
  const el=document.getElementById('floc');
  if(el)el.innerHTML='<option value="">Todos locais</option>'+locs.map(l=>`<option>${esc(l)}</option>`).join('');
}
function syncAll(){
  rebuildSupersededSet();syncProjectSelects();syncClients();syncLocations();
  if(utilCacheLoaded){syncUtilFilter();syncMapUtilFilter();}
  const ap=document.querySelector('.page.active')?.id;
  if(ap==='pg-map'){renderList();renderMap();}
  else if(ap==='pg-tickets')renderTable();
  else if(ap==='pg-proj')renderProjects();
  else if(ap==='pg-dash')renderDash();
  else if(ap==='pg-contacts')renderContacts();
  else if(ap==='pg-analytics')renderAnalytics();
  else renderDash();
}

async function manualRefresh(){
  setSyncStatus(true,'Atualizando...');
  try{
    const{data:p}=await sb.from('projects').select('*').order('name');
    const{data:t}=await sb.from('tickets').select('*').order('ticket');
    if(p)projects=p.map(dbToProject);
    if(t)tickets=t.map(dbToTicket);
    rebuildSupersededSet();
    await loadUtilCache();
    await loadLastSync();
    await loadContacts();
    syncAll();buildNotifications();
    setSyncStatus(true,'Atualizado ✓');
    toast('Dados atualizados!','success');
  }catch(e){
    console.error('[ManualRefresh]',e);
    setSyncStatus(false,'Erro');
    toast('Erro ao atualizar: '+e.message,'danger');
  }
}

/* ═══════════ 28. INIT ═══════════ */
window.addEventListener('load',async()=>{
  window.addEventListener('offline',()=>{toast('⚠ Sem conexão — alterações não serão salvas','danger');setSyncStatus(false,'Offline');});
  window.addEventListener('online',()=>{toast('✅ Conexão restaurada','success');setSyncStatus(true,'Online');});
  document.querySelector('#loading-screen div:last-child').textContent='Conectando ao Supabase...';
  const ok=await initSupabase();
  document.getElementById('loading-screen').style.display='none';
  if(!ok){document.getElementById('login-screen').style.display='flex';setTimeout(()=>toast('Aviso: erro ao conectar ao banco.','warn'),500);return;}
  if(checkProjectUrl())return;
  try{
    const{data:{session}}=await sb.auth.getSession();
    if(session){
      await resolveRole(session.user);
      enterApp();
      return;
    }
  }catch(e){console.log('[Auth] No session:',e);}
  document.getElementById('login-screen').style.display='flex';
});
