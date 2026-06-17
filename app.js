
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
// Item 4: config via meta tag no HTML (fallback hardcoded mantido por seguranca)
const SUPABASE_URL=document.querySelector('meta[name="supabase-url"]')?.content||'https://ofbqtaulvzeltfpqcjhh.supabase.co';
const SUPABASE_KEY=document.querySelector('meta[name="supabase-anon-key"]')?.content||'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9mYnF0YXVsdnplbHRmcHFjamhoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ0MDMyMjAsImV4cCI6MjA4OTk3OTIyMH0.zPU8SCUAVrTOxp-cuKupXBt0QgRkxnLcpScwnHJKVWE';
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

/** Retorna Date no final do dia (23:59:59) — tickets vencem às 23:59, não à meia-noite.
 *  Fix bug #11: parse explícito MM/DD/YYYY em vez de depender de new Date(string), que é
 *  unreliable em Safari antigo/mobile e pode retornar Invalid Date com format MM/DD/YYYY.
 *  O sistema inteiro usa MM/DD/YYYY nos tickets (padrão US), então tratamos como primário
 *  e só caímos pro parser nativo como fallback (ISO, etc).
 */
function _eod(dateStr){
  if(!dateStr)return new Date(NaN);
  const s=String(dateStr).trim();
  // Primário: MM/DD/YYYY ou MM/DD/YY (formato que o sistema usa em todos os tickets)
  const m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if(m){
    const mo=parseInt(m[1],10), dy=parseInt(m[2],10);
    let yr=parseInt(m[3],10);
    if(yr<100)yr+=2000;
    if(mo>=1&&mo<=12&&dy>=1&&dy<=31){
      return new Date(yr, mo-1, dy, 23, 59, 59, 999);
    }
  }
  // Fallback: parser nativo pra ISO, etc
  const d=new Date(s);
  if(!isNaN(d.getTime())){ d.setHours(23,59,59,999); return d; }
  return new Date(NaN);
}

/** Cor do status */
function scol(s){
  const m={open:'#dc2626',clear:'#16a34a',damage:'#db2777',closed:'#1a1a18',cancel:'#6d28d9'};
  return m[(s||'').toLowerCase()]||'#9a9888';
}
function tipoDash(t){return(t||'').toLowerCase().includes('main')?null:'6,4';}
function lineWeight(t){return(t||'').toLowerCase().includes('main')?5:3;}

/* ═══════════ 3. STATE ═══════════ */
let sb,projects=[],tickets=[],parsed=[],parsedProjectTotals={},parsedProjectCoords={};
let isAdmin=false,role='viewer',isSharedView=false,sharedProjectId=null,currentUserEmail='';
let currentDetailId=null,currentPanelId=null,editingTicketId=null,editingProjectId=null,deletingProjectId=null;
let sortCol='ticket',sortAsc=true;
let mf={open:true,damage:true,clear:true,closed:false,cancel:false};
let _graceFilterActive=false; // Toggle "🔄 Em carência" na aba Tickets — mostra só tickets renovados ainda em período de graça
let map,satL,strL,hybL,mkrs=[],lines=[],labels=[];
let shMap,shSatL,shStrL,shHybL,shMkrs=[],shLines=[],shLabels=[];
let clusterGroup=null;
let fieldDrawing=false,fieldPts=[],fieldLine=null,fieldTicketId=null;
let utilCache={},utilCacheLoaded=false;
let dashStateVal='';
let _soonDays=10,_metricProjFilter='',_metricPrimeFilter='',_clearProjFilter='',_progProjFilter='',_velProjFilter='',_analyticsScope='all';
// Qual card expandiu os tickets clareados: 'today' | '7d' | '30d' | null
let _clearedExpand=null;
// Qual dia do bar chart expandiu (formato YYYY-MM-DD) | null.
// Exclusivo com _clearedExpand — só um dos dois fica aberto por vez.
let _clearedExpandDay=null;
let utilContacts=[],editingContactId=null,_hasContactEmailCol=false;
// Email do contato sem coluna no banco (Eric não roda DDL): guardado embutido no campo notes ("✉ email@x").
const _EMAIL_RE=/[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}/;
function _emailFromNotes(notes){const m=(notes||'').match(_EMAIL_RE);return m?m[0]:'';}
function _contactEmail(c){return(((c&&c.email)||'').trim())||_emailFromNotes(c&&c.notes);}
function _notesClean(notes){return(notes||'').replace(_EMAIL_RE,'').replace(/✉/g,'').replace(/\s*[·|]\s*$/,'').replace(/^\s*[·|]\s*/,'').replace(/\s{2,}/g,' ').trim();}
// Fase 3 do filtro county: cobertura utility × county (auto-derivada pelo Python)
let utilCoverage=[];
// County pré-selecionado quando usuário abre a aba Contatos vindo de um ticket específico
let _contactsPreselectCounty='';
// Cache dos damages do ticket atualmente aberto no modal (ticket_damages rows)
let _currentDamages=[];
// ID do damage em edição (null = registrar novo)
let _editingDamageId=null;
let supersededSet=new Set();
let miniMap=null;
let _expAlertEl=null;
let _t2; // toast timer
let _hasCompletedFeetCol=false; // Detectado no init: coluna completed_feet existe no banco?
let _autoRefreshId=null; // fix bug #2: guardar ID do setInterval de auto-refresh pra limpar antes de recriar
let _sharedRefreshId=null; // mesmo padrão pro auto-refresh do shared view (link público)
let _syncTimerId=null;   // fix bug #3: guardar ID do setTimeout de updateSyncTimer pra evitar cascata

// Debounced renderers
const debouncedRedraw=debounce(()=>redrawAll(),250);
const debouncedTable=debounce(()=>renderTable(),250);
const debouncedContacts=debounce(()=>renderContacts(),250);

/* ═══════════ 4. SUPERSEDED SET ═══════════ */
function rebuildSupersededSet(){
  supersededSet=new Set();
  for(const t of tickets){
    const chain=String(t.oldTicket2||'').trim();
    if(!chain)continue;
    // Suporta cadeias: "OLD1 → OLD2 → OLD3" → adiciona cada num individualmente.
    // Anomalia conhecida (2026-06-05): renewTicket pode gravar o próprio número
    // no chain (ex: X261480125 com oldTicket2="A261141153 → X261480125"). Nesse caso,
    // sem o filtro abaixo, o próprio ticket seria marcado como supersedido — e o mapa
    // pulava de desenhar o trajeto dele. Filtra: nunca incluir o ticket nele mesmo.
    const self=String(t.ticket||'').trim();
    const parts=chain.split(/\s*→\s*/);
    for(const p of parts){const num=p.trim();if(num&&num!==self)supersededSet.add(num);}
  }
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
    prime='',
    search='',
    state='',
    utility='',
    excludeSuperseded=true,
    excludeCompleted=true,
    statusFilter=null,
    mapUtilFilter='',
    onlyGrace=false,
    expireDays=''
  }=opts;

  // Pre-compute completed project IDs for fast lookup
  const completedProjIds=excludeCompleted?new Set(projects.filter(p=>p.status==='Completed').map(p=>p.id)):null;

  const sr=search.toLowerCase();

  return tickets.filter(t=>{
    // Exclude tickets from completed projects
    if(completedProjIds&&t.projectId&&completedProjIds.has(t.projectId)) return false;

    // Superseded
    if(excludeSuperseded && isSuperseded(t)) return false;

    // Status exato (usa status real — effectiveStatus é só visual)
    if(status && t.status!==status) return false;

    // Status filter (mapa checkboxes) — usa effectiveStatus pra respeitar
    // a carencia: ticket renovado em grace com status=Open mas effective=Clear
    // deve aparecer quando filtro = Clear (visualmente esta Clear pro operador).
    // Closed/Cancel sao raw (effectiveStatus nao muda eles).
    if(statusFilter){
      const sl=(effectiveStatus(t)||'').toLowerCase();
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

    // Prime
    if(prime && t.prime!==prime) return false;

    // Estado
    if(state && t.state!==state) return false;

    // Busca textual
    // Fix bug #7: (t.ticket||'') pra evitar crash se ticket vier null/undefined do banco.
    // Também inclui job e notes na busca (bug #22): supervisor que busca "Job #4521" ou nota.
    if(sr && !(t.ticket||'').toLowerCase().includes(sr)
          && !(t.client||'').toLowerCase().includes(sr)
          && !(t.location||'').toLowerCase().includes(sr)
          && !(t.address||'').toLowerCase().includes(sr)
          && !(t.prime||'').toLowerCase().includes(sr)
          && !(t.job||'').toLowerCase().includes(sr)
          && !(t.notes||'').toLowerCase().includes(sr)) return false;

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

    // "Em carência" — só tickets renovados que ainda estão no período de graça.
    // Depende de utilCacheLoaded indiretamente (via isInRenewalGrace → newTicketFullyCleared);
    // o toggleGraceFilter já guarda contra clique prematuro.
    if(onlyGrace && !(isRenewed(t) && isInRenewalGrace(t))) return false;

    if(expireDays){
      const exp=t.expire;
      if(!exp||exp==='—')return false;
      const ed=_eod(exp);
      if(!ed||isNaN(ed.getTime()))return false;
      const now=new Date();
      if(expireDays==='expired'){if(ed>=now)return false;}
      else{const days=parseInt(expireDays,10);const cutoff=new Date();cutoff.setDate(cutoff.getDate()+days);if(ed<now||ed>cutoff)return false;}
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

/* ═══════════ 6b. HELPERS: Baixa parcial (progresso campo) ═══════════ */
// Formata footage com progresso parcial: "800 / 1,000 ft (80%)" ou "1,000 ft" se sem baixa
function fmtProgress(t){
  const total=t.footage||0;const done=t.completedFeet||0;
  if(!total&&!done)return'0 ft';
  if(!done)return total.toLocaleString()+' ft';
  const pct=total>0?Math.min(Math.round(done/total*100),100):0;
  return done.toLocaleString()+' / '+total.toLocaleString()+' ft'+(total>0?' ('+pct+'%)':'');
}
// Mini barra de progresso inline (HTML). Retorna '' se sem baixa parcial.
function miniProgressBar(t,width){
  const total=t.footage||0;const done=t.completedFeet||0;
  if(!done||!total)return'';
  const pct=Math.min(Math.round(done/total*100),100);
  const c=pct>=100?'var(--green)':pct>=50?'#3b82f6':'var(--amber)';
  const w=width||'100%';
  return'<div style="width:'+w+';height:5px;background:var(--border);border-radius:3px;overflow:hidden;margin-top:3px">'
    +'<div style="width:'+pct+'%;height:100%;background:'+c+';border-radius:3px;transition:width .3s"></div></div>';
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
function _toIsoDate(mmddyyyy){
  if(!mmddyyyy)return'';
  const m=String(mmddyyyy).match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if(!m)return'';
  return m[3]+'-'+m[1].padStart(2,'0')+'-'+m[2].padStart(2,'0');
}
/** Normaliza valores de expire vindos do banco/portal pra 'MM/DD/YYYY' ou ''.
 * Lida com legado poluído (ex: "04/15/26 Time: 23:59", "05/13/26 Time: 23:59ET"),
 * ano 2 dígitos, sufixos de timezone. Espelho exato do normalize_expire() em 811_sync.py.
 * Se não conseguir parsear, retorna ''.
 */
function normalizeExpire(s){
  if(!s)return'';
  s=String(s).trim();
  if(s==='—'||s==='-'||s==='N/A'||s==='null'||s==='None')return'';
  // Mesmas limpezas do Python:
  let c=s.replace(/\s+at\s+/gi,' ');
  c=c.replace(/\s*Time\s*:\s*/gi,' ');
  c=c.replace(/\s*(ET|EST|EDT|CT|CST|CDT|PT|PST|PDT|MT|MST|MDT|UTC|GMT)\b/gi,'');
  c=c.replace(/\s+/g,' ').trim();
  // Tenta ISO primeiro (YYYY-MM-DD): "2026-05-13" → "05/13/2026"
  const iso=c.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if(iso){
    const yr=parseInt(iso[1],10),mo=parseInt(iso[2],10),day=parseInt(iso[3],10);
    if(mo>=1&&mo<=12&&day>=1&&day<=31)return String(mo).padStart(2,'0')+'/'+String(day).padStart(2,'0')+'/'+yr;
  }
  // Fix bug #24: paridade com Python — suporte a "May 13, 2026" e "Jan 5, 2026".
  // Sem isso, importações/migrações com esse formato resultavam em string vazia (data perdida).
  const MONTHS={jan:1,feb:2,mar:3,apr:4,may:5,jun:6,jul:7,aug:8,sep:9,oct:10,nov:11,dec:12,
                january:1,february:2,march:3,april:4,june:6,july:7,august:8,september:9,october:10,november:11,december:12};
  const mm=c.match(/^([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})/);
  if(mm){
    const moNum=MONTHS[mm[1].toLowerCase()];
    if(moNum){
      const day=parseInt(mm[2],10),yr=parseInt(mm[3],10);
      if(day>=1&&day<=31)return String(moNum).padStart(2,'0')+'/'+String(day).padStart(2,'0')+'/'+yr;
    }
  }
  // Pega MM/DD/YY ou MM/DD/YYYY
  const m=c.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
  if(!m)return'';
  let mo=parseInt(m[1],10),day=parseInt(m[2],10),yr=parseInt(m[3],10);
  if(yr<100)yr+=2000;
  if(mo<1||mo>12||day<1||day>31)return'';
  return String(mo).padStart(2,'0')+'/'+String(day).padStart(2,'0')+'/'+yr;
}

// Fix bug #24: testes de paridade JS ↔ Python.
// Expectativas aqui devem bater EXATAMENTE com os outputs do normalize_expire do 811_sync.py.
// Se alguém mudar uma implementação sem atualizar a outra, esses testes disparam um warning
// no console ao carregar o app. Abre ?dev=1 na URL pra ver os resultados detalhados.
(function testNormalizeExpireParity(){
  const cases=[
    ['05/13/2026','05/13/2026'], ['5/13/2026','05/13/2026'],
    ['05/13/26','05/13/2026'],   ['5/13/26','05/13/2026'],
    ['2026-05-13','05/13/2026'], ['2026-5-13','05/13/2026'],
    ['May 13, 2026','05/13/2026'], ['Jan 5, 2026','01/05/2026'],
    ['December 31, 2025','12/31/2025'], ['Feb 29, 2024','02/29/2024'],
    ['05/13/2026 11:59 PM','05/13/2026'], ['05/13/2026 23:59','05/13/2026'],
    ['05/13/26 23:59ET','05/13/2026'], ['05/13/2026 Time: 23:59','05/13/2026'],
    ['05/13/2026 at 23:59','05/13/2026'],
    ['',''], ['—',''], ['-',''], ['N/A',''], ['None',''], ['null',''],
    ['lixo que não é data',''], ['13/05/2026',''],
  ];
  const fails=[];
  for(const[inp,exp]of cases){
    const got=normalizeExpire(inp);
    if(got!==exp)fails.push({inp,exp,got});
  }
  if(fails.length){
    console.warn('[normalizeExpire parity] ⚠️ '+fails.length+' casos divergindo do Python:',fails);
  }else if(new URLSearchParams(location.search).get('dev')==='1'){
    console.log('[normalizeExpire parity] ✅ '+cases.length+' casos OK (paridade JS ↔ Python)');
  }
})();

function dbToTicket(r){
  return{
    id:r.id, ticket:r.ticket, projectId:r.project_id||'', company:r.company||'',
    state:r.state||'', location:r.location||'', status:r.status||'Open',
    expire:normalizeExpire(r.expire||''), footage:r.footage||0, client:r.client||'', prime:r.prime||'',
    job:r.job||'', tipo:r.tipo||'', address:r.address||'', pending:r.pending||'',
    oldTicket2:r.old_ticket2||'', statusOld:r.status_old||'', expireOld:normalizeExpire(r.expire_old||''),
    notes:r.notes||'', fieldPath:r.field_path||null,
    _geocoded:(r.geocoded_lat&&r.geocoded_lon)?[r.geocoded_lat,r.geocoded_lon]:null,
    history:r.history||[], attachments:r.attachments||[], status_locked:r.status_locked||false,
    project_locked:r.project_locked||false,
    damageCount:r.damage_count||0,// Fase 1 do refactor Damage: contador separado do status
    county:r.county||'',// Fase 1 filtro county: auto-derivado pelo Python via base cidade→county
    completedFeet:r.completed_feet||0,// Baixa parcial: footage concluído no campo
    created_at:r.created_at||null
  };
}
function ticketToDb(t){
  // GUARDA ÚNICA de escrita: normaliza expire/expire_old antes de gravar no banco.
  // Todos os caminhos de save passam por aqui (import Excel, edit, renew, Clear manual,
  // save de trajeto, batch upsert) — garante que o banco NUNCA recebe formato poluído.
  const o={
    ticket:t.ticket, project_id:t.projectId||null, company:t.company||'',
    state:t.state||'', location:t.location||'', status:t.status||'Open',
    expire:normalizeExpire(t.expire||''), footage:t.footage||0, client:t.client||'', prime:t.prime||'',
    job:t.job||'', tipo:t.tipo||'', address:t.address||'', pending:t.pending||'',
    old_ticket2:t.oldTicket2||'', status_old:t.statusOld||'', expire_old:normalizeExpire(t.expireOld||''),
    notes:t.notes||'', field_path:t.fieldPath&&t.fieldPath.length>=2?t.fieldPath:null,
    geocoded_lat:t._geocoded?t._geocoded[0]:null, geocoded_lon:t._geocoded?t._geocoded[1]:null,
    history:t.history||[], attachments:t.attachments||[], status_locked:t.status_locked||false,
    project_locked:t.project_locked||false,
    damage_count:Math.max(0,parseInt(t.damageCount)||0),// Fase 1 refactor Damage: força integer não-negativo
    county:t.county||''// Fase 1 filtro county
  };
  // Só inclui completed_feet se coluna existe no banco (evita erro 42703 que quebraria TODOS os saves)
  if(_hasCompletedFeetCol)o.completed_feet=Math.max(0,parseInt(t.completedFeet)||0);
  return o;
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
    // Config explicita de auth (item login persistente):
    //  - persistSession: salva token no localStorage entre sessoes
    //  - autoRefreshToken: renova automatico antes de expirar
    //  - storageKey unico: nao colide com outros apps Supabase
    sb=supabase.createClient(SUPABASE_URL,SUPABASE_KEY,{
      auth:{
        persistSession:true,
        autoRefreshToken:true,
        detectSessionInUrl:true,
        storage:window.localStorage,
        storageKey:'sb-onedrill-auth'
      }
    });
    // Listener pra detectar refresh/logout em qualquer aba
    sb.auth.onAuthStateChange((event,session)=>{
      if(event==='TOKEN_REFRESHED')console.log('[Auth] token renovado');
      if(event==='SIGNED_OUT'){isAdmin=false;role='viewer';}
    });
    // Fix bug #13: 8s é agressivo demais em 4G no campo (tablets de supervisor em obra).
    // 15s dá margem mas ainda detecta servidor down em tempo razoável.
    const timeout=new Promise((_,reject)=>setTimeout(()=>reject(new Error('timeout')),15000));
    const fetchData=async()=>{
      const{data:p,error:ep}=await sb.from('projects').select('*').order('name');
      // Supabase default limit = 1000 rows. Paginar pra carregar TODOS os tickets.
      let allTickets=[];let from=0;const PAGE=1000;
      while(true){
        const{data:page,error:et}=await sb.from('tickets').select('*').order('ticket').range(from,from+PAGE-1);
        if(et)throw new Error('Tickets: '+et.message);
        if(!page||!page.length)break;
        allTickets=allTickets.concat(page);
        if(page.length<PAGE)break;
        from+=PAGE;
      }
      if(ep)throw new Error('Projetos: '+ep.message);
      return{p,t:allTickets};
    };
    const{p,t}=await Promise.race([fetchData(),timeout]);
    projects=(p||[]).map(dbToProject);
    tickets=(t||[]).map(dbToTicket);
    // Detecta se coluna completed_feet existe (evita erro 42703 em saves se ainda não criou)
    const{error:cfErr}=await sb.from('tickets').select('completed_feet').limit(1);
    _hasCompletedFeetCol=!cfErr;
    if(!_hasCompletedFeetCol)console.warn('[Init] Coluna completed_feet não existe no banco. Baixa parcial desativada até rodar o ALTER TABLE.');
    // Detecta se coluna email existe em utility_contacts (evita erro 42703 nos saves de contato)
    const{error:emErr}=await sb.from('utility_contacts').select('email').limit(1);
    _hasContactEmailCol=!emErr;
    if(!_hasContactEmailCol)console.warn('[Init] Coluna email não existe em utility_contacts. Campo email só salva após rodar: ALTER TABLE utility_contacts ADD COLUMN email text;');
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
function getTicketPendingUtils(ticketNum, ticketObj){
  if(ticketObj){
    const merged=getMergedUtils(ticketObj);
    if(merged)return merged.filter(u=>u.status==='Pending');
  }
  const key=String(ticketNum||'').trim();
  return(utilCache[key]||[]).filter(u=>u.status==='Pending');
}
function getTicketUtils(ticketNum){
  const key=String(ticketNum||'').trim();
  return utilCache[key]||[];
}

/* ═══════════ 8b. FIBRA + 2ND NOTICE (No-Show) ═══════════ */
// Utilities de FIBRA (telecom/cabo). A liberação automática por no-show SÓ vale
// pra estas — nunca água, gás, elétrico ou sewer. Lista mantida aqui + override
// pelo campo utility_type quando preenchido no banco.
const FIBER_NAMES=['COMCAST','XFINITY','AT&T','ATT','FRONTIER','UNITI','TIME WARNER','SPECTRUM','CHARTER','METRO FIBERNET','METRONET','MIDWEST FIBER','LUMEN','CENTURYLINK','ZAYO','WINDSTREAM','CROWN CASTLE','LEVEL 3','VERIZON','SEGRA','EVERSTREAM','WIDEOPENWEST','WOW','SURF'];
const FIBER_KEYWORDS=['FIBER','FIBRE','FIBERNET','TELECOM','CABLE','BROADBAND','COMMUNICATION'];
const NONFIBER_TYPES=['water','gas','electric','sewer','agua','água','eletrico','elétrico','esgoto'];
function isFiberUtility(name,utilityType){
  const ut=(utilityType||'').toLowerCase().trim();
  if(['fiber','fibra','telecom','comm','cable'].includes(ut))return true;
  if(NONFIBER_TYPES.includes(ut))return false;
  const n=(name||'').toUpperCase();
  if(FIBER_NAMES.some(f=>n.includes(f)))return true;
  if(FIBER_KEYWORDS.some(k=>n.includes(k)))return true;
  return false;
}

// No-shows (2nd/3rd/4th notice). Guardados no history do ticket (entrada `sn`),
// SEM tabela nova. sn={u:utility, d:dataISO, n:ordinal(2,3,4...), src, tk:numero_do_ticket}.
// `tk` = número do ticket em que o no-show foi feito. getSecondNotices só conta os do
// número ATUAL — então renovação NÃO herda no-show do antigo (e a regra laranja recalcula
// junto quando o antigo expira/renova). Entrada legada sem `tk` = tratada como do atual.
function _ordEn(n){n=parseInt(n,10)||2;if(n===1)return'1st';if(n===2)return'2nd';if(n===3)return'3rd';return n+'th';}
function getSecondNotices(t, onlyTk){
  const cur=String((t&&t.ticket)||'').trim();
  // Em CARÊNCIA (renovação), o ticket ainda opera sob o número ANTIGO — então mostra
  // e conta os no-shows do antigo até a carência expirar; depois, só os do número atual.
  // onlyTk: se passado, devolve SÓ os no-shows daquele número (usado pra exibir o grupo
  // "ticket antigo" separado do "ticket atual" no painel e pra inserir em cada um).
  const inGrace=!!(t&&isRenewed(t)&&isInRenewalGrace(t));
  const oldNum=inGrace?((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim():'';
  const want=(onlyTk!==undefined&&onlyTk!==null)?String(onlyTk).trim():null;
  const raw=((t&&t.history)||[]).filter(h=>{
    if(!h||!h.sn)return false;
    const tk=String(h.sn.tk||'').trim();
    if(want!==null)return tk===want||(!tk&&want===cur);   // grupo de UM número (legado sem tk = atual)
    return !tk||tk===cur||(oldNum&&tk===oldNum);          // visão padrão: atual + (carência) antigo
  }).map(h=>({ts:h.ts,utility:h.sn.u,date:h.sn.d,source:h.sn.src||'manual',n:parseInt(h.sn.n,10)||0}));
  // Recalcula o ordinal por utility pela ordem de data (1º registro = "2nd notice" = n2).
  // Corrige rótulos herdados (o n salvo podia ter contado no-shows de outro número de ticket).
  const byU={};
  raw.forEach(x=>{const k=(x.utility||'').toUpperCase();(byU[k]=byU[k]||[]).push(x);});
  Object.keys(byU).forEach(k=>{
    byU[k].sort((a,b)=>String(a.date||'').localeCompare(String(b.date||''))||((a.ts||0)-(b.ts||0)));
    byU[k].forEach((x,i)=>{x.n=i+2;});
  });
  return raw;
}
function countSecondNotices(t,utilityName){
  // Retorna o MAIOR ordinal (2-based) dos no-shows DESTE ticket pra essa utility:
  // 1 no-show→2, 2→3, 3→4. NÃO é a contagem (é qtd+1). A regra laranja usa >=4,
  // ou seja exige 3 no-shows reais. Se mudar o recompute pra 1-based, ajustar o limiar.
  const u=(utilityName||'').toUpperCase();
  const list=getSecondNotices(t).filter(n=>(n.utility||'').toUpperCase()===u);
  return list.length?Math.max(...list.map(x=>x.n)):0;
}
function _snDateToIso(s){
  s=(s||'').trim();
  const m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if(m)return m[3]+'-'+m[1].padStart(2,'0')+'-'+m[2].padStart(2,'0');
  if(/^\d{4}-\d{2}-\d{2}/.test(s))return s.slice(0,10);
  return null;
}
async function addSecondNotice(ticketNum, targetTk){
  const t=tickets.find(x=>String(x.ticket).trim()===String(ticketNum).trim());
  if(!t){toast('Ticket não encontrado.','danger');return;}
  // target = número AO QUAL o no-show será atribuído (sn.tk). Pode ser o ATUAL ou,
  // num ticket renovado, o número ANTIGO — assim dá pra registrar no-show em ambos.
  const target=String(targetTk||t.ticket).trim();
  const isOld=target!==String(t.ticket).trim();
  const uniq=[...new Set(getTicketUtils(target).map(u=>u.utility_name).filter(Boolean))];
  const listaTxt=uniq.length?uniq.map((u,i)=>(i+1)+') '+u+(isFiberUtility(u)?' [fibra]':'')).join('\n'):'(sem utilities no cache — digite o nome)';
  const pick=prompt('Registrar NO-SHOW pra qual utility?\n(ticket '+target+(isOld?' — ANTIGO':' — atual')+')\n\n'+listaTxt+'\n\nDigite o NÚMERO da lista ou o nome:','');
  if(pick===null)return;
  let util=pick.trim();
  const asNum=parseInt(util,10);
  if(!isNaN(asNum)&&uniq[asNum-1])util=uniq[asNum-1];
  if(!util){toast('Utility vazia.','warn');return;}
  const existing=getSecondNotices(t,target).filter(x=>(x.utility||'').toUpperCase()===util.toUpperCase());
  const n=existing.length?Math.max(...existing.map(x=>x.n))+1:2;
  const dateRaw=prompt('Data do '+_ordEn(n)+' no-show (MM/DD/YYYY):',new Date().toLocaleDateString('en-US'));
  if(dateRaw===null)return;
  const iso=_snDateToIso(dateRaw)||new Date().toISOString().slice(0,10);
  const label=iso.split('-').reverse().join('/');
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'['+_ordEn(n).toUpperCase()+' NO-SHOW] '+util+' ('+label+')'+(isOld?' [ticket '+target+']':''),color:'#db2777',sn:{u:util,d:iso,n:n,src:'manual',tk:target}});
  const ok=await saveTicketToDb(t);
  if(ok){toast(_ordEn(n)+' no-show registrado: '+util+(isOld?' (ticket antigo '+target+')':''),'success');renderSecondNotices(t);}
  else{t.history.pop();toast('Erro ao salvar. Tente Refresh (⟳).','danger');}
}
async function deleteSecondNotice(ticketNum,ts){
  const t=tickets.find(x=>String(x.ticket).trim()===String(ticketNum).trim());
  if(!t)return;
  if(!confirm('Remover este 2nd notice?'))return;
  const bk=t.history||[];
  const filtered=bk.filter(h=>!(h&&h.sn&&h.ts===ts));
  if(filtered.length===bk.length)return;
  t.history=filtered;
  const ok=await saveTicketToDb(t);
  if(ok){toast('2nd notice removido','success');renderSecondNotices(t);}
  else{t.history=bk;toast('Erro ao remover.','danger');}
}
async function renderSecondNotices(t){
  const el=document.getElementById('det-2nd-notices');if(!el)return;
  // Busca o history fresco do banco — 2nd notices podem ter sido inseridos em massa
  // ou pelo sync, sem estar no array `tickets` em memória (igual renderUtils faz).
  try{
    if(typeof t.id==='number'&&t.id>0){
      const{data}=await sb.from('tickets').select('history').eq('id',t.id).maybeSingle();
      if(data&&Array.isArray(data.history)){t.history=data.history;if(typeof renderHistory==='function')renderHistory(t);}
    }
  }catch(e){}
  const cur=String(t.ticket||'').trim();
  const inGrace=isRenewed(t)&&isInRenewalGrace(t);
  const graceOld=inGrace?((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim():'';
  // Grupos: ticket ATUAL + (se renovado) cada número ANTIGO da chain — cada um com sua
  // lista e seu botão. Assim dá pra ver/registrar no-show no antigo E no novo (o antigo
  // some da regra laranja, mas continua gerenciável aqui).
  const chain=isRenewed(t)?((t.oldTicket2||t.old_ticket2)||'').split(' → ').map(s=>s.trim()).filter(Boolean):[];
  const groups=[{tk:cur,old:false}];
  chain.forEach(o=>{if(o&&o!==cur&&!groups.some(g=>g.tk===o))groups.push({tk:o,old:true});});
  const renderRow=(x)=>{
    const d=x.date?x.date.split('-').reverse().join('/'):(x.ts?new Date(x.ts).toLocaleDateString('pt-BR'):'—');
    const fiber=isFiberUtility(x.utility)?' <span style="font-size:9px;color:#db2777;font-weight:700">FIBRA</span>':'';
    const src=x.source==='auto'?' <span style="font-size:9px;color:var(--muted)">(auto)</span>':'';
    const del=isAdmin?' <button class="btn btn-sm btn-danger" style="font-size:9px;padding:1px 5px" onclick="deleteSecondNotice(\''+esc(cur)+'\','+x.ts+')">×</button>':'';
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:3px 0;border-top:1px solid var(--border);font-size:12px"><span><b style="color:#db2777">'+_ordEn(x.n)+'</b> '+esc(x.utility)+fiber+src+'</span><span style="color:var(--text2)">'+d+del+'</span></div>';
  };
  let h='';
  groups.forEach(g=>{
    const list=getSecondNotices(t,g.tk).sort((a,b)=>(a.utility||'').localeCompare(b.utility||'')||(a.n-b.n));
    if(groups.length>1){
      const tag=g.old?(g.tk===graceOld?' · conta (carência)':' · não conta hoje'):'';
      h+='<div style="margin-top:'+(h?'10':'0')+'px;font-size:10px;font-weight:700;letter-spacing:.4px;text-transform:uppercase;color:'+(g.old?'#7c3aed':'var(--text2)')+'">'+(g.old?'🔄 Ticket antigo '+esc(g.tk):'Ticket atual '+esc(g.tk))+tag+'</div>';
    }
    h+=list.length?list.map(renderRow).join(''):'<div style="font-size:11px;color:var(--muted);padding:2px 0">Nenhum no-show registrado.</div>';
    if(isAdmin)h+='<button class="btn btn-sm" style="margin-top:5px;font-size:11px" onclick="addSecondNotice(\''+esc(cur)+'\',\''+esc(g.tk)+'\')">+ Registrar no-show'+(groups.length>1?(g.old?' (antigo)':' (atual)'):'')+'</button>';
  });
  el.innerHTML=h;
}
/**
 * WI Relo-No-Show: merge utilities do Relo (novo) com herdadas do Standard (antigo).
 * Retorna array combinada ou null se não aplicável.
 * Ativa SÓ quando: WI + renovado + novo tem MENOS utils que antigo.
 */
function getMergedUtils(t){
  if(!utilCacheLoaded)return null;
  if((t.state||'').toUpperCase()!=='WI')return null;
  if(!isRenewed(t))return null;
  const newKey=String(t.ticket||'').trim();
  const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
  if(!oldNum)return null;
  const newUtils=utilCache[newKey]||[];
  const oldUtils=utilCache[oldNum]||[];
  if(!oldUtils.length||newUtils.length>=oldUtils.length)return null;
  // Merge: herda do antigo, sobrescreve com o novo
  const merged={};
  for(const u of oldUtils)merged[u.utility_name]={...u,_inherited:true};
  for(const u of newUtils)merged[u.utility_name]=u;
  return Object.values(merged);
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

// Carrega tabela utility_county_coverage (auto-derivada pela Fase 2 do Python).
// Usada pra saber quais utilities atendem cada county.
async function loadUtilCoverage(){
  try{
    let allData=[];
    let offset=0;
    const pageSize=1000;
    while(true){
      const{data,error}=await sb
        .from('utility_county_coverage')
        .select('utility_name,county,state,response_count')
        .order('response_count',{ascending:false})
        .range(offset,offset+pageSize-1);
      if(error){
        // Tabela pode não existir ainda (Fase 2 SQL não rodou). Falha silenciosa — UI lida.
        console.warn('[Coverage] load error:',error.message);
        break;
      }
      if(!data||!data.length)break;
      allData=allData.concat(data);
      if(data.length<pageSize)break;
      offset+=pageSize;
    }
    utilCoverage=allData;
    console.log('[Coverage]',utilCoverage.length,'cobertas utility×county carregadas');
  }catch(e){console.error('Coverage load error:',e);utilCoverage=[];}
}

// Preenche dropdown de county baseado no state selecionado.
// Lê counties únicos da tabela utility_county_coverage filtrados pelo state.
// Se _contactsPreselectCounty tá setado (usuário veio de um ticket), pré-seleciona.
function repopulateCountyDropdown(){
  const stateSel=document.getElementById('contacts-state-filter');
  const countySel=document.getElementById('contacts-county-filter');
  if(!stateSel||!countySel)return;
  const st=stateSel.value||'';
  if(!st){
    // Sem state → county desabilitado
    countySel.innerHTML='<option value="">Todos counties</option>';
    countySel.disabled=true;
    countySel.title='Selecione um estado primeiro';
    return;
  }
  // Counties únicos daquele state, ordenados alfabeticamente
  const counties=[...new Set(utilCoverage.filter(c=>c.state===st).map(c=>c.county).filter(Boolean))].sort();
  countySel.disabled=false;
  countySel.title='';
  let options='<option value="">Todos counties</option>';
  for(const c of counties){
    options+='<option value="'+esc(c)+'">'+esc(c)+'</option>';
  }
  countySel.innerHTML=options;
  // Pré-seleção (só uma vez)
  if(_contactsPreselectCounty&&counties.indexOf(_contactsPreselectCounty)>=0){
    countySel.value=_contactsPreselectCounty;
    _contactsPreselectCounty='';// consume
  }
}

function renderContacts(){
  const grid=document.getElementById('contacts-grid');if(!grid)return;
  const sr=(document.getElementById('contacts-search')?.value||'').toLowerCase();
  const sf=document.getElementById('contacts-state-filter')?.value||'';
  const cf=document.getElementById('contacts-county-filter')?.value||'';
  // Se county foi selecionado, calcula set de utility_names que atendem esse county (daquele state)
  let allowedUtilities=null;
  if(cf&&sf){
    allowedUtilities=new Set(
      utilCoverage
        .filter(c=>c.state===sf&&c.county===cf)
        .map(c=>(c.utility_name||'').toUpperCase())
    );
  }
  let f=utilContacts.filter(c=>{
    if(sf&&(c.state||'')!==sf)return false;
    if(sr&&!(c.utility_name||'').toLowerCase().includes(sr)&&!(c.phone_main||'').includes(sr))return false;
    if(allowedUtilities&&!allowedUtilities.has((c.utility_name||'').toUpperCase()))return false;
    return true;
  });
  if(!f.length){
    const hint=cf?'<br><span style="font-size:11px">Nenhum contato de utility que atenda '+esc(cf)+' County, '+esc(sf)+'.</span>':
      (utilContacts.length===0?' Execute <code>python 811_sync.py --contacts --state FL</code> para importar.':'');
    grid.innerHTML='<div style="color:var(--muted);font-size:13px;padding:20px;text-align:center">Nenhum contato encontrado.'+hint+'</div>';
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
        const _em=_contactEmail(c);
        if(_em)phones.push('<span class="cc-tag">📧</span> <a href="mailto:'+esc(_em)+'">'+esc(_em)+'</a>');
        return'<div style="display:flex;justify-content:space-between;align-items:center;padding:5px 0;border-top:1px solid var(--border)">'
          +(name?'<span style="font-size:12px;font-weight:600;color:var(--text2);min-width:120px">'+name+'</span>':'')
          +'<div class="cc-phones" style="margin:0;gap:10px">'+phones.join(' ')+'</div>'
          +(isAdmin?'<div style="display:flex;gap:4px;margin-left:8px"><button class="btn btn-sm" onclick="openContactModal('+c.id+')" style="font-size:10px;padding:2px 6px">✏</button><button class="btn btn-sm btn-danger" onclick="deleteContact('+c.id+')" style="font-size:10px;padding:2px 6px">×</button></div>':'')
          +'</div>';
      }).join('')
      +(_notesClean(contacts[0].notes)?'<div class="cc-meta" style="margin-top:6px">'+esc(_notesClean(contacts[0].notes))+'</div>':'')
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
      document.getElementById('ct-notes').value=_notesClean(c.notes);
      document.getElementById('ct-email').value=_contactEmail(c);
    }
  }else{
    ['ct-utility','ct-name','ct-ticket','ct-phone1','ct-phone2','ct-phone3','ct-notes','ct-email'].forEach(id=>document.getElementById(id).value='');
    document.getElementById('ct-state').value='FL';
  }
  openModal('ov-contact');
}
// Fix bug #31: função wrapper editContact removida — era só um alias de openContactModal.
// Callers já foram atualizados para chamar openContactModal direto.

async function saveContact(){
  const name=document.getElementById('ct-utility').value.trim();
  if(!name){toast('Preencha o nome da utility.','danger');return;}
  const _email=document.getElementById('ct-email').value.trim();
  const _notes=_notesClean(document.getElementById('ct-notes').value.trim());
  const data={
    utility_name:name,
    contact_name:document.getElementById('ct-name').value.trim(),
    state:document.getElementById('ct-state').value,
    ticket_ref:document.getElementById('ct-ticket').value.trim(),
    phone_main:document.getElementById('ct-phone1').value.trim(),
    phone_alt:document.getElementById('ct-phone2').value.trim(),
    phone_emergency:document.getElementById('ct-phone3').value.trim(),
    notes:_notes
  };
  if(_hasContactEmailCol){
    data.email=_email;
  }else if(_email){
    // Sem coluna email no banco: embute no notes ("✉ email") pra não perder o dado nem precisar de DDL.
    data.notes=_notes?_notes+' · ✉ '+_email:'✉ '+_email;
  }
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
    ['Utility','Nome Contato','Estado','Tel. Principal','Tel. Alternativo','Tel. Emergência','Email','Ticket Ref','Notas'],
    ...utilContacts.map(c=>[c.utility_name,c.contact_name||'',c.state,c.phone_main||'',c.phone_alt||'',c.phone_emergency||'',_contactEmail(c),c.ticket_ref||'',_notesClean(c.notes)])
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
  currentUserEmail=(user.email||'').toLowerCase();
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

// Senha pra acessar como Manager / Visualizador.
// Hardcoded por simplicidade — pra trocar, edita aqui e dá push no GitHub.
// Não protege contra alguém com F12 (a senha é visível em texto puro no JS),
// mas barra acesso casual de quem só descobriu a URL do app.
const VIEWER_PASSWORD = '#Onedrill@2020';

function enterViewer(){
  // UX (2026-05-13): se ja autorizou nesse dispositivo, pula a senha.
  // Salva flag em localStorage apos primeiro acerto. doLogout limpa.
  try {
    if (localStorage.getItem('viewer-auth') === 'true') {
      isAdmin=false;role='viewer';
      document.getElementById('login-screen').style.display='none';
      enterApp();
      return;
    }
  } catch(e) {}
  const pw = prompt('Digite a senha de acesso (so na primeira vez):');
  if(pw === null) return;
  if(pw !== VIEWER_PASSWORD){
    alert('Senha incorreta.');
    return;
  }
  try { localStorage.setItem('viewer-auth', 'true'); } catch(e) {}
  isAdmin=false;role='viewer';
  document.getElementById('login-screen').style.display='none';
  enterApp();
}

async function doLogout(){
  await sb.auth.signOut();
  try { localStorage.removeItem('viewer-auth'); } catch(e) {}
  // Fix bug #2: limpar auto-refresh e sync timer ao fazer logout.
  // Evita que o interval continue rodando (e tentando fetchar dados) após logout.
  if(_autoRefreshId){clearInterval(_autoRefreshId);_autoRefreshId=null;}
  if(_syncTimerId){clearTimeout(_syncTimerId);_syncTimerId=null;}
  isAdmin=false;role='viewer';currentUserEmail='';
  document.getElementById('app-shell').style.display='none';
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById('pg-dash').classList.add('active');
  document.getElementById('login-screen').classList.remove('hidden');
  document.getElementById('login-screen').style.display='flex';
}

/* ═══════════ 11. APP SHELL ═══════════ */
function enterApp(){
  const shell=document.getElementById('app-shell');
  shell.classList.remove('hidden');
  shell.style.display='grid';
  document.getElementById('role-badge').textContent=isAdmin?'ADMIN':'VIEWER';
  document.getElementById('role-badge').style.background=isAdmin?'var(--green-bg)':'var(--accent-bg)';
  document.getElementById('role-badge').style.color=isAdmin?'var(--green)':'var(--accent)';
  const logoutBtn=document.getElementById('btn-logout');
  if(logoutBtn)logoutBtn.style.display='';
  const adminEls=['btn-import','btn-new-ticket','btn-new-proj','det-edit-btn','det-draw-btn','btn-add-contact'];
  adminEls.forEach(id=>{const e=document.getElementById(id);if(e){if(isAdmin)e.classList.remove('hidden');else e.classList.add('hidden');}});
  if(!isAdmin){const fss=document.getElementById('field-status-section');if(fss)fss.style.display='none';}
  syncAll();renderDash();
  loadLastSync();
  loadUtilCache().then(()=>{syncAll();buildNotifications();});
  loadContacts().then(()=>renderContacts());
  loadUtilCoverage();// Fase 3 filtro county — independente, não bloqueia

  // Auto-refresh
  // Fix bug #2: limpa interval anterior antes de criar novo.
  // enterApp() pode ser chamado várias vezes (login, viewer, share fallback, sessão restaurada).
  // Sem este clearInterval, cada chamada empilha um novo setInterval → race conditions e
  // múltiplos fetches paralelos do banco sobrescrevendo tickets/projects.
  if(_autoRefreshId){clearInterval(_autoRefreshId);_autoRefreshId=null;}
  _autoRefreshId=setInterval(async()=>{
    if(fieldDrawing){console.log('[AutoRefresh] Pulado — desenho em andamento');return;}
    if(document.querySelector('.overlay.open')){console.log('[AutoRefresh] Pulado — modal aberto');return;}
    try{
      const{data:p}=await sb.from('projects').select('*').order('name');
      let _art=[];let _af=0;
      while(true){const{data:pg}=await sb.from('tickets').select('*').order('ticket').range(_af,_af+999);if(!pg||!pg.length)break;_art=_art.concat(pg);if(pg.length<1000)break;_af+=1000;}
      if(p)projects=p.map(dbToProject);
      if(_art.length)tickets=_art.map(dbToTicket);
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
  if(page==='completed')renderCompletedPage();
  // Timeline removido
}

// Navega pra aba Contatos já pré-filtrando state+county do ticket atual.
// Usada pelo link clicável do campo "County" no modal de detalhes.
function gotoContactsForCounty(county,state){
  if(!county||!state)return;
  closeModal('ov-detail');
  // Seta state primeiro, daí county é pré-selecionado automaticamente pelo repopulate
  const stateSel=document.getElementById('contacts-state-filter');
  if(stateSel)stateSel.value=state;
  _contactsPreselectCounty=county;
  nav('contacts');
  // repopulate é chamado pelo listener de change do state, mas como setamos programaticamente,
  // precisamos chamar manualmente
  repopulateCountyDropdown();
  renderContacts();
  toast('📞 Filtro: '+county+' County, '+state,'info');
}
function openModal(id){document.getElementById(id).classList.add('open');}
function closeModal(id){document.getElementById(id).classList.remove('open');if(id==='ov-detail')_detailNavList=[];}
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
  const isCompletedProj=pf&&projects.find(p=>p.id===pf&&p.status==='Completed');
  return filterTickets({
    projectId:pf,
    search:sr,
    client:cl,
    excludeSuperseded:true,
    excludeCompleted:!isCompletedProj,
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
  const es=effectiveStatus(t);
  const inGrace=isRenewed(t)&&isInRenewalGrace(t);
  const isStale=expireIsStale(t);
  // Bloqueio "não cavar" — baseado em datas, NUNCA suprimido pela trava. '' / 'expired' / 'grace'.
  const isExp=ticketExpiredEffective(t);
  return`<div style="font-family:'DM Sans',sans-serif;font-size:13px;line-height:1.6;min-width:180px;padding:2px">`
    +(isExp?'<div style="background:#dc2626;color:white;padding:6px 10px;border-radius:6px;margin-bottom:8px;text-align:center;font-weight:700;font-size:12px">⛔ NÃO TRABALHAR — '+(isExp==='grace'?'CARÊNCIA VENCIDA':'VENCIDO')+'</div>':'')
    +(isStale&&!inGrace?'<div style="background:#fffbeb;border:1px solid #fde68a;padding:5px 8px;border-radius:6px;margin-bottom:6px;text-align:center;font-size:11px;font-weight:600;color:#b45309">⏳ Aguardando sync 811 — data não confirmada</div>':'')
    +(inGrace?(()=>{if(newTicketFullyCleared(t))return'<div style="background:#f0fdf4;border:1px solid #86efac;padding:5px 8px;border-radius:6px;margin-bottom:6px;text-align:center;font-size:11px;font-weight:600;color:#16a34a">✅ Liberado — novo clareou</div>';const os=t.statusOld||t.status_old||'Open';return os==='Clear'?'<div style="background:#f0fdf4;border:1px solid #86efac;padding:5px 8px;border-radius:6px;margin-bottom:6px;text-align:center;font-size:11px;font-weight:600;color:#16a34a">✅ Carência até '+graceCutoverDate(t)+'</div>':'<div style="background:#fffbeb;border:1px solid #fde68a;padding:5px 8px;border-radius:6px;margin-bottom:6px;text-align:center;font-size:11px;font-weight:600;color:#b45309">⚠ Carência ('+esc(os)+') até '+graceCutoverDate(t)+'</div>';})():'')
    +`<div style="font-weight:700;color:#18180f;margin-bottom:6px;font-size:14px;font-family:'DM Mono',monospace">${esc(t.ticket)}</div>`
    +(proj?`<div><span style="color:#9a9888">Projeto: </span>${esc(proj.name)}</div>`:'')
    +`<div><span style="color:#9a9888">Cliente: </span>${esc(t.client)}</div>`
    +(t.prime?`<div><span style="color:#9a9888">Prime: </span>${esc(t.prime)}</div>`:'')
    +`<div><span style="color:#9a9888">Footage: </span>${fmtProgress(t)}</div>`+miniProgressBar(t)
    +(t.tipo?`<div><span style="color:#9a9888">Tipo: </span>${esc(t.tipo)}</div>`:'')
    +`<div><span style="color:#9a9888">Status: </span><span style="color:${scol(es)};font-weight:700">${esc(es)}${inGrace?' 🔄':''}</span></div>`
    +(()=>{const pu=getTicketPendingUtils(String(t.ticket).trim());if(!pu.length)return'';return'<div style="display:flex;flex-wrap:wrap;gap:2px;margin:4px 0">'+pu.slice(0,4).map(p=>'<span style="font-size:9px;padding:1px 5px;border-radius:10px;background:#fef2f2;color:#dc2626;font-family:monospace;white-space:nowrap">'+esc(p.utility_name.length>18?p.utility_name.substring(0,18)+'…':p.utility_name)+'</span>').join('')+(pu.length>4?'<span style="font-size:9px;color:#9a9888">+'+( pu.length-4)+'</span>':'')+'</div>';})()
    +`<div><span style="color:#9a9888">Expira: </span><span${isExp?' style="color:#dc2626;font-weight:700"':''}>${esc(t.expire||'—')}${isExp?(isExp==='grace'?' ⚠ CARÊNCIA VENCIDA':' ⚠ VENCIDO'):''}</span></div>`
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
    const c=effColor(t),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
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
  const es=effectiveStatus(t);
  const c=effColor(t);
  const inGrace=isRenewed(t)&&isInRenewalGrace(t);
  const isStale=expireIsStale(t);
  const proj=projects.find(p=>p.id===t.projectId);
  currentPanelId=t.id;
  // Bloqueio "não cavar" — baseado em datas, NUNCA suprimido pela trava. '' / 'expired' / 'grace'.
  const isExp=ticketExpiredEffective(t);
  document.getElementById('ptitle-txt').textContent=t.ticket+(isRenewed(t)?' (🔄 '+( t.oldTicket2||t.old_ticket2)+')':'');
  document.getElementById('pbody').innerHTML=
    (isExp?'<div style="background:#dc2626;color:white;padding:8px 10px;border-radius:var(--r);margin-bottom:8px;text-align:center;font-weight:700;font-size:12px;animation:expPulse 1.5s infinite">⛔ NÃO TRABALHAR — '+(isExp==='grace'?'CARÊNCIA VENCIDA':'VENCIDO')+'</div>':'')
    +(isStale&&!inGrace?'<div style="background:#fffbeb;border:1px solid #fde68a;padding:6px 10px;border-radius:var(--r);margin-bottom:6px;text-align:center;font-size:11px;font-weight:600;color:#b45309">⏳ Aguardando sync 811 — data de vencimento ainda não confirmada</div>':'')
    +(inGrace?(()=>{if(newTicketFullyCleared(t))return'<div style="background:#f0fdf4;border:1px solid #86efac;padding:5px 8px;border-radius:var(--r);margin-bottom:6px;text-align:center;font-size:10px;font-weight:600;color:#16a34a">✅ Liberado — novo clareou</div>';const os=t.statusOld||t.status_old||'Open';return os==='Clear'?'<div style="background:#f0fdf4;border:1px solid #86efac;padding:5px 8px;border-radius:var(--r);margin-bottom:6px;text-align:center;font-size:10px;font-weight:600;color:#16a34a">✅ Carência até '+graceCutoverDate(t)+'</div>':'<div style="background:#fffbeb;border:1px solid #fde68a;padding:5px 8px;border-radius:var(--r);margin-bottom:6px;text-align:center;font-size:10px;font-weight:600;color:#b45309">⚠ Carência ('+esc(os)+') até '+graceCutoverDate(t)+'</div>';})():'')
    +(proj?`<div class="mp-row"><span class="mp-key">Projeto</span><span class="mp-val">${esc(proj.name)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Cliente</span><span class="mp-val">${esc(t.client)}</span></div>`
    +(t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${esc(t.prime)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val" style="cursor:pointer;color:var(--accent)" onclick="quickEditFootage(currentPanelId);return false;" title="Clique para editar">${fmtProgress(t)} ✏</span></div>`+miniProgressBar(t)
    +(t.tipo?`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${esc(t.tipo)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${esc(effStatusLabel(t))}${inGrace?' 🔄':''}</span></div>`
    +(_isNoShowReleased(t)?`<div style="background:#fff3e0;border:1px solid #fed7aa;border-radius:var(--r);padding:6px 9px;margin:4px 0;font-size:10px;color:#b45309;font-weight:600">🟠 Liberado por no-show — necessário localizar a fibra em campo</div>`:'')
    +(()=>{const pu=getTicketPendingUtils(String(t.ticket).trim());if(!pu.length)return'';return'<div class="mp-row"><span class="mp-key">Pendentes</span><span class="mp-val"><div style="display:flex;flex-wrap:wrap;gap:3px">'+pu.map(p=>'<span style="font-size:9px;padding:1px 6px;border-radius:10px;background:var(--red-bg);color:var(--red);font-family:var(--mono);white-space:nowrap">'+esc(p.utility_name.length>22?p.utility_name.substring(0,22)+'…':p.utility_name)+'</span>').join('')+'</div></span></div>';})()
    +`<div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val"${isExp?' style="color:#dc2626;font-weight:700"':''}>${isStale?'⏳ aguardando sync':esc(t.expire||'—')}${isExp?(isExp==='grace'?' ⚠ CARÊNCIA VENCIDA':' ⚠ VENCIDO'):''}</span></div>`;
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

/** Toggle do botão "🔄 Em carência" na aba Tickets.
 *  Mostra apenas tickets renovados (oldTicket2 preenchido) ainda dentro do
 *  período de graça do antigo. Útil pra revisar status efetivo antes da cutover.
 *  Guarda contra clique prematuro: se utilCache não carregou, o filtro funcionaria
 *  com dados parciais (newTicketFullyCleared retorna false enquanto carrega).
 */
function toggleGraceFilter(){
  if(!utilCacheLoaded){
    toast('Aguarde — dados 811 ainda carregando','warn');
    return;
  }
  _graceFilterActive=!_graceFilterActive;
  const btn=document.getElementById('tbl-grace-toggle');
  if(btn){
    if(_graceFilterActive){
      btn.style.background='#7c3aed';
      btn.style.color='#fff';
      btn.style.borderColor='#7c3aed';
    }else{
      btn.style.background='';
      btn.style.color='';
      btn.style.borderColor='';
    }
  }
  renderTable();
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
  document.getElementById('tlist').innerHTML=f.length?f.map(t=>{
    const es=effectiveStatus(t);const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    return`<div class="tcard s-${effStatusCls(t)}" data-id="${t.id}" onclick="focusT(${t.id})">`
    +`<div class="tcard-top"><span class="tcard-num">${esc(t.ticket)}${isRenewed(t)?' <span style="font-size:9px;color:#7c3aed">🔄</span>':''}</span><span class="sbadge b-${effStatusCls(t)}">${esc(effStatusLabel(t))}${inGrace?' 🔄':''}</span></div>`
    +`<div class="tcard-client">${esc(t.client)}${t.prime?' · '+esc(t.prime):''}</div>`
    +`<div class="tcard-meta"><span>${esc(t.location)}, ${esc(t.state)}</span><span>${fmtProgress(t)}</span>${t.tipo?`<span>${esc(t.tipo)}</span>`:''}</div>`
    +miniProgressBar(t)
    +(inGrace?(()=>{if(newTicketFullyCleared(t))return`<div style="font-size:10px;color:#16a34a;font-weight:600;margin-top:2px">✅ Liberado — novo clareou</div>`;const os=t.statusOld||t.status_old||'Open';return os==='Clear'?`<div style="font-size:10px;color:#16a34a;font-weight:600;margin-top:2px">✅ Carência até ${graceCutoverDate(t)}</div>`:`<div style="font-size:10px;color:#b45309;font-weight:600;margin-top:2px">⚠ Carência (${esc(os)}) até ${graceCutoverDate(t)}</div>`;})():'')
    +(t.pending&&!inGrace?`<div style="font-size:10px;color:var(--amber);font-weight:600;margin-top:2px">⏳ ${esc(t.pending)}</div>`:'')
    +`</div>`;
  }).join(''):'<div style="text-align:center;padding:28px 16px;color:var(--muted);font-size:13px">Nenhum ticket</div>';
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
  t.history=t.history||[];// Fix bug #20: garante array antes de push
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
let _detailNavList=[];  // lista filtrada atual pra navegação ◀ ▶

function _getFilteredListForNav(){
  const ap=document.querySelector('.page.active')?.id;
  if(ap==='pg-map')return mapFiltered();
  // Aba Tickets: usa a mesma lista renderizada na tabela
  if(ap==='pg-tickets'){
    const sf=document.getElementById('tbl-status')?.value||'';
    const pf=document.getElementById('tbl-proj')?.value||'';
    const sr=(document.getElementById('tbl-search')?.value||'').toLowerCase();
    const stf=document.getElementById('tbl-state')?.value||'';
    const prf=document.getElementById('tbl-prime')?.value||'';
    const utf=document.getElementById('tbl-util')?.value||'';
    return filterTickets({status:sf,projectId:pf,search:sr,state:stf,prime:prf,utility:utf});
  }
  return tickets.filter(t=>!['Closed','Cancel'].includes(t.status));
}

function navTicketDetail(dir){
  if(!_detailNavList.length)_detailNavList=_getFilteredListForNav();
  const idx=_detailNavList.findIndex(t=>t.id===currentDetailId);
  if(idx<0)return;
  const next=idx+dir;
  if(next<0||next>=_detailNavList.length)return;
  openTicketDetail(_detailNavList[next].id);
}
// Teclas ← → pra navegar entre tickets quando modal aberto
document.addEventListener('keydown',e=>{
  if(!document.getElementById('ov-detail')?.classList.contains('open'))return;
  if(document.activeElement?.tagName==='INPUT'||document.activeElement?.tagName==='TEXTAREA'||document.activeElement?.tagName==='SELECT')return;
  if(e.key==='ArrowLeft'){e.preventDefault();navTicketDetail(-1);}
  if(e.key==='ArrowRight'){e.preventDefault();navTicketDetail(1);}
});

function openTicketDetail(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  currentDetailId=id;
  // Atualiza lista de navegação se vazia
  if(!_detailNavList.length)_detailNavList=_getFilteredListForNav();
  // Atualiza estado dos botões ◀ ▶
  const idx=_detailNavList.findIndex(x=>x.id===id);
  const prevBtn=document.getElementById('det-prev');
  const nextBtn=document.getElementById('det-next');
  if(prevBtn)prevBtn.disabled=(idx<=0);
  if(nextBtn)nextBtn.disabled=(idx<0||idx>=_detailNavList.length-1);

  const isStale=expireIsStale(t);
  const inGrace=isRenewed(t)&&isInRenewalGrace(t);
  const es=effectiveStatus(t);
  const c=effColor(t);
  // FIX 2026-05-13: isExpired dispara banner + popup fullscreen
  // pra QUALQUER ticket vencido (exceto Closed/Cancel).
  // Antes: so Open/Damage. Eric reportou que ticket Clear vencido
  // tambem precisa avisar pra renovar antes de trabalhar.
  const isExpired=ticketExpiredEffective(t);
  if(isExpired)showExpiredAlert(t,isExpired);

  const proj=projects.find(p=>p.id===t.projectId);
  document.getElementById('det-title').textContent=t.ticket+(isRenewed(t)?' (renovou '+((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0]+')':'');
  document.getElementById('det-sub').textContent=(proj?proj.name+' · ':'')+t.client+(t.prime?' · '+t.prime:'')+(inGrace?' · 🔄 Carência até '+graceCutoverDate(t):'');
  const hasOldInfo=t.oldTicket2||t.statusOld||t.expireOld||t.pending;

  const expiredBanner=isExpired?'<div style="background:#dc2626;color:white;padding:10px 14px;border-radius:var(--r);margin-bottom:10px;text-align:center;font-weight:700;font-size:14px;animation:expPulse 1.5s infinite">⛔ NÃO TRABALHAR — '+(isExpired==='grace'?'CARÊNCIA VENCIDA (renovar/liberar antes de cavar)':'TICKET VENCIDO ('+esc(t.expire)+')')+'</div>':'';

  // Ticket renovado cujo scraper 811 ainda não confirmou a data nova no portal.
  // Mostra aviso em vez de banner vermelho — evita falso "VENCIDO".
  const staleBanner=(isStale&&!inGrace)?'<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r);padding:10px 14px;margin-bottom:10px"><div style="font-size:12px;font-weight:700;color:#b45309">⏳ Aguardando sync do portal 811</div><div style="font-size:11px;color:#92400e;margin-top:3px">A data de vencimento exibida é do <strong>ticket anterior</strong> e ainda não foi confirmada pelo portal. O scraper 811 vai atualizar automaticamente no próximo ciclo.</div></div>':'';

  const graceBannerDet=(()=>{
    if(!inGrace)return'';
    const oldSt=t.statusOld||t.status_old||'Open';
    const oldNum=esc(((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0]);

    // NOVA PRIORIDADE: novo totalmente clareado sobrepõe graça.
    // Mesmo que o antigo fosse Open, se o novo já foi resolvido, mostra liberado.
    if(newTicketFullyCleared(t)){
      return'<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:var(--r);padding:10px 14px;margin-bottom:10px"><div style="font-size:12px;font-weight:700;color:#16a34a">✅ LIBERADO — Novo ticket clareou</div><div style="font-size:11px;color:#15803d;margin-top:3px">Todas as utilities do ticket <strong>'+esc(t.ticket)+'</strong> responderam <strong>Clear</strong>. As respostas do novo sobrepõem a carência do antigo ('+oldNum+').</div></div>';
    }

    if(oldSt==='Clear')return'<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:var(--r);padding:10px 14px;margin-bottom:10px"><div style="font-size:12px;font-weight:700;color:#16a34a">✅ LIBERADO — Carência do ticket anterior</div><div style="font-size:11px;color:#15803d;margin-top:3px">Status efetivo: <strong>Clear</strong> até 23:59 de '+graceCutoverDate(t)+'. Utilities do ticket antigo ('+oldNum+') ainda válidas.</div></div>';
    return'<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r);padding:10px 14px;margin-bottom:10px"><div style="font-size:12px;font-weight:700;color:#b45309">⚠ Carência — Ticket anterior era '+esc(oldSt)+'</div><div style="font-size:11px;color:#92400e;margin-top:3px">O ticket antigo ('+oldNum+') <strong>não estava liberado</strong>. Status mantido como <strong>'+esc(oldSt)+'</strong> até '+graceCutoverDate(t)+'. Após essa data, segue as respostas do ticket novo.</div></div>';
  })();

  // ── WATCH & PROTECT / PRIVATE LOCATOR BANNERS ──
  const pendingText=(t.pending||'').toUpperCase();
  const wpBanner=pendingText.includes('WATCH & PROTECT')
    ?'<div style="background:#fef2f2;border:2px solid #fca5a5;border-radius:var(--r);padding:12px 14px;margin-bottom:10px;cursor:pointer" onclick="alert(\'⚠️ WATCH & PROTECT\\n\\nEste ticket tem utility com instalação CRÍTICA.\\nUm representante da utility DEVE estar presente durante toda a escavação.\\n\\nNÃO inicie a escavação sem a presença do técnico.\\nSe não entrarem em contato 24h antes, ligue para o número listado no campo Pending.\')">'
    +'<div style="font-size:13px;font-weight:700;color:#dc2626">⚠️ WATCH & PROTECT — Representante obrigatório</div>'
    +'<div style="font-size:11px;color:#991b1b;margin-top:4px">Utility com instalação crítica exige presença de técnico durante escavação. <strong>Toque aqui para mais detalhes.</strong></div>'
    +'</div>':'';
  const pvtBanner=pendingText.includes('PRIVATE LOCATOR')
    ?'<div style="background:#faf5ff;border:2px solid #d8b4fe;border-radius:var(--r);padding:12px 14px;margin-bottom:10px">'
    +'<div style="font-size:13px;font-weight:700;color:#7c3aed">🔒 PRIVATE LOCATOR — Locator privado necessário</div>'
    +'<div style="font-size:11px;color:#6b21a8;margin-top:4px">Este ticket tem utilities com instalações privadas (3H). Contrate um locator privado antes de escavar.</div>'
    +'</div>':'';

  const nsBanner=_isNoShowReleased(t)?'<div style="background:#fff3e0;border:2px solid #fed7aa;border-radius:var(--r);padding:10px 14px;margin-bottom:10px"><div style="font-size:13px;font-weight:700;color:#d97706">🟠 LIBERADO POR NO-SHOW</div><div style="font-size:11px;color:#b45309;margin-top:4px">Fibra atingiu o 4º no-show e o resto está Clear. Status efetivo <strong>Clear</strong> — mas é necessário <strong>localizar a fibra em campo</strong> antes de escavar.</div></div>':'';
  document.getElementById('det-info').innerHTML=expiredBanner+staleBanner+graceBannerDet+wpBanner+pvtBanner+nsBanner
    +`<div class="mp-row"><span class="mp-key">Status</span><span class="mp-val" style="color:${c};font-weight:700">${esc(effStatusLabel(t))}${inGrace?' <span style="font-size:10px;color:#7c3aed;font-weight:600">(🔄 carência)</span>':''}${t.status_locked?' 🔒':''}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Empresa</span><span class="mp-val">${esc(t.company||'—')}</span></div>`
    +(t.prime?`<div class="mp-row"><span class="mp-key">Prime</span><span class="mp-val">${esc(t.prime)}</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Local</span><span class="mp-val">${esc(t.location)}, ${esc(t.state)}</span></div>`
    +(t.county?`<div class="mp-row"><span class="mp-key">County</span><span class="mp-val" style="cursor:pointer;color:var(--accent);text-decoration:underline" onclick="gotoContactsForCounty('${esc(t.county).replace(/'/g,"\\\\'")}','${esc(t.state)}');return false;" title="Ver contatos que atendem ${esc(t.county)} County">📞 ${esc(t.county)} County</span></div>`:'')
    +`<div class="mp-row"><span class="mp-key">Footage</span><span class="mp-val">${fmtProgress(t)}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Campo</span><span class="mp-val">`
    +(isAdmin&&!isSharedView
      ?`<span style="cursor:pointer;color:var(--accent)" onclick="quickEditProgress(${t.id});return false;" title="Registrar baixa parcial">${t.completedFeet?(t.completedFeet.toLocaleString()+' ft ✏'):'📝 Registrar baixa'}</span>`
      :(t.completedFeet?t.completedFeet.toLocaleString()+' ft':'—'))
    +`</span></div>`
    +(t.completedFeet?miniProgressBar(t):'')
    +`<div class="mp-row"><span class="mp-key">Tipo</span><span class="mp-val">${esc(t.tipo||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Job #</span><span class="mp-val">${esc(t.job||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Endereço</span><span class="mp-val">${esc(t.address||'—')}</span></div>`
    +`<div class="mp-row"><span class="mp-key">Expira</span><span class="mp-val"${isExpired?' style="color:#dc2626;font-weight:700"':(isStale?' style="color:#b45309"':'')}>${isStale?'⏳ aguardando sync 811':esc(t.expire||'—')}${isExpired?(isExpired==='grace'?' ⚠ CARÊNCIA VENCIDA':' ⚠ VENCIDO'):''}</span></div>`
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
  // Badge "🔒 Travado" só aparece quando realmente travado
  if(t.status_locked){lockBadge.style.display='';}
  else{lockBadge.style.display='none';}
  // Botão de desbloqueio: sempre visível pra admin, habilitado apenas quando há lock ativo.
  // Feedback visual (disabled + opacity) indica pro admin que "não há bloqueio ativo agora".
  if(!isSharedView&&isAdmin){
    unlockBtn.style.display='';
    unlockBtn.disabled=!t.status_locked;
    unlockBtn.title=t.status_locked
      ? 'Remove trava manual — sincronização 811 volta a atualizar status'
      : 'Nenhum bloqueio ativo';
  }else{
    unlockBtn.style.display='none';
  }
  // Project lock indicator on the Projeto button
  const projBtn=document.getElementById('det-proj-btn');
  if(projBtn)projBtn.innerHTML=t.project_locked?'📁 Projeto 🔒':'📁 Projeto';
  // Seção de Danos v2: lista de registros individuais (ticket_damages)
  const damageBadge=document.getElementById('det-damage-badge');
  const damageBadgeWrap=document.getElementById('det-damage-badge-wrap');
  const damageSection=document.getElementById('field-damage-section');
  const damageBtn=document.getElementById('det-damage-btn');
  const damageListEl=document.getElementById('det-damage-list');

  // Carrega damages do banco (async — se falhar, lista vazia sem quebrar o modal)
  loadTicketDamages(t.id).then(function(){
    const count=(_currentDamages||[]).length;
    // Sincroniza damageCount local (defensivo — caso tabela e contador estejam fora de sync)
    if(t.damageCount!==count){
      t.damageCount=count;
    }
    // Badge
    if(damageBadge&&damageBadgeWrap){
      if(count>0){
        damageBadge.textContent='⚠ '+count+(count===1?' dano':' danos');
        damageBadgeWrap.classList.remove('hidden');
      }else{
        damageBadgeWrap.classList.add('hidden');
      }
    }
    // Lista
    if(damageListEl){
      damageListEl.innerHTML=renderDamagesList();
    }
  });

  // Seção visível pra admin logado. Em share view, só aparece se tem damage (info pública).
  const dmgCountSync=parseInt(t.damageCount)||0;
  if(damageSection){
    if(isSharedView){
      damageSection.style.display=dmgCountSync>0?'':'none';
    }else{
      damageSection.style.display='';
    }
  }
  // Botão de registrar só pra admin
  if(damageBtn){
    damageBtn.style.display=(!isSharedView&&isAdmin)?'':'none';
  }

  if(!isSharedView&&isAdmin){
    document.getElementById('det-edit-btn').classList.remove('hidden');
    document.getElementById('det-draw-btn').classList.remove('hidden');
    const renewBtn=document.getElementById('det-renew-btn');
    if(t.status!=='Closed'&&t.status!=='Cancel')renewBtn.classList.remove('hidden');
    else renewBtn.classList.add('hidden');
    // Botao 'Editar Renovacao' aparece SO em tickets que ja foram renovados
    const editRenewBtn=document.getElementById('det-edit-renewal-btn');
    if(editRenewBtn){
      if(isRenewed(t))editRenewBtn.classList.remove('hidden');
      else editRenewBtn.classList.add('hidden');
    }
    document.getElementById('field-status-section').style.display='';
  }else{
    document.getElementById('det-edit-btn').classList.add('hidden');
    document.getElementById('det-draw-btn').classList.add('hidden');
    document.getElementById('det-renew-btn').classList.add('hidden');
    const erb=document.getElementById('det-edit-renewal-btn');if(erb)erb.classList.add('hidden');
    document.getElementById('field-status-section').style.display='none';
  }
  renderHistory(t);renderMiniMap(t);renderUtils(t);renderSecondNotices(t);openModal('ov-detail');
}

// ── EXPIRED TICKET FULLSCREEN ALERT ──
function showExpiredAlert(t,kind){
  if(_expAlertEl){_expAlertEl.remove();_expAlertEl=null;}
  const isGrace=kind==='grace';
  const el=document.createElement('div');
  el.id='expired-alert-overlay';
  el.innerHTML='<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;padding:20px;text-align:center">'
    +'<div style="font-size:60px;margin-bottom:16px">⛔</div>'
    +'<div style="font-size:28px;font-weight:700;color:white;margin-bottom:8px;text-shadow:0 2px 8px rgba(0,0,0,.3)">NÃO TRABALHAR</div>'
    +'<div style="font-size:20px;color:rgba(255,255,255,.9);margin-bottom:16px">'+(isGrace?'CARÊNCIA VENCIDA':'TICKET VENCIDO')+'</div>'
    +'<div style="font-family:var(--mono);font-size:24px;color:white;padding:8px 20px;background:rgba(0,0,0,.3);border-radius:12px;margin-bottom:8px">'+esc(t.ticket)+'</div>'
    +'<div style="font-size:16px;color:rgba(255,255,255,.8);margin-bottom:20px">'+(isGrace?'Carência venceu — ticket novo ainda não liberado':'Expirou em '+esc(t.expire))+'</div>'
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
    ?[...t.history].reverse().map(h=>{
        // No-show feito sob OUTRO número (ticket renovado): marca discreta pra não confundir
        // com o número atual — o painel de 2nd notices já não conta esses.
        const _oldTk=(h&&h.sn&&String(h.sn.tk||'').trim()&&String(h.sn.tk).trim()!==String(t.ticket||'').trim())?String(h.sn.tk).trim():'';
        const _mark=_oldTk?` <span style="font-size:9px;color:var(--muted)">· nº antigo ${esc(_oldTk)}</span>`:'';
        return `<div class="hist-item"><div class="hist-dot" style="background:${h.color||'#9a9888'}"></div><div style="flex:1"><div style="color:var(--text2);font-size:12px">${esc(h.action)}${_mark}</div><div class="hist-time">${fmtDt(h.ts)}</div></div></div>`;
      }).join('')
    :'<div style="color:var(--muted);font-size:12px">Sem histórico</div>';
}

async function quickEditFootage(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  const val=prompt('Footage total para '+t.ticket+':',t.footage||0);
  if(val===null)return;
  const num=parseInt(val)||0;
  t.footage=num;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'Footage: '+num+' ft',color:'#1a6cf0'});
  // Pergunta concluído logo em seguida
  const val2=prompt('Footage CONCLUÍDO no campo (0 = sem baixa parcial):',t.completedFeet||0);
  if(val2!==null){
    const done=Math.max(0,parseInt(val2)||0);
    if(done!==t.completedFeet){
      const old=t.completedFeet||0;
      t.completedFeet=done;
      t.history.push({ts:Date.now(),action:`Progresso campo: ${old} → ${done} ft`,color:'#16a34a'});
    }
  }
  const ok=await saveTicketToDb(t);
  if(ok){toast('Footage atualizado: '+num+' ft'+(t.completedFeet?' ('+t.completedFeet+' concluído)':''),'success');openTicketDetail(id);syncAll();}
}

// ── QUICK EDIT: Baixa parcial (só concluído) ──
async function quickEditProgress(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  const total=t.footage||0;
  const val=prompt('Footage concluído no campo para '+t.ticket+(total?' (total: '+total+' ft)':'')+':',t.completedFeet||0);
  if(val===null)return;
  const done=Math.max(0,parseInt(val)||0);
  if(done===t.completedFeet)return;
  const old=t.completedFeet||0;
  t.completedFeet=done;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:`Progresso campo: ${old} → ${done} ft`,color:'#16a34a'});
  const ok=await saveTicketToDb(t);
  if(ok){
    const pct=total>0?Math.round(done/total*100):0;
    toast(`Progresso: ${done}/${total} ft (${pct}%)`,'success');
    openTicketDetail(id);syncAll();
  }
}

// ── TICKET RENEWAL ──
async function renewTicket(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  const newNum=prompt('Número do ticket NOVO (renovação de '+t.ticket+'):');
  if(!newNum||!newNum.trim())return;
  const newTicket=newNum.trim();

  // ── Verifica duplicado: primeiro local, depois NO BANCO ──
  let dup=tickets.find(x=>x.ticket===newTicket&&x.id!==t.id);
  if(!dup){
    // Checa no Supabase (o scraper pode ter importado sem refresh local)
    try{
      const{data:dbDup}=await sb.from('tickets').select('*').eq('ticket',newTicket).neq('id',t.id).maybeSingle();
      if(dbDup){
        // Encontrou no banco — carrega como dup local para merge
        dup=dbToTicket(dbDup);
        console.log('[Renew] Duplicado encontrado no banco (não estava local):',dbDup.id);
      }
    }catch(e){console.warn('[Renew] Erro ao checar duplicado no banco:',e);}
  }

  let merged=false;
  // Captura dados do ticket ATUAL (antigo) ANTES de qualquer merge.
  // NORMALIZA o expire aqui — evita propagar formato poluído (ex: "04/15/26 Time: 23:59")
  // pro expire_old do ticket novo.
  const oldNum=t.ticket;
  const oldExpire=normalizeExpire(t.expire);
  const oldStatus=t.status;
  const oldStatusLocked=t.status_locked;

  if(dup){
    if(!confirm('Ticket '+newTicket+' já existe no sistema (importado pelo scraper).\n\nDeseja MESCLAR?\n• O trajeto e dados do ticket atual serão mantidos\n• O registro duplicado ('+newTicket+') será removido\n• O número será atualizado para '+newTicket))return;
    // Copia expire do duplicado se disponível (expire do ticket NOVO)
    // Fix bug #10: usa _eod() em vez de new Date() direto — parse MM/DD/YYYY confiável
    // (new Date('05/13/2026') é unreliable em Safari antigo → pode retornar Invalid Date → comparação NaN)
    if(dup.expire&&dup.expire!=='—'&&(!t.expire||t.expire==='—'||_eod(dup.expire)>_eod(t.expire))){
      t.expire=dup.expire;
    }
    // Deleta o duplicado via Supabase client (fix: usava SB_H que não existe no frontend)
    try{
      const{error:delErr}=await sb.from('tickets').delete().eq('id',dup.id);
      if(!delErr){
        const localIdx=tickets.findIndex(x=>x.id===dup.id);
        if(localIdx>=0)tickets.splice(localIdx,1);
        toast('Registro duplicado removido.','success');
        merged=true;
      }else{
        console.error('[Renew] Erro ao deletar duplicado:',delErr);
        toast('Erro ao remover duplicado: '+delErr.message,'danger');
        return;
      }
    }catch(e){console.error('Erro ao deletar duplicado:',e);toast('Erro ao remover duplicado','danger');return;}
  }else{
    if(!confirm('Renovar ticket?\n\nANTIGO: '+t.ticket+' (expira '+t.expire+')\nNOVO: '+newTicket+'\n\nO ticket manterá status até o vencimento do antigo.\nApós vencer, precisará de novas liberações.'))return;
  }
  // Cadeia de tickets anteriores (suporta múltiplas renovações)
  const prevChain=t.oldTicket2||t.old_ticket2||'';
  const fullChain=prevChain?oldNum+' → '+prevChain:oldNum;
  // Fix bug #16 (dualidade camelCase/snake_case): escreve SÓ em camelCase.
  // ticketToDb converte pra snake_case na hora do save. Antes, o código escrevia nos
  // DOIS formatos por paranoia, mas snake_case era ignorado por ticketToDb — código morto
  // que confundia manutenção. Leituras mantêm fallback defensivo (t.oldTicket2 || t.old_ticket2)
  // pra proteger contra subscribes raw do Supabase que pulam dbToTicket.
  t.oldTicket2=fullChain;
  t.expireOld=oldExpire;
  t.statusOld=oldStatus;
  // No-shows pertencem ao número em que foram feitos: carimba as entradas ainda sem
  // `tk` com o número ANTIGO antes de renomear, pra NÃO contarem no ticket renovado.
  (t.history||[]).forEach(h=>{if(h&&h.sn&&!String(h.sn.tk||'').trim())h.sn.tk=oldNum;});
  // Atualiza para novo ticket
  t.ticket=newTicket;
  // Nova data de expire — comportamento depende do estado:
  // - IN/FL: zera pra forçar o scraper 811 a buscar do portal Indiana/Florida.
  //   Sem isso, o expire fica com a data do antigo e, assim que a graça termina,
  //   dispara falso banner de "VENCIDO". filter_tickets_for_sync detecta expire
  //   vazio e prioriza esse ticket no próximo scrape.
  // - IL: NÃO há scraper de data (JULIE só lê respostas, tickets IL são manuais).
  //   Se zerasse aqui, o ticket ficaria órfão sem expire pra sempre.
  //   Pede a data nova ao operador.
  if((t.state||'').toUpperCase()==='IL'){
    let newExpireRaw=prompt(
      'Nova data de vencimento do ticket '+newTicket+' (formato MM/DD/YYYY):\n\n'+
      'Ticket antigo expirava em: '+(oldExpire||'sem data'),
      ''
    );
    if(newExpireRaw===null){
      // Cancelou o prompt — aborta a renovação inteira sem deixar lixo
      toast('Renovação cancelada.','warn');
      return;
    }
    const newExpire=normalizeExpire(newExpireRaw);
    if(!newExpire){
      toast('Data inválida — renovação cancelada. Use MM/DD/YYYY.','danger');
      return;
    }
    t.expire=newExpire;
  }else{
    // IN/FL: zera pra scraper buscar do portal
    t.expire='';
  }
  if(t.projectId)t.project_locked=true;// trava projeto ao renovar
  // Ticket novo nasce Open e destravado — as utilities respondem DE NOVO. statusOld preserva
  // o Clear do antigo p/ a carência exibir Clear até o vencimento; sem isto, a trava manual 🔒
  // do antigo viajava pro novo e congelava Clear pra sempre, mesmo com utilities pendentes.
  t.status='Open';
  t.status_locked=false;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'[RENOVAÇÃO] '+oldNum+' → '+newTicket+(merged?' (mesclado)':'')+' (graça até '+oldExpire+')',color:'#7c3aed'});
  const ok=await saveTicketToDb(t);
  if(ok){
    const isIL=(t.state||'').toUpperCase()==='IL';
    toast(
      '✅ Ticket renovado: '+oldNum+' → '+newTicket+
      (isIL ? ' (vence '+t.expire+')' : ' — aguardando sync 811 pra confirmar vencimento'),
      'success'
    );
    closeModal('ov-detail');syncAll();
    setTimeout(()=>openTicketDetail(t.id),300);
  }else{
    // Rollback completo incluindo expire (que foi zerado acima)
    // Fix bug #16: rollback também em camelCase só — ticketToDb é a guarda única.
    t.ticket=oldNum;t.oldTicket2=prevChain;t.expireOld='';t.statusOld='';t.expire=oldExpire;
    t.status=oldStatus;t.status_locked=oldStatusLocked;
    t.history.pop();
    toast('Erro ao salvar renovação. Tente dar Refresh (⟳) e tentar novamente.','danger');
  }
}
// ── EDIT/UNDO RENEWAL ──
// Permite editar manualmente os campos da renovação (oldTicket2, statusOld, expireOld)
// ou DESFAZER a renovação. Desfazer RESTAURA o ticket antigo:
//   - Se o antigo já existe como registro próprio (renovação detectada via sync): só limpa a
//     relação e ele reaparece (deixa de ser superseded).
//   - Se o antigo sumiu porque a renovação RENOMEOU o mesmo registro (renewTicket): reverte o
//     número de volta pro antigo e restaura status/expire antigos. O número novo deixa de existir.
// Útil quando o auto-sync inferiu renovação errada ou o operador renovou por engano.
async function editRenewal(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  if(!isRenewed(t)){toast('Esse ticket não tem renovação registrada.','warn');return;}
  const curOld=(t.oldTicket2||t.old_ticket2||'').trim();
  const curStatus=(t.statusOld||'').trim();
  const curExpire=(t.expireOld||'').trim();
  const action=prompt(
    'EDITAR RENOVAÇÃO do ticket '+t.ticket+'\n\n'+
    'Atual:\n'+
    '  Ticket Antigo: '+curOld+'\n'+
    '  Status Antigo: '+(curStatus||'(vazio)')+'\n'+
    '  Expira Antigo: '+(curExpire||'(vazio)')+'\n\n'+
    'O que fazer?\n'+
    '  1 = Editar dados da renovação (campos individuais)\n'+
    '  2 = DESFAZER renovação (volta pro ticket antigo — restaura número, status e vencimento)\n'+
    '  (cancelar pra sair sem mudar)',
    '1'
  );
  if(!action)return;
  const choice=action.trim();
  if(choice==='2'){
    // Predecessor imediato (1º da chain) = pra onde o ticket volta; resto = níveis mais antigos.
    const oldNum=curOld.split('→')[0].trim();
    const restChain=curOld.split('→').slice(1).map(s=>s.trim()).filter(Boolean).join(' → ');
    if(!oldNum){toast('Não há número de ticket antigo registrado pra restaurar.','warn');return;}
    // O antigo já existe como registro próprio? (renovação via sync deixou o antigo oculto/superseded)
    const oldExists=tickets.some(x=>String(x.ticket||'').trim()===oldNum&&x.id!==t.id);
    // Backups pra rollback em caso de erro no save
    const _bk={ticket:t.ticket,status:t.status,expire:t.expire,locked:t.status_locked,old:curOld,sOld:curStatus,eOld:curExpire};

    if(oldExists){
      // Antigo existe oculto — limpar a relação faz ele reaparecer (rebuildSupersededSet).
      if(!confirm('DESFAZER renovação?\n\nO ticket antigo '+oldNum+' JÁ existe na base (estava oculto como substituído).\nVou limpar a relação de renovação do '+t.ticket+' — o antigo volta a aparecer.\n\nContinuar?'))return;
      t.oldTicket2=restChain;t.statusOld='';t.expireOld='';
      t.history=t.history||[];
      t.history.push({ts:Date.now(),action:'[RENOVAÇÃO DESFEITA] relação limpa — antigo '+oldNum+' reexibido'+(restChain?' / chain restante: '+restChain:''),color:'#dc2626'});
    }else{
      // Antigo NÃO existe (a renovação RENOMEOU este registro). Reverte: volta a ser o antigo.
      if(!confirm('DESFAZER renovação (voltar pro ticket antigo)?\n\n'+
        'Este ticket vai VOLTAR a ser:\n'+
        '  Número:  '+t.ticket+'  →  '+oldNum+'\n'+
        '  Status:  '+(curStatus||'(mantém atual)')+'\n'+
        '  Expira:  '+(curExpire||'(mantém atual)')+'\n\n'+
        'O número '+t.ticket+' deixa de existir.'+(restChain?'\n(Continua como renovação de '+restChain+'.)':'')+'\n\n'+
        'Obs.: respostas de utilities do número novo podem ficar órfãs — o sync 811 reorganiza.\n\nContinuar?'))return;
      // No-shows feitos sob o número NOVO não devem contar no antigo restaurado.
      (t.history||[]).forEach(h=>{if(h&&h.sn&&!String(h.sn.tk||'').trim())h.sn.tk=_bk.ticket;});
      t.ticket=oldNum;
      if(curStatus)t.status=curStatus;   // restaura status antigo (se foi registrado na renovação)
      if(curExpire)t.expire=curExpire;   // restaura vencimento antigo (se foi registrado)
      t.status_locked=false;             // lock antigo não foi preservado — sync/operador reajusta
      t.oldTicket2=restChain;t.statusOld='';t.expireOld='';
      t.history=t.history||[];
      t.history.push({ts:Date.now(),action:'[RENOVAÇÃO DESFEITA] '+_bk.ticket+' → '+oldNum+' (ticket antigo restaurado)'+(restChain?' / chain restante: '+restChain:''),color:'#dc2626'});
    }

    const ok=await saveTicketToDb(t);
    if(ok){
      toast(oldExists?('Renovação desfeita — '+oldNum+' voltou a aparecer.'):('Renovação desfeita — ticket voltou a ser '+oldNum+'.'),'success');
      rebuildSupersededSet();
      closeModal('ov-detail');syncAll();
      setTimeout(()=>openTicketDetail(t.id),300);
    }else{
      // Rollback completo
      t.ticket=_bk.ticket;t.status=_bk.status;t.expire=_bk.expire;t.status_locked=_bk.locked;
      t.oldTicket2=_bk.old;t.statusOld=_bk.sOld;t.expireOld=_bk.eOld;t.history.pop();
      toast('Erro ao salvar. Tente Refresh (⟳) e novamente.','danger');
    }
    return;
  }
  if(choice!=='1'){toast('Opção inválida — só 1 ou 2.','warn');return;}
  // Edita campo a campo
  const newOld=prompt('Ticket Antigo (número, em chain use " → " entre números mais antigos)\n\nDeixe vazio = limpar:',curOld);
  if(newOld===null)return;
  const newStatus=prompt('Status do Ticket Antigo (Open, Clear, Damage, Closed, Cancel):',curStatus||'Clear');
  if(newStatus===null)return;
  const newExpireRaw=prompt('Expira do Ticket Antigo (MM/DD/AAAA) — usado pra calcular o grace period:',curExpire);
  if(newExpireRaw===null)return;
  const newExpire=normalizeExpire((newExpireRaw||'').trim());
  if(newExpireRaw.trim()&&!newExpire){toast('Data inválida. Use MM/DD/AAAA.','danger');return;}
  t.oldTicket2=newOld.trim();
  t.statusOld=newStatus.trim();
  t.expireOld=newExpire;
  t.history=t.history||[];
  t.history.push({ts:Date.now(),action:'[RENOVAÇÃO EDITADA] antigo: '+curOld+'→'+t.oldTicket2+' / status: '+curStatus+'→'+t.statusOld+' / expira: '+curExpire+'→'+t.expireOld,color:'#b45309'});
  const ok=await saveTicketToDb(t);
  if(ok){
    toast('Renovação atualizada.','success');
    rebuildSupersededSet();
    closeModal('ov-detail');syncAll();
    setTimeout(()=>openTicketDetail(t.id),300);
  }else{
    t.oldTicket2=curOld;t.statusOld=curStatus;t.expireOld=curExpire;t.history.pop();
    toast('Erro ao salvar. Tente Refresh (⟳) e novamente.','danger');
  }
}

function isInRenewalGrace(t){
  if(!isRenewed(t))return false;
  let cutoverMs=0;

  // 1. expireOld direto (definido na renovação manual)
  let cutover=t.expireOld||t.expire_old||'';
  if(cutover&&cutover!=='—'){cutoverMs=_eod(cutover).getTime();if(isNaN(cutoverMs))cutoverMs=0;}

  // 2. Fallback: busca expiração do ticket antigo no sistema
  if(!cutoverMs){
    const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
    if(oldNum){
      const oldT=tickets.find(x=>String(x.ticket).trim()===oldNum);
      if(oldT&&oldT.expire&&oldT.expire!=='—')cutoverMs=_eod(oldT.expire).getTime();
    }
  }

  if(!cutoverMs)return false;
  return cutoverMs>=Date.now();// true até 23:59:59 do cutover (expire_old)
}
function isRenewed(t){return !!(t.oldTicket2||t.old_ticket2);}

/**
 * Ticket renovado cujo expire AINDA NÃO foi atualizado pelo scraper 811.
 * expire == expireOld significa que renewTicket herdou a data do antigo
 * e o portal ainda não forneceu a data real do ticket novo.
 * Nesses casos, NÃO confiar em t.expire pra alertar vencimento — é um
 * valor "stale" (obsoleto), herança da renovação, não verdade do portal.
 */
function expireIsStale(t){
  if(!isRenewed(t))return false;
  const eo=String(t.expireOld||t.expire_old||'').trim();
  const en=String(t.expire||'').trim();
  // Se expire foi zerado pela renovação OU é idêntico ao do antigo,
  // o scraper ainda não atualizou.
  if(!en||en==='—')return true;
  return !!eo&&eo===en;
}

/**
 * Ticket NOVO (renovado) já foi totalmente resolvido pelas utilities?
 *
 * Retorna true quando, durante a graça, todas as utilities do ticket novo
 * responderam E todas estão em status liberador (Clear/Private/Marked/Unmarked).
 * Nenhuma Pending, nenhuma "No Response" sobrando.
 *
 * Usado pra SOBREPOR a graça do antigo: se o novo já foi completamente
 * clareado, não faz sentido segurar status Open só porque o antigo estava
 * Open — temos clear real pra trabalhar.
 *
 * Requisitos (todos os 3):
 *   - Cache de utilities carregado (senão não dá pra avaliar)
 *   - Novo tem pelo menos 1 resposta registrada
 *   - Todas as respostas em status liberador
 */
function newTicketFullyCleared(t){
  if(!utilCacheLoaded)return false;
  if(!isRenewed(t))return false;
  const releasedStatuses=new Set(['Clear','Private','Marked','Unmarked']);
  // WI Relo-No-Show: avalia o conjunto COMBINADO (Relo + Standard herdadas)
  const merged=getMergedUtils(t);
  if(merged)return merged.length>0&&merged.every(u=>releasedStatuses.has(u.status));
  // FL/IN/IL: lógica original
  const newKey=String(t.ticket||'').trim();
  const newUtils=utilCache[newKey]||[];
  if(newUtils.length===0)return false;
  return newUtils.every(u=>releasedStatuses.has(u.status));
}

/**
 * Status efetivo — durante período de carência de renovação.
 *
 * Regra do usuário (legal):
 *   "A lei reconhece o ticket Clear até o vencimento. Por isso a carência existe."
 *
 *   1. ANTIGO não-Clear + NOVO não-Clear → Open + pendências do novo
 *      (não há carência protetora — ambos têm problema, mostra a verdade do novo)
 *
 *   2. ANTIGO Clear + NOVO qualquer coisa → Clear durante toda a carência
 *      (a carência DO ANTIGO continua valendo legalmente até a data de vencimento;
 *       a equipe pode cavar com base no certificado do antigo. Pendências do novo
 *       ficam visíveis pelas tags, mas o STATUS continua Clear até fim da carência.
 *       Quando a carência expirar, isInRenewalGrace() volta false e a regra natural
 *       do código vê o status real do novo — que pode ser Open com pendências.)
 *
 *   3. ANTIGO qualquer + NOVO totalmente Clear → Clear sem carência
 *      (novo já foi resolvido por completo, sobrepõe — temos clear pra trabalhar)
 *
 *   4. Status travado manualmente → respeita sempre (override absoluto)
 */
// Liberação automática por no-show de FIBRA (Eric): se TODAS as pendências do ticket
// são utilities de FIBRA que chegaram ao 4º no-show (3 falhas reais: 2nd/3rd/4th) e
// todo o resto está Clear → o ticket é liberado. effectiveStatus vira 'Clear'; a COR
// é laranja (via _isNoShowReleased nos pontos de render) com a nota "localizar fibra".
function _isNoShowReleased(t){
  if(!utilCacheLoaded||!t||t.status_locked)return false;
  if((t.status||'')!=='Open')return false;      // só tickets em aberto
  const utils=getTicketUtils(t.ticket);
  if(!utils.length)return false;
  const released=new Set(['Clear','Private','Marked','Unmarked']);
  let fiberExhausted=false;
  for(const u of utils){
    if(released.has(u.status))continue;          // utility já liberada
    if(isFiberUtility(u.utility_name)&&countSecondNotices(t,u.utility_name)>=4){fiberExhausted=true;continue;}
    return false;                                // pendência que NÃO é fibra no 4º no-show → bloqueia
  }
  return fiberExhausted;                          // libera só se ≥1 fibra esgotada e nada mais bloqueia
}
// Cor / classe de badge / label do status considerando a liberação por no-show (laranja).
function effColor(t){return _isNoShowReleased(t)?'#d97706':scol(effectiveStatus(t));}
function effStatusCls(t){return _isNoShowReleased(t)?'noshow':(effectiveStatus(t)||'').toLowerCase();}
function effStatusLabel(t){return _isNoShowReleased(t)?'Clear · no-show':effectiveStatus(t);}
function effectiveStatus(t){
  // Status travado manualmente = sempre respeitar
  if(t.status_locked)return t.status;

  if(isRenewed(t)&&isInRenewalGrace(t)){
    // Regra 3: novo já totalmente resolvido — sobrepõe carência
    if(utilCacheLoaded&&newTicketFullyCleared(t))return 'Clear';

    // Regra 2: antigo era Clear → mantém Clear durante TODA a carência,
    // independente do que o novo está mostrando.
    // Status do antigo vem de statusOld (snapshot da hora da renovação)
    // OU do cache em runtime (se as utilities do antigo continuam liberadas).
    const oldStatusSnapshot=(t.statusOld||t.status_old||'').trim();
    if(oldStatusSnapshot==='Clear')return 'Clear';

    // Mesmo se o snapshot não dizia Clear, pode ser que o antigo tenha
    // virado Clear depois (ex: utility respondeu Clear no antigo mesmo
    // após a renovação). Cache em runtime checa.
    // IMPORTANTE: Cancel (Closed by DHL) = utility NUNCA respondeu →
    // linhas podem estar lá sem marcação. NÃO é Clear.
    // Not Participating = utility não tem rede na área → seguro, ignorar.
    // Pra considerar antigo Clear: TODAS as utilities relevantes
    // (exceto Not Participating) devem estar em status liberador.
    if(utilCacheLoaded){
      const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
      if(oldNum){
        const oldUtils=utilCache[oldNum]||[];
        const released=new Set(['Clear','Private','Marked','Unmarked']);
        const notParticipating=u=>(u.response_text||'').toLowerCase().includes('not participating');
        const relevant=oldUtils.filter(u=>!notParticipating(u));
        if(relevant.length>0&&relevant.every(u=>released.has(u.status))){
          return 'Clear';
        }
      }
    }

    // Regra 1: antigo não-Clear → mostra status real do novo (sem proteção de carência)
    return t.status;
  }
  if(_isNoShowReleased(t))return 'Clear';
  return t.status;
}

/** Retorna a data de corte da carência (para exibição) */
function graceCutoverDate(t){
  let d=t.expireOld||t.expire_old||'';
  if(!d||d==='—'){
    const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
    if(oldNum){const oldT=tickets.find(x=>String(x.ticket).trim()===oldNum);if(oldT&&oldT.expire&&oldT.expire!=='—')d=oldT.expire;}
  }
  return d||'—';
}

/**
 * Bloqueio efetivo de trabalho ("não cavar"), baseado em DATAS — NUNCA suprimido pela trava.
 * Retorna '' (ok), 'expired' (expire do próprio ticket vencido) ou 'grace' (renovado,
 * carência vencida e ticket novo ainda não liberado). Regra do Eric: a trava 🔒 não esconde
 * o vencido; e se o ticket novo já está Clear de verdade (utilities respondidas), vale a
 * data do novo e não a carência — por isso só bloqueia quando newTicketFullyCleared é falso.
 */
function ticketExpiredEffective(t){
  const es=effectiveStatus(t);
  if(es==='Closed'||es==='Cancel')return'';
  if(expireIsStale(t))return'';                    // data ainda não confirmada pelo portal
  if(isRenewed(t)&&isInRenewalGrace(t))return'';   // carência ativa → coberto
  const now=new Date();
  if(t.expire&&t.expire!=='—'&&_eod(t.expire)<now)return'expired';
  if(isRenewed(t)&&!newTicketFullyCleared(t)){
    // Carência só existe quando o antigo era Clear (protegia o trabalho).
    // Se antigo era Open/Pending, não havia carência — ticket novo é Open normal.
    // Regra 1 do effectiveStatus: "ANTIGO não-Clear → sem carência protetora"
    const snap=(t.statusOld||t.status_old||'').trim();
    let oldWasClear=snap==='Clear';
    if(!oldWasClear&&utilCacheLoaded){
      const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
      if(oldNum){const ou=utilCache[oldNum]||[];if(ou.length>0&&!ou.some(u=>u.status==='Pending'))oldWasClear=true;}
    }
    if(oldWasClear){
      const cut=graceCutoverDate(t);
      if(cut&&cut!=='—'&&_eod(cut)<now){
        // Carência do antigo expirou. Mas se o ticket NOVO tem expire próprio
        // e ainda não venceu, é um Open normal com pendências — não bloqueia.
        // Só mostra "CARÊNCIA VENCIDA" se o novo NÃO tem data (aguardando sync)
        // ou se a data do novo também já venceu.
        if(t.expire&&t.expire!=='—'&&_eod(t.expire)>=now)return'';
        return'grace';
      }
    }
  }
  return'';
}

async function setManualStatus(newStatus){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  const old=t.status;const wasLocked=t.status_locked;
  t.status=newStatus;t.status_locked=true;
  t.history=t.history||[];

  // Determina ts do evento: pra Clear manual, usa data da última resposta real
  // (Clear/Private/Marked) entre as utilities — assim o ticket é contabilizado
  // no dashboard na data em que o último locator respondeu, não na hora do clique.
  let evtTs=Date.now();
  let actionTxt=`Status manual: ${old} → ${newStatus} 🔒`;
  if(newStatus==='Clear'){
    // Durante graça, olhar respostas do ticket antigo (mesma regra do effectiveStatus/renderUtils)
    const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    const oldTicketNum=inGrace?((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim():'';
    const queryTicket=inGrace&&oldTicketNum?oldTicketNum:String(t.ticket||'').trim();
    const resps=utilCache[queryTicket]||[];
    let lastResponded=0;
    for(const u of resps){
      if(u.status!=='Clear'&&u.status!=='Private'&&u.status!=='Marked')continue;
      const raw=u.responded_at;if(!raw)continue;
      const ms=new Date(raw).getTime();
      if(!isNaN(ms)&&ms>lastResponded)lastResponded=ms;
    }
    if(lastResponded>0){
      evtTs=lastResponded;
      const d=new Date(lastResponded);
      const dateLabel=String(d.getMonth()+1).padStart(2,'0')+'/'+String(d.getDate()).padStart(2,'0')+'/'+d.getFullYear();
      actionTxt=`Status manual → Clear em ${dateLabel} 🔒 (última resposta 811)`;
    }
  }

  t.history.push({ts:evtTs,action:actionTxt,color:scol(newStatus)});
  const ok=await saveTicketToDb(t);
  if(ok){toast(`✅ Status: ${old} → ${newStatus} (travado)`,'success');openTicketDetail(currentDetailId);syncAll();}
  else{t.status=old;t.status_locked=wasLocked;t.history.pop();toast(`Erro ao salvar — status revertido para ${old}`,'danger');}
}
async function unlockStatus(id){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  // Auth: só admin (feature request explícito — botão já tá oculto pra não-admin,
  // mas revalida aqui pra defesa em profundidade contra chamada direta no console)
  if(!isAdmin){toast('Apenas admin pode desbloquear status','warn');return;}
  // Idempotência: se não tá travado, não faz nada (evita escrita desnecessária no banco)
  if(!t.status_locked){toast('Status já está em modo automático','info');return;}
  // Confirm leve pra feedback do que vai acontecer
  if(!confirm('Desbloquear automação 811?\n\nO status "'+t.status+'" será mantido por enquanto,\nmas a próxima sincronização 811 poderá sobrescrever conforme as utilities responderem.'))return;
  const wasLocked=t.status_locked;
  t.status_locked=false;
  t.history=t.history||[];// Fix bug #20: garante array antes de push
  t.history.push({ts:Date.now(),action:'Automação 811 desbloqueada 🔓',color:'#1a6cf0'});
  const ok=await saveTicketToDb(t);
  if(ok){toast('🔓 Automação desbloqueada — 811 volta a atualizar o status','success');openTicketDetail(id);}
  else{
    // Rollback
    t.status_locked=wasLocked;
    t.history.pop();
    toast('Erro ao salvar — tente dar refresh e novamente','danger');
  }
}
async function unlockProject(id){
  const t=tickets.find(x=>x.id===id||x.id===currentDetailId);if(!t)return;
  t.project_locked=false;
  t.history=t.history||[];// Fix bug #20: garante array antes de push
  t.history.push({ts:Date.now(),action:'Projeto desbloqueado 🔓',color:'#1a6cf0'});
  const ok=await saveTicketToDb(t);
  if(ok){toast('🔓 Projeto desbloqueado','success');openTicketDetail(t.id);}
}

// ═══════════ DAMAGE TRACKING v2 (tabela separada ticket_damages) ═══════════
// Damage é um registro individual na tabela ticket_damages com (utility, description,
// reported_by). damage_count no ticket é mantido sincronizado (soma dos registros).
// Permite editar, remover e ver detalhes de cada dano individualmente.

// Carrega os damages do ticket. Chamada dentro de openTicketDetail.
async function loadTicketDamages(ticketId){
  try{
    const{data,error}=await sb
      .from('ticket_damages')
      .select('id,seq,utility,description,reported_by,created_at')
      .eq('ticket_id',ticketId)
      .order('seq',{ascending:true});
    if(error){
      console.warn('[Damages] load error:',error.message);
      _currentDamages=[];
      return;
    }
    _currentDamages=data||[];
  }catch(e){
    console.error('[Damages] load error:',e);
    _currentDamages=[];
  }
}

// Renderiza a lista de damages dentro da seção "Danos reportados" do modal.
// Retorna HTML. Chamada do openTicketDetail.
function renderDamagesList(){
  if(!_currentDamages.length){
    return'<div style="color:var(--muted);font-size:11px;padding:6px 0">Nenhum dano registrado.</div>';
  }
  return _currentDamages.map(function(d){
    const dt=d.created_at?new Date(d.created_at):null;
    const dateStr=dt?
      String(dt.getMonth()+1).padStart(2,'0')+'/'+String(dt.getDate()).padStart(2,'0')+' '+
      String(dt.getHours()).padStart(2,'0')+':'+String(dt.getMinutes()).padStart(2,'0'):'—';
    const showActions=!isSharedView&&isAdmin;
    const actionsHtml=showActions?
      '<div style="display:flex;gap:4px;flex-shrink:0">'
      +'<button class="btn btn-sm" onclick="openDamageModal(currentDetailId,'+d.id+')" style="font-size:10px;padding:2px 6px" title="Editar dano">✏</button>'
      +'<button class="btn btn-sm btn-danger" onclick="deleteDamage('+d.id+')" style="font-size:10px;padding:2px 6px" title="Remover dano">🗑</button>'
      +'</div>':'';
    const titleLine='<div style="font-size:11px;color:var(--text2);font-weight:600">#'+d.seq+' · '+esc(dateStr)+(d.reported_by?' · <span style="color:var(--muted);font-weight:400">por '+esc(d.reported_by)+'</span>':'')+'</div>';
    const utilityLine=d.utility?'<div style="font-size:12px;font-weight:700;color:var(--amber);margin-top:2px">'+esc(d.utility)+'</div>':'';
    const descLine=d.description?'<div style="font-size:11px;color:var(--text);margin-top:2px;white-space:pre-wrap;word-break:break-word">'+esc(d.description)+'</div>':'';
    return'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px;padding:6px 0;border-bottom:1px solid var(--border)">'
      +'<div style="flex:1;min-width:0">'+titleLine+utilityLine+descLine+'</div>'
      +actionsHtml
      +'</div>';
  }).join('');
}

// Abre modal pra registrar (damageId=null) ou editar (damageId setado).
function openDamageModal(id,damageId){
  const t=tickets.find(x=>x.id===id);if(!t)return;
  if(!isAdmin){toast('Apenas admin pode registrar danos','warn');return;}
  _editingDamageId=damageId||null;
  const editing=!!_editingDamageId;
  const existing=editing?_currentDamages.find(d=>d.id===_editingDamageId):null;

  // Título do modal
  const titleEl=document.querySelector('#ov-damage .mtitle');
  if(titleEl)titleEl.textContent=editing?'⚠ Editar dano #'+(existing?existing.seq:''):'⚠ Registrar dano';

  // Label do botão confirmar
  const confirmBtn=document.querySelector('#ov-damage [data-action="confirm-register-damage"]');
  if(confirmBtn)confirmBtn.textContent=editing?'Salvar alterações':'Registrar dano';

  // Info do ticket
  const infoEl=document.getElementById('damage-ticket-info');
  if(infoEl){
    const totalNow=(_currentDamages||[]).length;
    infoEl.innerHTML='Ticket <strong style="font-family:var(--mono)">'+esc(t.ticket)+'</strong> — '+esc(t.location||'')+(totalNow>0&&!editing?' · <span style="color:#b45309">Já tem '+totalNow+' dano(s) registrado(s)</span>':'');
  }
  // Preenche inputs (com valores atuais se editando, vazio se novo)
  document.getElementById('damage-utility').value=editing&&existing?existing.utility||'':'';
  document.getElementById('damage-description').value=editing&&existing?existing.description||'':'';
  openModal('ov-damage');
}

// Salva: INSERT se _editingDamageId==null, UPDATE se setado.
async function confirmRegisterDamage(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  if(!isAdmin){toast('Apenas admin pode registrar danos','warn');return;}
  const utility=(document.getElementById('damage-utility').value||'').trim();
  const description=(document.getElementById('damage-description').value||'').trim();
  const editing=!!_editingDamageId;

  try{
    if(editing){
      // UPDATE — só utility e description mudam, seq/created_at/reported_by mantém
      const{error}=await sb.from('ticket_damages').update({
        utility:utility,
        description:description
      }).eq('id',_editingDamageId);
      if(error)throw error;
      toast('Dano atualizado','success');
    }else{
      // INSERT — calcula próximo seq (max+1 dos damages atuais do ticket)
      const nextSeq=(_currentDamages.length>0?Math.max(..._currentDamages.map(d=>d.seq||0)):0)+1;
      const reportedBy=currentUserEmail||'';
      const{error}=await sb.from('ticket_damages').insert({
        ticket_id:t.id,
        seq:nextSeq,
        utility:utility,
        description:description,
        reported_by:reportedBy
      });
      if(error)throw error;
      // Incrementa damage_count no ticket (mantido sincronizado pro analytics)
      t.damageCount=(parseInt(t.damageCount)||0)+1;
      await saveTicketToDb(t);
      toast('⚠ Dano registrado — #'+nextSeq,'success');
    }
    _editingDamageId=null;
    closeModal('ov-damage');
    // Recarrega damages e re-renderiza modal
    await loadTicketDamages(t.id);
    openTicketDetail(currentDetailId);
    syncAll();
  }catch(e){
    console.error('[Damages] save error:',e);
    toast('Erro ao salvar — tente novamente','danger');
  }
}

// Remove um dano (hard delete). Decrementa damage_count do ticket.
async function deleteDamage(damageId){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  if(!isAdmin){toast('Apenas admin pode remover danos','warn');return;}
  const dmg=_currentDamages.find(d=>d.id===damageId);
  if(!dmg){toast('Dano não encontrado','warn');return;}
  const label='#'+dmg.seq+(dmg.utility?' — '+dmg.utility:'');
  if(!confirm('Remover dano '+label+'?\n\nEssa ação é permanente e não pode ser desfeita.'))return;
  try{
    const{error}=await sb.from('ticket_damages').delete().eq('id',damageId);
    if(error)throw error;
    // Decrementa damage_count
    t.damageCount=Math.max(0,(parseInt(t.damageCount)||0)-1);
    await saveTicketToDb(t);
    toast('Dano removido','success');
    await loadTicketDamages(t.id);
    openTicketDetail(currentDetailId);
    syncAll();
  }catch(e){
    console.error('[Damages] delete error:',e);
    toast('Erro ao remover — tente novamente','danger');
  }
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
      const c=effColor(t);
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
    // Durante graça, MOSTRA utilities do ticket ANTIGO — exceto quando o NOVO já
    // foi totalmente clareado (nesse caso, as respostas reais e autoritativas são
    // as do ticket novo; o antigo virou histórico).
    const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    const novoClareado=inGrace&&newTicketFullyCleared(t);
    const oldTicketNum=inGrace?((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0]:'';
    // novoClareado → lê do ticket novo. Caso contrário, lógica atual: em graça lê do antigo.
    const queryTicket=novoClareado?t.ticket:(inGrace&&oldTicketNum?oldTicketNum:t.ticket);

    const{data,error}=await sb
      .from('ticket_811_responses')
      .select('utility_name,status,response_text,responded_at')
      .eq('ticket_num',queryTicket)
      .order('utility_name');

    if(error)throw error;

    // Banner de graça
    let graceBanner='';
    if(inGrace){
      const oldSt=t.statusOld||t.status_old||'Open';
      if(novoClareado){
        // Novo já clareou — banner verde claro explicando a sobreposição.
        graceBanner='<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:var(--r);padding:8px 12px;margin-bottom:8px">'
          +'<div style="font-size:11px;font-weight:700;color:#16a34a">✅ LIBERADO — Utilities do novo ticket ('+esc(t.ticket)+')</div>'
          +'<div style="font-size:10px;color:#15803d;margin-top:2px">Novo ticket totalmente resolvido — respostas abaixo sobrepõem a carência do antigo ('+esc(oldTicketNum)+').</div>'
          +'</div>';
      }else if(oldSt==='Clear'){
        graceBanner='<div style="background:#f0fdf4;border:1px solid #86efac;border-radius:var(--r);padding:8px 12px;margin-bottom:8px">'
          +'<div style="font-size:11px;font-weight:700;color:#16a34a">✅ LIBERADO — Utilities do ticket antigo ('+esc(oldTicketNum)+')</div>'
          +'<div style="font-size:10px;color:#15803d;margin-top:2px">Válido até '+graceCutoverDate(t)+'. Após essa data, novas liberações serão necessárias do ticket '+esc(t.ticket)+'.</div>'
          +'</div>';
      }else{
        graceBanner='<div style="background:#fffbeb;border:1px solid #fde68a;border-radius:var(--r);padding:8px 12px;margin-bottom:8px">'
          +'<div style="font-size:11px;font-weight:700;color:#b45309">⚠ Ticket antigo ('+esc(oldTicketNum)+') era '+esc(oldSt)+'</div>'
          +'<div style="font-size:10px;color:#92400e;margin-top:2px">Pendências do ticket antigo permanecem. Carência até '+graceCutoverDate(t)+'.</div>'
          +'</div>';
      }
    }

    if(!data||!data.length){
      el.innerHTML=graceBanner+'<div style="color:var(--muted);font-size:12px">Sem dados de utilities</div>';
      if(sm)sm.textContent='';
      return;
    }
    // Fix 2026-05-14: detecta Not Participating como variante visual (badge laranja)
    // SÓ pra WI — em FL/IN/IL "3U: Not service provider" é Clear normal, sem badge especial.
    // WI: "Not Participating" = utility não participa do programa (badge laranja)
    // WI: "Closed by DHL" SEM "Not Participating" = prazo expirou (Cancel)
    const tState=(t.state||'').toUpperCase();
    const isNotPart=u=>{
      if(tState!=='WI')return false;
      const rt=(u.response_text||'').toLowerCase();
      return rt.includes('not participating')||rt.includes('not service provider');
    };
    data.forEach(u=>{ u._isNotPart=isNotPart(u); });
    const pending=data.filter(u=>u.status==='Pending');
    const cleared=data.filter(u=>(u.status==='Clear'||u.status==='Private')&&!u._isNotPart);
    const notpart=data.filter(u=>u._isNotPart);
    const marked=data.filter(u=>u.status==='Marked');
    const canceled=data.filter(u=>u.status==='Cancel');
    if(sm){
      const parts=[];
      if(inGrace)parts.push('<span style="color:#7c3aed">🔄 graça'+(novoClareado?' (novo clareou)':'')+'</span>');
      if(pending.length)parts.push(`<span style="color:var(--red)">${pending.length} pendente${pending.length>1?'s':''}</span>`);
      if(marked.length)parts.push(`<span style="color:var(--amber)">${marked.length} marcada${marked.length>1?'s':''}</span>`);
      if(notpart.length)parts.push(`<span style="color:#d97706">⚠ ${notpart.length} não participa</span>`);
      if(canceled.length)parts.push(`<span style="color:#dc2626">${canceled.length} cancelada${canceled.length>1?'s':''}</span>`);
      if(cleared.length)parts.push(`<span style="color:var(--green)">${cleared.length} clear</span>`);
      sm.innerHTML=parts.join(' · ');
    }
    const badgeClass={Pending:'ub-pending',Clear:'ub-clear',Marked:'ub-marked',Private:'ub-private',Unmarked:'ub-clear',Cancel:'ub-cancel'};
    const label={Pending:'Pendente',Clear:'Clear',Marked:'Marcado',Private:'Privado',Unmarked:'Desmarcado',Cancel:'Cancel'};
    // Override pra Not Participating: badge laranja + label especifico
    const getBadgeClass=u=>u._isNotPart?'ub-notpart':(badgeClass[u.status]||'ub-pending');
    const getLabel=u=>u._isNotPart?'⚠ Não Participa':(label[u.status]||u.status);
    const order={Pending:0,Marked:1,Private:2,Cancel:3,Clear:4,Unmarked:5};
    data.sort((a,b)=>{
      // Not Participating sempre depois de Cancel e antes de Clear normal
      const aO=a._isNotPart?3.5:(order[a.status]??9);
      const bO=b._isNotPart?3.5:(order[b.status]??9);
      return aO-bO;
    });
    el.innerHTML=graceBanner+data.map(u=>{
      const resp=(u.response_text||'').trim();
      let detail='';
      // Detail em LARANJA pro Not Participating (mais visivel)
      if(resp && u._isNotPart){
        const short=resp.length>80?resp.substring(0,80)+'…':resp;
        detail=`<div style="font-size:10px;color:#d97706;margin-top:2px;line-height:1.3;font-weight:600">⚠ ${esc(short)}</div>`;
      } else if(resp && u.status==='Cancel'){
        const short=resp.length>80?resp.substring(0,80)+'…':resp;
        detail=`<div style="font-size:10px;color:#dc2626;margin-top:2px;line-height:1.3;opacity:.85">${esc(short)}</div>`;
      } else if(resp && u.status==='Clear'){
        const short=resp.length>80?resp.substring(0,80)+'…':resp;
        detail=`<div style="font-size:10px;color:var(--green);margin-top:2px;line-height:1.3;opacity:.85">${esc(short)}</div>`;
      }
      return`<div style="display:flex;justify-content:space-between;align-items:flex-start;padding:6px 0;border-bottom:1px solid var(--border)"><div style="flex:1;min-width:0"><span class="util-name" style="display:block">${esc(u.utility_name)}</span>${detail}</div><span class="util-badge ${getBadgeClass(u)}" style="flex-shrink:0;margin-top:2px">${esc(getLabel(u))}</span></div>`;
    }).join('');
  }catch(e){el.innerHTML='<div style="color:var(--muted);font-size:12px">Erro ao carregar utilities</div>';}
}

/* ═══════════ 17. TICKET TABLE ═══════════ */
function riskScore(t){
  if(!utilCacheLoaded)return 0;
  // Tickets em carência de renovação → risco zero APENAS se antigo era Clear
  if(isRenewed(t)&&isInRenewalGrace(t)){
    const oldSt=(t.statusOld||t.status_old||'').toLowerCase();
    if(oldSt==='clear')return 0;
  }
  let s=0;const now=Date.now();
  if(t.expire&&t.expire!=='—'&&!expireIsStale(t)){
    const diff=(_eod(t.expire)-now)/86400000;
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
  const sta=document.getElementById('tbl-state')?.value||'';
  const pr=document.getElementById('tbl-proj').value;
  const pm=document.getElementById('tbl-prime')?.value||'';
  const ut=document.getElementById('tbl-util')?.value||'';
  const ed=document.getElementById('tbl-exp')?.value||'';

  const isCompletedProj=pr&&projects.find(p=>p.id===pr&&p.status==='Completed');
  const _stReal=st==='Sem Trajeto'?'':st;
  let f=filterTickets({status:_stReal,projectId:pr,prime:pm,state:sta,search:sr,utility:ut,expireDays:ed,excludeCompleted:!isCompletedProj,onlyGrace:_graceFilterActive});
  if(st==='Sem Trajeto')f=f.filter(t=>(!t.fieldPath||t.fieldPath.length<2)&&t.status!=='Cancel'&&t.status!=='Closed');

  f.sort((a,b)=>{
    if(sortCol==='risk'){const ra=riskScore(a),rb=riskScore(b);return sortAsc?ra-rb:rb-ra;}
    if(sortCol==='footage')return sortAsc?(a.footage||0)-(b.footage||0):(b.footage||0)-(a.footage||0);
    return sortAsc?String(a[sortCol]||'').localeCompare(String(b[sortCol]||'')):String(b[sortCol]||'').localeCompare(String(a[sortCol]||''));
  });

  const _totalFtTbl=f.reduce((s,t)=>s+(t.footage||0),0);
  document.getElementById('tbl-count').textContent=`${f.length} tickets · ${_totalFtTbl.toLocaleString()} ft`;
  document.getElementById('tbl-body').innerHTML=f.map(t=>{
    const pends=getTicketPendingUtils(String(t.ticket).trim());
    const pendNames=pends.map(p=>p.utility_name);
    const pendChips=pendNames.length
      ?`<div style="display:flex;flex-wrap:wrap;gap:2px;margin-top:3px">`
        +pendNames.slice(0,3).map(n=>`<span style="font-size:9px;padding:1px 5px;border-radius:10px;background:var(--red-bg);color:var(--red);font-family:var(--mono);white-space:nowrap">${esc(n.length>20?n.substring(0,20)+'…':n)}</span>`).join('')
        +(pendNames.length>3?`<span style="font-size:9px;color:var(--muted)">+${pendNames.length-3}</span>`:'')
        +`</div>`
      :'';
    const es=effectiveStatus(t);const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    return`<tr onclick="openTicketDetail(${t.id})">`
      +`<td style="font-family:var(--mono);font-weight:500">${esc(t.ticket)}${isRenewed(t)?'<div style="font-size:9px;color:#7c3aed">🔄 renovou '+esc(((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0])+(inGrace?' (carência)':'')+'</div>':''}</td>`
      +`<td style="color:var(--text2);font-size:12px">${esc(t.client)}</td>`
      +`<td style="color:var(--muted);font-size:12px">${esc(t.prime||'—')}</td>`
      +`<td>${esc(t.location)}, ${esc(t.state)}</td>`
      +`<td class="tc-${es.toLowerCase()}">${esc(es)}${inGrace?' <span style="font-size:9px;color:#7c3aed">🔄</span>':''}${pendChips}</td>`
      +`<td style="font-family:var(--mono)">${t.completedFeet?fmtProgress(t):(t.footage+' ft')}</td>`
      +`<td style="font-family:var(--mono);font-size:12px">${esc(t.expire||'—')}</td>`
      +`<td style="color:var(--muted)">${esc(t.tipo||'—')}</td>`
      +`<td onclick="event.stopPropagation()"><div style="display:flex;gap:5px"><button class="btn btn-sm" onclick="openTicketDetail(${t.id})">Ver</button>${isAdmin?`<button class="btn btn-sm" onclick="editFromTbl(${t.id})">Editar</button>`:''}</div></td>`
      +`</tr>`;
  }).join('');
}
function sortBy(col){sortAsc=sortCol===col?!sortAsc:true;sortCol=col;renderTable();}
function editFromTbl(id){currentDetailId=id;editCurrentTicket();}

/* ═══════════ 18. DASHBOARD ═══════════ */

/**
 * Fix bug #9: renderiza a página ativa (Dashboard OU Analytics).
 * Usado pelos selects de filtro em renderClearedStats, renderProgressoFootage e
 * renderClearTimeMetrics — essas funções são chamadas em AMBAS as páginas.
 * Sem isso, trocar filtro no Analytics re-renderizava o Dashboard (invisível),
 * e o usuário via "filtro não funciona".
 */
function refreshDashOrAnalytics(){
  const ap=document.querySelector('.page.active')?.id;
  if(ap==='pg-analytics')renderAnalytics();
  else renderDash();
}

function renderDash(){
  const states=[...new Set(tickets.map(t=>t.state).filter(Boolean))].sort();
  const dsf=dashStateVal;
  const fTickets=filterTickets({state:dsf});
  const now=Date.now();const week=7*86400000;

  const total=fTickets.length;
  const openT=fTickets.filter(t=>effectiveStatus(t)==='Open');
  const clearT=fTickets.filter(t=>effectiveStatus(t)==='Clear');
  const damageT=fTickets.filter(t=>effectiveStatus(t)==='Damage');
  const open=openT.length,clear=clearT.length,damage=damageT.length;
  const totalFt=fTickets.reduce((s,t)=>s+(t.footage||0),0);
  const openFt=openT.reduce((s,t)=>s+(t.footage||0),0);
  const clearFt=clearT.reduce((s,t)=>s+(t.footage||0),0);
  const damageFt=damageT.reduce((s,t)=>s+(t.footage||0),0);
  const completedFt=fTickets.reduce((s,t)=>s+(t.completedFeet||0),0);
  const pctCampo=totalFt>0?Math.round(completedFt/totalFt*100):0;
  const noMap=fTickets.filter(t=>(!t.fieldPath||t.fieldPath.length<2)&&t.status!=='Cancel'&&t.status!=='Closed');
  const noProj=fTickets.filter(t=>!t.projectId&&t.status!=='Cancel'&&t.status!=='Closed');
  const _sd=_soonDays||10;
  const soon=fTickets.filter(t=>{if(!t.expire||t.expire==='—')return false;if(isSuperseded(t))return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;if(expireIsStale(t))return false;if(t.status==='Closed'||t.status==='Cancel')return false;const d=_eod(t.expire);const diff=(d-Date.now())/86400000;return diff>=0&&diff<=_sd;});

  function wCount(status,start,end){
    return fTickets.filter(t=>t.history&&t.history.some(h=>{
      const a=(h.action||'').toLowerCase();
      if(h.ts<start||h.ts>=end)return false;
      if(status==='Open')return a.includes('importado')||a.includes('ticket criado');
      // Clear: auto-clear, auto 811 (não revertido, não cancelado), ou clear manual
      return a.includes('auto-clear')
        ||(a.includes('auto 811')&&!a.includes('revertido')&&!a.includes('cancelado'))
        ||(a.includes('status manual')&&a.includes('→ clear'));
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
  +'<div style="display:grid;grid-template-columns:1fr 1.4fr 1.4fr 1.2fr 1.2fr 1.2fr;gap:8px;margin-bottom:16px">'
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
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid #3b82f6;cursor:pointer" onclick="showNoProjModal()">'
  +'<div style="font-size:9px;color:var(--muted);text-transform:uppercase;margin-bottom:3px">Sem projeto</div>'
  +'<div style="font-size:20px;font-weight:700;font-family:var(--mono);color:#3b82f6">'+noProj.length+'</div>'
  +'<div style="font-size:10px;color:var(--muted)">de '+total+' ativos</div>'
  +'<div style="font-size:10px;color:'+(noProj.length>0?'#3b82f6':'var(--muted)')+';margin-top:3px">'+(noProj.length>0?Math.round(noProj.length/Math.max(total,1)*100)+'% sem projeto':'todos alocados')+'</div></div>'
  +'<div class="stat-card" style="padding:10px 12px;border-left:3px solid var(--purple);cursor:pointer" onclick="nav(\'tickets\');setTimeout(()=>{document.getElementById(\'tbl-stat\').value=\'Sem Trajeto\';renderTable();},100)">'
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
  +soon.sort((a,b)=>{const da=_eod(a.expire)-now;const db=_eod(b.expire)-now;return da-db;}).map(t=>{
    const d2=Math.ceil((_eod(t.expire)-now)/86400000);
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

  // Cleared stats, W&P alert, private locator, sync timer
  +renderClearedStats(fTickets)
  +renderWatchAndProtectAlert(fTickets)
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
  // Fix bug #3: limpa timeout anterior antes de criar novo.
  // Evita cascata exponencial quando renderDash/loadLastSync chamam essa função.
  if(_syncTimerId)clearTimeout(_syncTimerId);
  _syncTimerId=setTimeout(updateSyncTimer,60000);
}

/* ═══════════ 19. PROJECTS ═══════════ */
function cleanLoc(l){return(l||'').replace(/\s*(Inside|Near|inside|near)\s*:.*/i,'').trim()||l;}
function projDropLabel(p){
  const ts=tickets.filter(t=>t.projectId===p.id);
  const allLocs=[...new Set(ts.map(t=>cleanLoc(t.location)).filter(Boolean))];
  const filtLocs=allLocs.filter(l=>l.toUpperCase()!==((p.state||'').toUpperCase()));
  const locs=(filtLocs.length?filtLocs:allLocs).join(', ');
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
    // Conta status pelo effectiveStatus — tickets renovados em grace com
    // status=Open mas effective=Clear devem aparecer como Clear no card.
    // Pra Closed/Cancel usa raw (effectiveStatus nao muda eles).
    const _es=(t)=>effectiveStatus(t);
    const openC=ts.filter(t=>_es(t)==='Open').length,clearC=ts.filter(t=>_es(t)==='Clear').length,damageC=ts.filter(t=>_es(t)==='Damage').length,closedC=ts.filter(t=>t.status==='Closed').length;
    const ticketFt=ts.reduce((s,t)=>s+(t.footage||0),0);const projTotal=p.totalFeet||ticketFt||1;
    const campoFt=ts.reduce((s,t)=>s+(t.completedFeet||0),0);const pctCampoP=projTotal>0?Math.round(campoFt/projTotal*100):0;
    const clearFtP=ts.filter(t=>_es(t)==='Clear').reduce((s,t)=>s+(t.footage||0),0);
    const openFtP=ts.filter(t=>_es(t)==='Open').reduce((s,t)=>s+(t.footage||0),0);
    const concluidoFt=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);
    const damageFtV=ts.filter(t=>_es(t)==='Damage').reduce((s,t)=>s+(t.footage||0),0);
    const pctConcluido=projTotal>0?Math.round(concluidoFt/projTotal*100):0;
    const pctClear=projTotal>0?Math.round(clearFtP/projTotal*100):0;
    const pctOpen=projTotal>0?Math.round(openFtP/projTotal*100):0;
    const pctDamage=projTotal>0?Math.round(damageFtV/projTotal*100):0;
    const locations=[...new Set(ts.map(t=>cleanLoc(t.location)).filter(Boolean))];
    const locsFiltered=locations.filter(l=>l.toUpperCase()!==((p.state||'').toUpperCase()));
    const aggLocStr=(locsFiltered.length?locsFiltered:locations).join(', ')||p.state;
    // Prioriza a localidade descrita no modal (p.desc); só usa cidades agregadas dos tickets como fallback.
    const locStr=p.desc||aggLocStr;
    return`<div class="pcard"><div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:3px"><div style="flex:1"><div style="display:flex;align-items:baseline;gap:8px;flex-wrap:wrap"><div class="pcard-name">${esc(p.name)}</div><div style="font-size:12px;color:var(--muted);font-family:var(--mono)">📍 ${esc(locStr)}</div></div></div><span class="status-pill pill-${p.status==='Active'?'active':'done'}" style="flex-shrink:0;margin-left:8px">${esc(p.status)}</span></div><div class="pcard-meta">${esc(p.client)} · ${esc(p.state)}</div><div class="prog-bar"><div style="width:${pctClear}%;background:var(--green)"></div><div style="width:${Math.min(pctOpen,100-pctClear)}%;background:var(--red)"></div><div style="width:${Math.min(pctDamage,100-pctClear-pctOpen)}%;background:#f59e0b"></div><div style="width:${Math.min(pctConcluido,100-pctClear-pctOpen-pctDamage)}%;background:var(--text)"></div></div>${campoFt?`<div style="margin-top:4px"><div style="display:flex;justify-content:space-between;align-items:center;font-size:10px;color:#3b82f6;font-weight:600;margin-bottom:2px"><span>📐 Campo: ${campoFt.toLocaleString()} ft</span><span>${pctCampoP}%</span></div><div style="height:4px;background:var(--border);border-radius:2px;overflow:hidden"><div style="width:${pctCampoP}%;height:100%;background:#3b82f6;border-radius:2px"></div></div></div>`:''}<div class="pcard-stats"><div class="pstat"><span class="pstat-val" style="color:var(--red)">${openC}</span><span class="pstat-lbl">Open</span></div><div class="pstat"><span class="pstat-val" style="color:var(--green)">${clearC}</span><span class="pstat-lbl">Clear</span></div><div class="pstat"><span class="pstat-val" style="color:var(--amber)">${damageC}</span><span class="pstat-lbl">Damage</span></div><div class="pstat"><span class="pstat-val" style="color:var(--muted)">${closedC}</span><span class="pstat-lbl">Closed</span></div><div class="pstat"><span class="pstat-val">${ts.length}</span><span class="pstat-lbl">Total</span></div></div><div style="font-size:12px;color:var(--muted);font-family:var(--mono);margin-bottom:10px">${ticketFt.toLocaleString()} ft${clearFtP?` · <span style="color:var(--green);font-weight:700">${clearFtP.toLocaleString()} ft clear</span>`:''}${p.totalFeet?' / '+p.totalFeet.toLocaleString()+' ft total':''}</div><div style="display:flex;gap:6px;flex-wrap:wrap"><button class="btn btn-sm" onclick="shareProject('${p.id}')" style="background:var(--accent);color:white;border-color:var(--accent)">📤 Compartilhar</button><button class="btn btn-sm" onclick="openProjectMap('${p.id}')">Ver no mapa</button><button class="btn btn-sm" onclick="exportProjectReport('${p.id}')" style="background:#3b82f6;color:white;border-color:#3b82f6">📊 Relatório</button>${isAdmin?`<button class="btn btn-sm" onclick="editProject('${p.id}')">Editar</button><button class="btn btn-sm btn-danger" onclick="openDelProj('${p.id}')">Excluir</button>`:''}</div></div>`;
  };

  const filterTag=sf?` · filtrado por ${esc(sf)}`:'';
  const exportBar=`<div style="grid-column:1/-1;margin-bottom:12px;padding:12px 14px;background:linear-gradient(90deg,#1e40af 0%,#3b82f6 100%);border-radius:8px;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;color:white"><div><div style="font-weight:700;font-size:14px">📋 Clear pra trabalhar${filterTag}</div><div style="font-size:11px;opacity:0.9">Clear (próprio/carência) e Pendências em aberto — respeitam o filtro de estado acima</div></div><div style="display:flex;gap:8px;flex-wrap:wrap"><button class="btn" onclick="openPendingReport()" style="background:rgba(255,255,255,0.18);color:white;border:1px solid rgba(255,255,255,0.55);padding:8px 16px;font-weight:700;border-radius:6px">📞 Pendências</button><button class="btn" onclick="exportClearReport()" style="background:white;color:#1e40af;border:none;padding:8px 16px;font-weight:700;border-radius:6px">📥 Baixar Clear</button></div></div>`;
  g.innerHTML=exportBar+(active.length?active.map(renderCard).join(''):'<div style="color:var(--muted);font-size:13px">Nenhum projeto ativo.</div>');
}

function openProjectMap(pid){nav('map');setTimeout(()=>{document.getElementById('proj-filter').value=pid;onProjFilter();},200);}

// ── COMPLETED PROJECTS ──
function updateCompletedSidebar(){
  const completed=projects.filter(p=>p.status==='Completed');
  const navBtn=document.getElementById('nav-completed');
  const countEl=document.getElementById('nav-completed-count');
  if(!navBtn)return;
  if(!completed.length){navBtn.style.display='none';return;}
  navBtn.style.display='';
  if(countEl)countEl.textContent='('+completed.length+')';
}

function renderCompletedPage(){
  const el=document.getElementById('completed-content');if(!el)return;
  const completed=projects.filter(p=>p.status==='Completed');
  if(!completed.length){
    el.innerHTML='<div class="page-title">📂 Histórico</div><div style="text-align:center;padding:40px;color:var(--muted)">Nenhum projeto finalizado</div>';
    return;
  }
  // Build tree: Ano → Cliente → Localização → Projeto
  const tree={};
  for(const p of completed){
    const ts=tickets.filter(t=>t.projectId===p.id);
    // Year: from first ticket created_at or fallback to current year
    const firstTs=ts.length?Math.min(...ts.map(t=>t.created_at?new Date(t.created_at).getTime():Date.now())):Date.now();
    const year=new Date(firstTs).getFullYear();
    const client=p.client||'Sem cliente';
    const locs=[...new Set(ts.map(t=>(t.location||'').replace(/\s*(Inside|Near).*/i,'').split(',')[0].trim()).filter(Boolean))];
    const loc=locs.length?locs.join(', '):(p.state||'—');
    if(!tree[year])tree[year]={};
    if(!tree[year][client])tree[year][client]={};
    if(!tree[year][client][loc])tree[year][client][loc]=[];
    tree[year][client][loc].push(p);
  }
  let html='<div class="page-title">📂 Histórico <span style="font-size:13px;font-weight:400;color:var(--muted)">'+completed.length+' projeto'+(completed.length>1?'s':'')+'</span></div>';
  // Render tree
  const years=Object.keys(tree).sort((a,b)=>b-a);
  for(const year of years){
    html+='<div style="margin-bottom:16px">'
      +'<div onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'\':\'none\';this.querySelector(\'.yr-arrow\').textContent=this.nextElementSibling.style.display===\'none\'?\'▶\':\'▼\'" style="cursor:pointer;padding:8px 12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);display:flex;align-items:center;gap:8px">'
      +'<span class="yr-arrow" style="font-size:10px;color:var(--muted)">▼</span>'
      +'<span style="font-size:14px;font-weight:700;font-family:var(--mono)">'+year+'</span>'
      +'</div>'
      +'<div style="margin-left:16px;margin-top:4px">';
    const clients=Object.keys(tree[year]).sort();
    for(const client of clients){
      html+='<div style="margin-bottom:8px">'
        +'<div onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'\':\'none\';this.querySelector(\'.cl-arrow\').textContent=this.nextElementSibling.style.display===\'none\'?\'▶\':\'▼\'" style="cursor:pointer;padding:6px 10px;display:flex;align-items:center;gap:8px">'
        +'<span class="cl-arrow" style="font-size:9px;color:var(--muted)">▼</span>'
        +'<span style="font-size:12px;font-weight:600;color:var(--text)">🏢 '+esc(client)+'</span>'
        +'</div>'
        +'<div style="margin-left:20px">';
      const locs=Object.keys(tree[year][client]).sort();
      for(const loc of locs){
        const projs=tree[year][client][loc];
        html+='<div style="margin-bottom:4px">'
          +'<div style="padding:4px 8px;font-size:11px;color:var(--muted);font-weight:600">📍 '+esc(loc)+'</div>'
          +'<div style="margin-left:16px">';
        for(const p of projs){
          const tCount=tickets.filter(t=>t.projectId===p.id&&!isSuperseded(t)).length;
          html+='<div onclick="openCompletedProjectDetail(\''+p.id+'\')" style="padding:6px 10px;border-radius:var(--r);cursor:pointer;display:flex;justify-content:space-between;align-items:center;transition:background .15s;border-bottom:1px solid var(--border)" onmouseover="this.style.background=\'var(--bg)\'" onmouseout="this.style.background=\'none\'">'
            +'<div style="display:flex;align-items:center;gap:8px">'
            +'<span style="font-size:12px">📋</span>'
            +'<span style="font-size:12px;font-weight:500;color:var(--text)">'+esc(p.name)+'</span>'
            +'</div>'
            +'<span style="font-size:10px;color:var(--muted);font-family:var(--mono)">'+tCount+' tickets</span>'
            +'</div>';
        }
        html+='</div></div>';
      }
      html+='</div></div>';
    }
    html+='</div></div>';
  }
  el.innerHTML=html;
}

function openCompletedProjectDetail(pid){
  const p=projects.find(x=>x.id===pid);if(!p)return;
  const ts=tickets.filter(t=>t.projectId===pid&&!isSuperseded(t));
  const clearC=ts.filter(t=>t.status==='Clear').length;
  const openC=ts.filter(t=>t.status==='Open').length;
  const totalFt=ts.reduce((s,t)=>s+(t.footage||0),0);
  // Summary cards
  const summary='<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:14px">'
    +'<div style="text-align:center;padding:10px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r)"><div style="font-size:20px;font-weight:700;font-family:var(--mono)">'+ts.length+'</div><div style="font-size:10px;color:var(--muted)">Total</div></div>'
    +'<div style="text-align:center;padding:10px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r)"><div style="font-size:20px;font-weight:700;color:var(--green);font-family:var(--mono)">'+clearC+'</div><div style="font-size:10px;color:var(--green)">Clear</div></div>'
    +'<div style="text-align:center;padding:10px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r)"><div style="font-size:20px;font-weight:700;color:var(--red);font-family:var(--mono)">'+openC+'</div><div style="font-size:10px;color:var(--muted)">Open</div></div>'
    +'<div style="text-align:center;padding:10px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r)"><div style="font-size:20px;font-weight:700;font-family:var(--mono)">'+totalFt.toLocaleString()+'</div><div style="font-size:10px;color:var(--muted)">Footage</div></div>'
    +'</div>';
  // Ticket table
  const tblHtml='<div style="max-height:300px;overflow-y:auto"><table style="width:100%;font-size:11px;border-collapse:collapse"><thead><tr style="text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;border-bottom:1px solid var(--border)"><th style="padding:4px 6px">Ticket</th><th style="padding:4px 6px">Local</th><th style="padding:4px 6px">Status</th><th style="padding:4px 6px">Footage</th><th style="padding:4px 6px">Expira</th></tr></thead><tbody>'
    +ts.map(t=>{const es=effectiveStatus(t);return'<tr style="border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail('+t.id+')"><td style="padding:4px 6px;font-family:var(--mono);font-weight:600">'+esc(t.ticket)+'</td><td style="padding:4px 6px">'+esc((t.location||'').split(',')[0])+', '+esc(t.state)+'</td><td style="padding:4px 6px"><span class="sbadge b-'+es.toLowerCase()+'">'+esc(es)+'</span></td><td style="padding:4px 6px;font-family:var(--mono)">'+t.footage+'</td><td style="padding:4px 6px">'+esc(t.expire||'—')+'</td></tr>';}).join('')
    +'</tbody></table></div>';
  // Buttons
  const btns='<div style="margin-top:12px;display:flex;gap:6px;flex-wrap:wrap">'
    +'<button class="btn btn-sm" onclick="closeModal(\'ov-completed-proj\');nav(\'map\');setTimeout(()=>{document.getElementById(\'proj-filter\').value=\''+pid+'\';onProjFilter();},200)" style="background:var(--accent);color:white;border-color:var(--accent)">🗺️ Ver no mapa</button>'
    +(isAdmin?'<button class="btn btn-sm" onclick="reopenProject(\''+pid+'\')">🔓 Reabrir projeto</button>':'')
    +'</div>';
  // Modal
  let ov=document.getElementById('ov-completed-proj');
  if(!ov){
    ov=document.createElement('div');ov.id='ov-completed-proj';ov.className='overlay';
    ov.innerHTML='<div class="modal" style="max-width:700px"><div class="modal-header"><h3 id="cp-title"></h3><button class="modal-close" onclick="closeModal(\'ov-completed-proj\')">×</button></div><div id="cp-body" style="padding:16px"></div></div>';
    document.body.appendChild(ov);
  }
  document.getElementById('cp-title').textContent='📁 '+p.name;
  document.getElementById('cp-body').innerHTML='<div style="font-size:12px;color:var(--muted);margin-bottom:10px">'+esc(p.client)+' · '+esc(p.state)+(p.totalFeet?' · Meta: '+p.totalFeet.toLocaleString()+' ft':'')+'</div>'+summary+tblHtml+btns;
  openModal('ov-completed-proj');
}

function reopenProject(pid){
  const p=projects.find(x=>x.id===pid);if(!p)return;
  if(!confirm('Reabrir projeto "'+p.name+'"?'))return;
  p.status='Active';
  saveProjectToDb(p).then(ok=>{
    if(ok){toast('Projeto reaberto!','success');closeModal('ov-completed-proj');updateCompletedSidebar();syncAll();}
  });
}

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
  // Localidade vem direto de p.desc (campo dedicado).
  // Retrocompat: pra projetos antigos onde desc está vazio mas o name foi salvo concatenado
  // ("LOC — NUM"), faz split como antes. Eric salvando esses projetos uma vez já "limpa".
  if(p.desc){
    document.getElementById('pm-loc').value=p.desc;
    document.getElementById('pm-num').value=p.name;
  }else{
    const parts=p.name.split(' — ');
    if(parts.length>=2){document.getElementById('pm-loc').value=parts[0].trim();document.getElementById('pm-num').value=parts.slice(1).join(' — ').trim();}
    else{document.getElementById('pm-loc').value='';document.getElementById('pm-num').value=p.name;}
  }
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
  // Nome do projeto = exatamente o "NÚMERO DO PROJETO" inserido.
  // Localidade só é usada como fallback caso o número esteja vazio.
  const name=num||loc;
  const coordStr=document.getElementById('pm-coords').value.trim();
  let centerCoords=null;
  if(coordStr){const m=coordStr.match(/([-\d.]+)\s*,\s*([-\d.]+)/);if(m)centerCoords=[parseFloat(m[1]),parseFloat(m[2])];}
  // Localidade armazenada no campo `desc` (coluna `description` no Supabase) — é o que aparece
  // no card como linha cinza embaixo do nome. Fica separada das cidades agregadas dos tickets.
  const data={name,client:document.getElementById('pm-client').value,state:document.getElementById('pm-state').value,status:document.getElementById('pm-status').value,desc:loc,totalFeet:parseFloat(document.getElementById('pm-feet').value)||0,centerCoords,_manual:true};
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
  document.getElementById('move-proj-ticket-info').innerHTML=`Ticket: ${esc(t.ticket)}`+(t.project_locked?'<div style="margin-top:6px;font-size:11px;padding:5px 10px;background:var(--accent-bg);border:1px solid var(--border);border-radius:var(--r);color:var(--accent)">🔒 Projeto travado — ao salvar, o novo projeto será travado também.<br><button class="btn btn-sm" onclick="unlockProject('+t.id+')" style="margin-top:4px;font-size:10px">🔓 Desbloquear projeto</button></div>':'');
  const sel=document.getElementById('move-proj-sel');
  sel.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}"${t.projectId===p.id?' selected':''}>${esc(p.name)}</option>`).join('');
  openModal('ov-move-proj');
}
async function saveMoveProj(){
  const t=tickets.find(x=>x.id===currentDetailId);if(!t)return;
  t.projectId=document.getElementById('move-proj-sel').value;
  t.project_locked=!!t.projectId;// trava se tem projeto, destrava se "Sem projeto"
  t.history=t.history||[];// Fix bug #20: garante array antes de push
  t.history.push({ts:Date.now(),action:`Movido para projeto: ${projects.find(p=>p.id===t.projectId)?.name||'Sem projeto'}${t.project_locked?' 🔒':''}`,color:'#1a6cf0'});
  await saveTicketToDb(t);
  closeModal('ov-move-proj');openTicketDetail(currentDetailId);syncAll();toast('Projeto atualizado!'+(t.project_locked?' (travado)':''),'success');
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
  if(!p){
    // ID inválido — mostra tela de login normal em vez de pular pro app
    // (era enterViewer() antes, mas agora viewer pede senha; não faz sentido
    // pedir senha em fallback de URL inválida).
    toast('Projeto não encontrado','danger');
    isSharedView=false;sharedProjectId=null;
    history.replaceState(null,'',window.location.pathname);
    document.getElementById('login-screen').classList.remove('hidden');
    document.getElementById('login-screen').style.display='flex';
    return;
  }
  sharedProjectId=p.id;
  document.getElementById('login-screen').style.display='none';
  document.getElementById('app-shell').style.display='none';
  document.querySelectorAll('.page').forEach(pg=>pg.classList.remove('active'));
  document.getElementById('pg-shared').classList.add('active');

  // ── FIX: carregar utilCache no shared view ──
  // Sem isso, `newTicketFullyCleared()` retorna sempre false (linha do check
  // `if(!utilCacheLoaded)return false`) → o sistema acha que o ticket novo ainda
  // está em carência e mostra utilities pendentes do ticket antigo, divergindo do
  // que aparece no admin. Fire-and-forget pra não bloquear o render inicial; quando
  // termina, re-renderiza a lista lateral e (se aberto) o modal de detalhe.
  loadUtilCache().then(()=>{
    if(!isSharedView)return;
    if(typeof renderSharedList==='function')renderSharedList();
    if(currentDetailId){
      const tt=tickets.find(x=>x.id===currentDetailId);
      if(tt&&typeof renderUtils==='function')renderUtils(tt);
    }
  });

  // ── Auto-refresh do shared view (mesmo intervalo do admin) ──
  // Quem deixar o link aberto recebe atualização automática. Pula refresh enquanto
  // há modal aberto pra não interromper leitura do detalhe do ticket.
  if(_sharedRefreshId){clearInterval(_sharedRefreshId);_sharedRefreshId=null;}
  _sharedRefreshId=setInterval(async()=>{
    if(!isSharedView){clearInterval(_sharedRefreshId);_sharedRefreshId=null;return;}
    if(document.querySelector('.overlay.open'))return;
    try{
      const{data:pp}=await sb.from('projects').select('*').order('name');
      let _srt=[];let _sf2=0;
      while(true){const{data:pg}=await sb.from('tickets').select('*').order('ticket').range(_sf2,_sf2+999);if(!pg||!pg.length)break;_srt=_srt.concat(pg);if(pg.length<1000)break;_sf2+=1000;}
      if(pp)projects=pp.map(dbToProject);
      if(_srt.length)tickets=_srt.map(dbToTicket);
      rebuildSupersededSet();
      await loadUtilCache();
      if(typeof renderSharedList==='function')renderSharedList();
      console.log('[SharedRefresh] OK');
    }catch(e){console.error('[SharedRefresh]',e);}
  },AUTO_REFRESH_MS);
  const ts0=tickets.filter(t=>t.projectId===p.id);
  const allLocs0=[...new Set(ts0.map(t=>t.location).filter(Boolean).map(l=>cleanLoc(l)))];
  const filtLocs0=allLocs0.filter(l=>l.toUpperCase()!==((p.state||'').toUpperCase()));
  const locs0=(filtLocs0.length?filtLocs0:allLocs0).join(', ')||p.state||'';
  document.getElementById('shared-proj-name').textContent=locs0+(locs0?' — ':'')+p.name+(p.client?' · '+p.client:'');

  const ts=filterTickets({projectId:p.id});
  const openC=ts.filter(t=>t.status==='Open').length;
  const clearC=ts.filter(t=>t.status==='Clear').length;
  const damageC=ts.filter(t=>t.status==='Damage').length;
  const totalFt=ts.reduce((s,t)=>s+(t.footage||0),0);
  const shDoneFt=ts.reduce((s,t)=>s+(t.completedFeet||0),0);
  const shPctCampo=totalFt>0?Math.round(shDoneFt/totalFt*100):0;
  document.getElementById('shared-stats').innerHTML=`
    <div class="shared-stat"><span class="shared-stat-val">${ts.length}</span><span class="shared-stat-lbl">Total</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--red)">${openC}</span><span class="shared-stat-lbl">Open</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--green)">${clearC}</span><span class="shared-stat-lbl">Clear</span></div>
    <div class="shared-stat"><span class="shared-stat-val" style="color:var(--amber)">${damageC}</span><span class="shared-stat-lbl">Damage</span></div>
    <div class="shared-stat"><span class="shared-stat-val">${totalFt.toLocaleString()}</span><span class="shared-stat-lbl">Feet</span></div>`
    +(shDoneFt?`<div style="margin-top:6px;padding:6px 10px;background:var(--bg);border-radius:var(--r);font-size:11px"><div style="display:flex;justify-content:space-between;align-items:center;color:#3b82f6;font-weight:600;margin-bottom:3px"><span>📐 Progresso campo</span><span>${shPctCampo}%</span></div><div style="height:5px;background:var(--border);border-radius:3px;overflow:hidden"><div style="width:${shPctCampo}%;height:100%;background:#3b82f6;border-radius:3px"></div></div><div style="text-align:center;font-size:10px;color:var(--muted);margin-top:3px">${shDoneFt.toLocaleString()} / ${totalFt.toLocaleString()} ft</div></div>`:'');

  renderSharedList();
  initSharedMap(p);

  // Expiring tickets alert for field view.
  // "expired" = ticket com Open/Damage efetivo e expire passado (precisa alerta).
  // Clear mesmo com expire passado não precisa do alerta (trabalho liberado).
  const now=new Date();
  const expired=ts.filter(t=>{
    if(!t.expire||t.expire==='—'||isSuperseded(t)||expireIsStale(t))return false;
    const es=effectiveStatus(t);
    if(es!=='Open'&&es!=='Damage')return false;
    return _eod(t.expire)<now;
  });
  const expiring3d=ts.filter(t=>{if(!t.expire||t.expire==='—')return false;if(t.status==='Closed'||t.status==='Cancel')return false;if(isSuperseded(t))return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;if(expireIsStale(t))return false;const d=_eod(t.expire);const diff=(d-now)/86400000;return diff>=0&&diff<=3;});
  const expiring7d=ts.filter(t=>{if(!t.expire||t.expire==='—')return false;if(t.status==='Closed'||t.status==='Cancel')return false;if(isSuperseded(t))return false;if(isRenewed(t)&&isInRenewalGrace(t))return false;if(expireIsStale(t))return false;const d=_eod(t.expire);const diff=(d-now)/86400000;return diff>3&&diff<=7;});

  const hasExpired=expired.length>0;
  if(hasExpired||expiring3d.length){
    const el=document.createElement('div');el.id='field-alert-overlay';
    const bgColor=hasExpired?'rgba(220,38,38,.92)':'rgba(217,119,6,.88)';
    const list=[...expired.map(t=>({t,label:'⛔ VENCIDO',color:'#fff'})),...expiring3d.map(t=>({t,label:'⚠ '+Math.ceil((_eod(t.expire)-now)/86400000)+'d',color:'#fde68a'})),...expiring7d.slice(0,3).map(t=>({t,label:Math.ceil((_eod(t.expire)-now)/86400000)+'d',color:'#bfdbfe'}))];
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
  if(_sharedRefreshId){clearInterval(_sharedRefreshId);_sharedRefreshId=null;}
  document.getElementById('pg-shared').classList.remove('active');
  history.replaceState(null,'',window.location.pathname);
  const fb=document.getElementById('field-expiring-banner');if(fb)fb.remove();
  const fa=document.getElementById('field-alert-overlay');if(fa)fa.remove();
  document.getElementById('login-screen').classList.remove('hidden');
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
  document.getElementById('shared-list').innerHTML=f.length?f.map(t=>{
    const es=effectiveStatus(t);const inGrace=isRenewed(t)&&isInRenewalGrace(t);
    return`<div class="tcard s-${effStatusCls(t)}" data-id="${t.id}" onclick="shFocusTicket(${t.id})">`
    +`<div class="tcard-top"><span class="tcard-num">${esc(t.ticket)}${isRenewed(t)?' <span style="font-size:9px;color:#7c3aed">🔄</span>':''}</span><span class="sbadge b-${effStatusCls(t)}">${esc(effStatusLabel(t))}${inGrace?' 🔄':''}</span></div>`
    +`<div class="tcard-client">${esc(t.client)}${t.prime?' · '+esc(t.prime):''}</div>`
    +`<div class="tcard-meta"><span>${esc(t.location)}, ${esc(t.state)}</span><span>${fmtProgress(t)}</span>${t.tipo?`<span>${esc(t.tipo)}</span>`:''}</div>`
    +miniProgressBar(t)
    +(inGrace?(()=>{const os=t.statusOld||t.status_old||'Open';return os==='Clear'?`<div style="font-size:10px;color:#16a34a;font-weight:600;margin-top:2px">✅ Carência até ${graceCutoverDate(t)}</div>`:`<div style="font-size:10px;color:#b45309;font-weight:600;margin-top:2px">⚠ Carência (${esc(os)}) até ${graceCutoverDate(t)}</div>`;})():'')
    +`</div>`;
  }).join(''):'<div style="text-align:center;padding:28px;color:var(--muted);font-size:13px">Nenhum ticket</div>';
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
    const c=effColor(t),dash=tipoDash(t.tipo),lw=lineWeight(t.tipo);
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
  ['tm-t','tm-c','tm-co','tm-l','tm-st','tm-f','tm-cf','tm-notes','tm-tipo','tm-job','tm-prime','tm-addr'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('tm-s').value='Open';
  document.getElementById('tm-e').value='';
  const eo=document.getElementById('tm-expireOld');if(eo)eo.value='';
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
  document.getElementById('tm-cf').value=t.completedFeet||0;
  document.getElementById('tm-e').value=_toIsoDate(t.expire)||t.expire;
  document.getElementById('tm-notes').value=t.notes||'';
  document.getElementById('tm-tipo').value=t.tipo||'';
  document.getElementById('tm-job').value=t.job||'';
  document.getElementById('tm-prime').value=t.prime||'';
  document.getElementById('tm-addr').value=t.address||'';
  const eoEl=document.getElementById('tm-expireOld');if(eoEl)eoEl.value=_toIsoDate(t.expireOld)||t.expireOld||'';
  openModal('ov-ticket');
}
async function saveTicket(){
  const tnum=document.getElementById('tm-t').value.trim();
  if(!tnum){toast('Preencha o número.','danger');return;}
  // Valida data de expiração: se preenchida mas não parseável, avisa.
  const rawExpire=document.getElementById('tm-e').value.trim();
  const normalizedExpire=normalizeExpire(rawExpire);
  if(rawExpire && !normalizedExpire){
    toast('Data de expiração inválida. Use MM/DD/AAAA (ex: 05/13/2026).','danger');
    return;
  }
  // Campo expireOld editavel — pra ajustar data de vencimento do ticket ANTIGO
  // (snapshot do renovado). Importante pra recalcular grace period.
  const rawExpireOld=(document.getElementById('tm-expireOld')?.value||'').trim();
  const normalizedExpireOld=normalizeExpire(rawExpireOld);
  if(rawExpireOld && !normalizedExpireOld){
    toast('Data antigo invalida. Use MM/DD/AAAA.','danger');
    return;
  }
  const newStatus=document.getElementById('tm-s').value;
  let savedId=null;
  if(editingTicketId){
    const t=tickets.find(x=>x.id===editingTicketId);
    if(t){
      const old=t.status;
      const newProjId=document.getElementById('tm-proj').value;
      const projChanged=newProjId!==t.projectId;
      const oldExpireOld=t.expireOld||'';
      const oldCompFeet=t.completedFeet||0;
      const newCompFeet=Math.max(0,parseInt(document.getElementById('tm-cf').value)||0);
      Object.assign(t,{
        ticket:tnum,projectId:newProjId,
        client:document.getElementById('tm-c').value,company:document.getElementById('tm-co').value,
        location:document.getElementById('tm-l').value,state:document.getElementById('tm-st').value,
        footage:parseInt(document.getElementById('tm-f').value)||0,expire:normalizedExpire,
        expireOld:normalizedExpireOld,completedFeet:newCompFeet,
        notes:document.getElementById('tm-notes').value,status:newStatus,
        tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,
        prime:document.getElementById('tm-prime').value,address:document.getElementById('tm-addr').value
      });
      if(projChanged&&newProjId)t.project_locked=true;// trava ao trocar projeto manualmente
      t.history=t.history||[];// Fix bug #20: garante array antes dos pushes abaixo
      if(old!==newStatus)t.history.push({ts:Date.now(),action:`Status: ${old} → ${newStatus}`,color:scol(newStatus)});
      if(oldCompFeet!==newCompFeet)t.history.push({ts:Date.now(),action:`Progresso campo: ${oldCompFeet} → ${newCompFeet} ft`,color:'#16a34a'});
      if(oldExpireOld!==normalizedExpireOld){
        t.history.push({ts:Date.now(),action:`Expira (Antigo): ${oldExpireOld||'—'} → ${normalizedExpireOld||'—'}`,color:'#b45309'});
      }
      t.history.push({ts:Date.now(),action:'Editado',color:'#9a9888'});
      await saveTicketToDb(t);savedId=t.id;
    }
    toast('Ticket atualizado!','success');
  }else{
    const t={id:null,ticket:tnum,projectId:document.getElementById('tm-proj').value,company:document.getElementById('tm-co').value||'One Drill',state:document.getElementById('tm-st').value||'FL',location:document.getElementById('tm-l').value||'',status:newStatus,expire:normalizedExpire,footage:parseInt(document.getElementById('tm-f').value)||0,completedFeet:Math.max(0,parseInt(document.getElementById('tm-cf').value)||0),client:document.getElementById('tm-c').value||'—',prime:document.getElementById('tm-prime').value,tipo:document.getElementById('tm-tipo').value,job:document.getElementById('tm-job').value,address:document.getElementById('tm-addr').value,notes:document.getElementById('tm-notes').value,fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Ticket criado',color:'#1a6cf0'}],attachments:[],pending:'',oldTicket2:'',statusOld:'',expireOld:'',status_locked:false,project_locked:!!document.getElementById('tm-proj').value};
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
      const idx={ticket:ci('ticket'),company:ci('company'),state:ci('state'),location:ci('location'),status:ci('status'),expire:ci('expireon'),footage:ci('footage'),completedFeet:ci('completedfeet','completed','concluido','donefeet','campo'),client:ci('client'),prime:ci('prime'),job:ci('jobnumber'),tipo:ci('tipo'),address:ci('mainaddress'),project:ci('project'),pending:ci('pending'),oldTicket2:ci('oldticket'),statusOld:ci('statusold'),expireOld:ci('oldexpirationdate')};
      const getCell=(row,i)=>{if(i<0||i>=row.length)return'';const v=row[i];if(v===null||v===undefined)return'';if(v instanceof Date)return v.toLocaleDateString('en-US');return String(v).replace(/\xa0/g,'').trim();};
      parsed=dataRows.map(row=>{
        const ticket=getCell(row,idx.ticket);if(!ticket)return null;
        let rawStatus=getCell(row,idx.status);
        if(rawStatus.includes('✅')||rawStatus.includes('⚠'))rawStatus='Open';
        let status='Open';const sl=rawStatus.toLowerCase();
        if(sl==='clear')status='Clear';else if(sl==='open')status='Open';
        else if(sl==='closed'||sl==='close')status='Closed';else if(sl==='damage')status='Damage';
        else if(sl==='cancel')status='Cancel';else if(rawStatus)status=rawStatus;
        // Normaliza expire (formato do Excel pode vir como Date → "4/15/2026" sem zero) e expireOld
        const expire=normalizeExpire(getCell(row,idx.expire));
        const expireOld=normalizeExpire(getCell(row,idx.expireOld));
        return{ticket,company:getCell(row,idx.company)||'One Drill',state:getCell(row,idx.state),location:getCell(row,idx.location),status,expire,footage:parseFloat(getCell(row,idx.footage))||0,completedFeet:parseFloat(getCell(row,idx.completedFeet))||0,client:getCell(row,idx.client),prime:getCell(row,idx.prime),job:getCell(row,idx.job),tipo:getCell(row,idx.tipo),address:getCell(row,idx.address),projectName:getCell(row,idx.project),pending:getCell(row,idx.pending),oldTicket2:getCell(row,idx.oldTicket2),statusOld:getCell(row,idx.statusOld),expireOld};
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
  // Fix bug #6: modo "Substituir tudo" é IRREVERSÍVEL — apaga todos os tickets
  // (inclusive trajetos desenhados manualmente). Exige confirmação explícita
  // digitando o texto exato. Se cancelar, retorna ANTES de qualquer modificação.
  if(mode==='replace'){
    const confirmText=prompt('⚠️ ATENÇÃO — O modo "Substituir tudo" vai APAGAR TODOS os tickets do banco (inclusive trajetos desenhados). Esta ação é IRREVERSÍVEL.\n\nDigite APAGAR TUDO (em maiúsculas) para confirmar:');
    if(confirmText!=='APAGAR TUDO'){
      toast('Importação cancelada — nenhum dado foi alterado.','info');
      return;
    }
  }
  const pw=document.getElementById('progwrap'),pf=document.getElementById('progfill'),pt=document.getElementById('progtxt');
  pw.style.display='block';document.getElementById('bimport').disabled=true;setSyncStatus(true,'Importando...');

  // Save projects first
  // Fix bug #12: helper pra gerar ID de projeto de forma consistente.
  // Antes: 'p'+projId cru deixava passar espaços, quebras de linha, aspas no id,
  // causando colisões (dois projetos "ABC " e "ABC" geram o mesmo id 'pABC '
  // vs match sendo feito contra 'pABC'). Normaliza nome antes de prefixar.
  const _mkProjectId=(name)=>'p'+String(name||'').trim().replace(/\s+/g,' ');
  for(const[projId,ft]of Object.entries(parsedProjectTotals)){
    const pc=parsedProjectCoords[projId]||null;
    const normalizedName=String(projId||'').trim();
    const normalizedId=_mkProjectId(projId);
    let p=projects.find(x=>x.name===normalizedName||x.id===normalizedId);
    if(!p){p={id:normalizedId,name:normalizedName,client:'',state:'',status:'Active',desc:'',totalFeet:ft,centerCoords:pc,_manual:false};projects.push(p);}
    else{if(!p.totalFeet)p.totalFeet=ft;if(!p.centerCoords)p.centerCoords=pc;}
    await saveProjectToDb(p);
  }

  if(mode==='replace'){
    pt.textContent='Limpando tickets antigos...';
    await sb.from('tickets').delete().neq('id',0);
    tickets=[];projects=projects.filter(p=>p._manual||parsedProjectTotals.has(p.id));
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
      // Fix bug #12: usa helper _mkProjectId definido acima para lookup consistente
      const normalizedName=String(r.projectName).trim();
      const normalizedId=_mkProjectId(r.projectName);
      let p=projects.find(x=>x.name.toLowerCase()===normalizedName.toLowerCase()||x.id===normalizedId);
      if(!p){
        const tf=parsedProjectTotals[normalizedName]||0;
        const pc=parsedProjectCoords[normalizedName]||null;
        p={id:normalizedId,name:normalizedName,client:r.client,state:r.state,status:'Active',desc:'',_manual:false,totalFeet:tf,centerCoords:pc};
        projects.push(p);await saveProjectToDb(p);
      }else{
        if(!p.totalFeet)p.totalFeet=parsedProjectTotals[normalizedName]||0;
        if(!p.centerCoords)p.centerCoords=parsedProjectCoords[normalizedName]||null;
      }
      pid=p.id;
    }

    if(mode==='update'){
      const existing=tickets.find(t=>String(t.ticket).trim()===String(r.ticket).trim());
      if(existing){
        const oldStatus=existing.status;
        // Respeita locks: não sobrescreve projeto travado nem status travado
        const newPid=existing.project_locked?existing.projectId:(pid||existing.projectId);
        const newStatus=existing.status_locked?existing.status:r.status;
        Object.assign(existing,{company:r.company,state:r.state,location:r.location,status:newStatus,expire:r.expire,footage:r.footage,completedFeet:r.completedFeet||existing.completedFeet||0,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,projectId:newPid});
        existing.history=existing.history||[];// Fix audit: garante array antes de push
      if(oldStatus!==newStatus)existing.history.push({ts:Date.now(),action:`Status: ${oldStatus} → ${newStatus}`,color:scol(newStatus)});
        existing.history.push({ts:Date.now(),action:'Atualizado via Excel ✅'+(existing.project_locked?' (projeto travado 🔒)':''),color:'#16a34a'});
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

    const t={id:null,ticket:r.ticket,projectId:pid,company:r.company||'One Drill',state:r.state,location:r.location,status:r.status,expire:r.expire,footage:r.footage,completedFeet:r.completedFeet||0,client:r.client,prime:r.prime,job:r.job,tipo:r.tipo,address:r.address,pending:r.pending,oldTicket2:r.oldTicket2,statusOld:r.statusOld,expireOld:r.expireOld,notes:'',fieldPath:null,_geocoded:null,history:[{ts:Date.now(),action:'Importado via Excel',color:'#1a6cf0'}],attachments:[],status_locked:false,project_locked:false};
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
function exportProjectReport(pid){
  const p=projects.find(x=>x.id===pid);if(!p){toast('Projeto não encontrado','danger');return;}
  const ts=tickets.filter(t=>t.projectId===pid&&!isSuperseded(t));
  const totalFt=ts.reduce((s,t)=>s+(t.footage||0),0);
  const doneFt=ts.reduce((s,t)=>s+(t.completedFeet||0),0);
  const openC=ts.filter(t=>effectiveStatus(t)==='Open').length;
  const clearC=ts.filter(t=>effectiveStatus(t)==='Clear').length;
  const damageC=ts.filter(t=>effectiveStatus(t)==='Damage').length;
  const closedC=ts.filter(t=>t.status==='Closed').length;
  const pendCampoP=ts.filter(t=>{const ft=t.footage||0;const cf=t.completedFeet||0;return effectiveStatus(t)==='Clear'&&ft>0&&cf<ft;});
  const pendFtP=pendCampoP.reduce((s,t)=>s+((t.footage||0)-(t.completedFeet||0)),0);
  const pctCampo=totalFt>0?Math.round(doneFt/totalFt*100):0;
  const wb=XLSX.utils.book_new();
  // Sheet 1: Resumo
  const resumo=[
    ['RELATÓRIO DE PROJETO — OneDrill 811'],[''],
    ['Projeto:',p.name],['Cliente:',p.client||'—'],['Estado:',p.state||'—'],
    ['Data:',new Date().toLocaleDateString('pt-BR')],[''],
    ['RESUMO'],
    ['Total tickets:',ts.length],['Footage total:',totalFt],
    ['Campo concluído:',doneFt+' ft ('+pctCampo+'%)'],
    ['A realizar:',pendCampoP.length+' tickets · '+pendFtP.toLocaleString()+' ft'],[''],
    ['POR STATUS'],['Open:',openC],['Clear:',clearC],['Damage:',damageC],['Closed:',closedC]
  ];
  const wsR=XLSX.utils.aoa_to_sheet(resumo);
  wsR['!cols']=[{wch:22},{wch:40}];
  XLSX.utils.book_append_sheet(wb,wsR,'Resumo');
  // Sheet 2: Tickets
  const tData=[['Ticket #','Status','Footage','Concluído','Restante','% Campo','Local','Endereço','Expira','Tipo','Job #','Utilities Pendentes']];
  for(const t of ts){
    const pends=getTicketPendingUtils(String(t.ticket).trim()).map(u=>u.utility_name).join(', ');
    const ft=t.footage||0;const cf=t.completedFeet||0;const rest=ft-cf;
    const pct=ft>0?Math.round(cf/ft*100):0;
    tData.push([t.ticket,effectiveStatus(t),ft,cf,rest,pct+'%',t.location,t.address,t.expire,t.tipo,t.job,pends]);
  }
  tData.push(['','TOTAL:',totalFt,doneFt,totalFt-doneFt,pctCampo+'%','','','','','','']);
  const wsT=XLSX.utils.aoa_to_sheet(tData);
  wsT['!cols']=[{wch:14},{wch:8},{wch:9},{wch:12},{wch:10},{wch:9},{wch:22},{wch:22},{wch:12},{wch:24},{wch:10},{wch:30}];
  XLSX.utils.book_append_sheet(wb,wsT,'Tickets');
  // Sheet 3: A Realizar (só pendentes)
  if(pendCampoP.length){
    const pData=[['Ticket #','Footage','Concluído','Restante','% Campo','Local','Expira']];
    for(const t of pendCampoP){
      const ft=t.footage||0;const cf=t.completedFeet||0;
      pData.push([t.ticket,ft,cf,ft-cf,(ft>0?Math.round(cf/ft*100):0)+'%',t.location,t.expire]);
    }
    pData.push(['TOTAL:',pendCampoP.reduce((s,t)=>s+(t.footage||0),0),pendCampoP.reduce((s,t)=>s+(t.completedFeet||0),0),pendFtP,'','','']);
    const wsP=XLSX.utils.aoa_to_sheet(pData);
    wsP['!cols']=[{wch:14},{wch:9},{wch:12},{wch:10},{wch:9},{wch:22},{wch:12}];
    XLSX.utils.book_append_sheet(wb,wsP,'A Realizar');
  }
  XLSX.writeFile(wb,'OneDrill_Relatorio_'+p.name.replace(/[^a-zA-Z0-9]/g,'_')+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Relatório exportado: '+p.name,'success');
}

// Exporta SÓ tickets Clear pra trabalhar (todos estados/projetos).
// Replica a regra do relatorio_clear.py: status=Clear OU (carência ativa + status_old=Clear).
// Exclui vencidos próprios e supersedidos. Footage = total − completed.
function exportClearReport(){
  if(typeof XLSX==='undefined'){toast('XLSX não carregado','danger');return;}
  // Respeita o filtro de estado da tela de Projetos (se selecionado)
  const stateFilterEl=document.getElementById('proj-state-filter');
  const stateFilter=(stateFilterEl?.value||'').trim().toUpperCase();
  const now=new Date();
  const rows=[];
  for(const t of tickets){
    if(stateFilter&&(t.state||'').toUpperCase()!==stateFilter)continue;
    if(isSuperseded(t))continue;
    // Próprio expire vencido → exclui (a menos que esteja em carência ativa)
    const renewed=isRenewed(t);
    const grace=renewed&&isInRenewalGrace(t);
    if(!grace){
      const exp=t.expire;
      if(exp&&exp!=='—'&&_eod(exp)<now)continue;
    }
    const statusOld=(t.statusOld||t.status_old||'').trim();
    const locked=!!t.status_locked;
    let expShow=t.expire,obs='',ok=false;
    if(grace&&statusOld==='Clear'){
      ok=true;
      const oldNum=((t.oldTicket2||t.old_ticket2)||'').split('→')[0].trim();
      expShow=t.expireOld||t.expire_old||t.expire;
      obs=`carência do antigo ${oldNum}`+(locked?' (Clear manual)':'');
    }else if(t.status==='Clear'){
      ok=true;
      obs=locked?'Clear manual 🔒':'';
    }
    if(!ok)continue;
    const proj=projects.find(p=>p.id===t.projectId);
    const projName=proj?proj.name:'(Sem projeto)';
    const remFt=Math.max(0,(t.footage||0)-(t.completedFeet||0));
    rows.push({state:t.state||'?',project:projName,ticket:t.ticket,footage:remFt,
               expire:expShow||'—',obs,client:t.client||'',company:t.company||''});
  }
  // Ordena: estado → projeto → expire crescente
  rows.sort((a,b)=>{
    if(a.state!==b.state)return a.state.localeCompare(b.state);
    if(a.project!==b.project)return a.project.localeCompare(b.project);
    const da=_eod(a.expire)||new Date(9999,11,31);
    const db=_eod(b.expire)||new Date(9999,11,31);
    return da-db;
  });
  // Gera XLSX com estilos (xlsx-js-style)
  const wb=XLSX.utils.book_new();
  const headers=['Estado','Projeto','Ticket','Footage','Clear até','Observação','Cliente','Empresa'];
  const data=[headers];
  let totalFt=0;
  for(const r of rows){
    data.push([r.state,r.project,r.ticket,r.footage,r.expire,r.obs,r.client,r.company]);
    totalFt+=r.footage;
  }
  const totalRowIdx=data.length;
  data.push(['','','TOTAL',totalFt,'','','','']);
  const ws=XLSX.utils.aoa_to_sheet(data);
  ws['!cols']=[{wch:8},{wch:38},{wch:18},{wch:10},{wch:12},{wch:40},{wch:30},{wch:12}];
  if(rows.length)ws['!autofilter']={ref:`A1:H${totalRowIdx}`};
  ws['!freeze']={xSplit:0,ySplit:1};

  // Estilos (xlsx-js-style). Mesma paleta da versão Python.
  const STATE_FILL={FL:'DDEEFF',IL:'FFF2CC',IN:'FFE0B2',WI:'DCEDC8'};
  const border={style:'thin',color:{rgb:'CCCCCC'}};
  const borderAll={top:border,bottom:border,left:border,right:border};
  // Header (linha 1)
  for(let c=0;c<headers.length;c++){
    const ref=XLSX.utils.encode_cell({r:0,c});
    if(!ws[ref])continue;
    ws[ref].s={
      font:{bold:true,color:{rgb:'FFFFFF'},sz:11},
      fill:{patternType:'solid',fgColor:{rgb:'1F4E79'}},
      alignment:{horizontal:'center',vertical:'center'},
      border:borderAll
    };
  }
  ws['!rows']=[{hpt:22}];
  // Linhas de dados (1..totalRowIdx-1)
  for(let r=1;r<totalRowIdx;r++){
    const stateVal=(data[r][0]||'').toUpperCase();
    const fillRgb=STATE_FILL[stateVal];
    for(let c=0;c<headers.length;c++){
      const ref=XLSX.utils.encode_cell({r,c});
      if(!ws[ref])continue;
      const s={border:borderAll};
      if(fillRgb)s.fill={patternType:'solid',fgColor:{rgb:fillRgb}};
      if(c===3){s.numFmt='#,##0';s.alignment={horizontal:'right'};}
      else if(c===2||c===4)s.alignment={horizontal:'center'};
      ws[ref].s=s;
    }
  }
  // Linha TOTAL (negrito + fundo cinza)
  for(let c=0;c<headers.length;c++){
    const ref=XLSX.utils.encode_cell({r:totalRowIdx,c});
    if(!ws[ref])continue;
    ws[ref].s={
      font:{bold:true,sz:11},
      fill:{patternType:'solid',fgColor:{rgb:'E0E0E0'}},
      border:{top:{style:'medium',color:{rgb:'606060'}},bottom:borderAll.bottom,left:borderAll.left,right:borderAll.right},
      alignment:c===3?{horizontal:'right'}:{horizontal:'center'}
    };
    if(c===3)ws[ref].s.numFmt='#,##0';
  }
  XLSX.utils.book_append_sheet(wb,ws,'Tickets Clear');
  const ts=now.getFullYear()+String(now.getMonth()+1).padStart(2,'0')+String(now.getDate()).padStart(2,'0')+
           '_'+String(now.getHours()).padStart(2,'0')+String(now.getMinutes()).padStart(2,'0');
  const suffix=stateFilter?'_'+stateFilter:'';
  XLSX.writeFile(wb,`OneDrill_Clear_pra_Trabalhar${suffix}_${ts}.xlsx`);
  const filtTag=stateFilter?` (${stateFilter})`:'';
  toast(`${rows.length} tickets · ${totalFt.toLocaleString()} ft exportados${filtTag}`,'success');
}

// ═══════════ RELATÓRIO DE PENDÊNCIAS (tickets em aberto + utilities faltando) ═══════════
/** Classifica uma resposta Pending de utility. O LABEL descreve o motivo da pendência.
 *  Regra do Eric (2026-06): TODA pendência (tudo que não é Clear) precisa de follow-up
 *  → acao=true ("pra LIGAR"). Não mexe no classify() do status — só na ação do relatório. */
function categorizePending(u){
  const raw=(u.response_text||'').trim();
  const txt=(raw+' '+(u.status||'')).toLowerCase();
  // Frases que exigem AÇÃO (keyword-first — evita falso positivo de código em endereço)
  const KW=[
    ['already performed','3E — escavação já feita/cancelada'],
    ['could not gain access','Sem acesso ao local'],
    ['no access','Sem acesso ao local'],
    ['incorrect address','Endereço incorreto'],
    ['unclear','Instruções pouco claras'],
    ['partially marked','Parcialmente marcado'],
    ['extraordinary','Circunstâncias extraordinárias'],
    ['active facilities','Facilidades ativas (NÃO escavar)'],
    ['joint meet','Joint meet agendado'],
    ['do not excavate','NÃO ESCAVAR'],
    ['do not demolish','NÃO DEMOLIR'],
    ['working with excavator','Em andamento c/ escavador'],
    ['line untonable','Linha não detectável'],
    ['ongoing','Trabalho em andamento'],
    ['marking delay','Atraso na marcação'],
    ['not marked','Não marcado'],
    ['unmarked','Não marcado'],
    ['re-mark needed','Precisa remarcar'],
    ['remark needed','Precisa remarcar'],
  ];
  for(let i=0;i<KW.length;i++){if(txt.indexOf(KW[i][0])>=0)return{acao:true,label:KW[i][1]};}
  // Códigos isolados (3A/3B/3C/3D/3E/3F/3G/3T/6A/6B) com fronteira pra não pegar "Apt 3A" etc.
  const m=raw.toLowerCase().match(/(?:^|[^0-9a-z])([367][abcdefgt])(?:[^0-9a-z]|$)/);
  const code=m?m[1]:'';
  const CODE_LBL={'3a':'3A — sem acesso','3b':'3B — endereço incorreto','3c':'3C — atraso na marcação','3d':'3D — instruções pouco claras','3e':'3E — escavação já feita/cancelada','3f':'3F — linha não detectável','3g':'3G — parcialmente marcado','3t':'3T — circ. extraordinárias','6a':'6A — facilidades ativas','6b':'6B — joint meet'};
  if(code&&CODE_LBL[code])return{acao:true,label:CODE_LBL[code]};
  // Eric: tudo que não é Clear é pendência pra LIGAR (acao=true). Label = só o motivo.
  if(txt.indexOf('no response')>=0)return{acao:true,label:'Sem resposta ainda'};
  if(txt.indexOf('scheduled marking')>=0)return{acao:true,label:'Marcação agendada'};
  if(txt.indexOf('re-mark not needed')>=0||txt.indexOf('remark not needed')>=0)return{acao:true,label:'Re-mark not needed (ack)'};
  if(txt.indexOf('late')>=0)return{acao:true,label:'Resposta tardia'};
  if(!raw)return{acao:true,label:'Sem resposta ainda'};
  return{acao:true,label:'Pendente'};
}

/** Formata responded_at em formato americano MÊS/DIA/ANO (só a data, sem hora).
 *  Parse direto dos componentes — sem drift de fuso (não usa new Date()). */
function _fmtRespDate(v){
  if(!v)return'';
  const s=String(v).trim();
  if(!s||s==='—')return'';
  let m=s.match(/^(\d{4})-(\d{2})-(\d{2})/);          // ISO 2026-06-11... -> 06/11/2026
  if(m)return`${m[2]}/${m[3]}/${m[1]}`;
  m=s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})/);         // já em barras (US) -> mantém mês/dia/ano
  if(m)return`${m[1]}/${m[2]}/${m[3]}`;
  return s;
}

// Acha contatos (telefone/email) cadastrados que casam com a utility pendente.
// Match por token significativo do nome (DUKE, FRONTIER, KNOLOGY, UNITI, ZAYO, WATER...) + mesmo estado.
const _CONTACT_STOP=new Set(['ENERGY','POWER','ELECTRIC','FIBER','FIBRE','COMMUNICATIONS','COMMUNICATION','BROADBAND','GROUP','UTILITIES','UTILITY','COUNTY','COMM','FORMERLY','COMPANY','SERVICES','NETWORKS','NETWORK','LIGHTWAVE','SYSTEMS','TELECOM','CABLE','LOCATOR','LOCATORS','THE','AND','LLC','INC','DBA']);
function _utilTokens(s){
  return (s||'').toUpperCase().replace(/[^A-Z0-9 ]/g,' ').split(/\s+/).filter(w=>w.length>=3&&!_CONTACT_STOP.has(w));
}
function _utilContactsFor(utilityName, state){
  const ut=_utilTokens(utilityName);
  if(!ut.length)return[];
  const st=(state||'').toUpperCase();
  const seen=new Set(), out=[];
  for(const c of utilContacts){
    const cSt=(c.state||'').toUpperCase();
    if(st&&cSt&&cSt!==st)continue;            // estado diferente → ignora (contato sem estado entra)
    if(!_utilTokens(c.utility_name).some(t=>ut.includes(t)))continue;  // sem token em comum
    for(const ph of [c.phone_main,c.phone_alt,c.phone_emergency].map(x=>(x||'').trim()).filter(Boolean)){
      const key=ph.replace(/\D/g,'').slice(-10);
      if(key&&!seen.has(key)){seen.add(key);out.push({name:c.contact_name||c.utility_name,phone:ph,email:_contactEmail(c)});}
    }
  }
  return out;
}

/** Coleta tickets em aberto (effectiveStatus=Open) com suas utilities pendentes.
 *  stateFilter: '' = todos, ou 'FL'/'IL'/'IN'/'WI'. */
function _collectPendingRows(stateFilter){
  const out=[];
  for(const t of tickets){
    if(stateFilter&&(t.state||'').toUpperCase()!==stateFilter)continue;
    if(isSuperseded(t))continue;
    if(effectiveStatus(t)!=='Open')continue;   // só os efetivamente EM ABERTO
    const pend=getTicketPendingUtils(String(t.ticket||'').trim(),t);
    const utils=pend.map(u=>{
      const cat=categorizePending(u);
      return{name:u.utility_name||'?',resp:(u.response_text||'').trim(),at:_fmtRespDate(u.responded_at),acao:cat.acao,label:cat.label,contacts:_utilContactsFor(u.utility_name||'',t.state)};
    });
    // ação primeiro, depois alfabético
    utils.sort((a,b)=>((b.acao?1:0)-(a.acao?1:0))||a.name.localeCompare(b.name));
    const proj=projects.find(p=>p.id===t.projectId);
    out.push({
      state:t.state||'?',
      project:proj?proj.name:'(Sem projeto)',
      ticket:t.ticket,
      location:cleanLoc(t.location)||'',
      expire:t.expire||'—',
      footage:Math.max(0,(t.footage||0)-(t.completedFeet||0)),
      client:t.client||'',
      utils,
      actionCount:utils.filter(u=>u.acao).length,
      pendCount:utils.length
    });
  }
  // tickets que precisam de ação primeiro, depois estado/projeto/expira
  out.sort((a,b)=>{
    const aa=a.actionCount>0,bb=b.actionCount>0;
    if(aa!==bb)return(bb?1:0)-(aa?1:0);
    if(a.state!==b.state)return a.state.localeCompare(b.state);
    if(a.project!==b.project)return a.project.localeCompare(b.project);
    return(a.expire||'').localeCompare(b.expire||'');
  });
  return out;
}

function _prStat(n,lbl,color){
  return`<div style="text-align:center;padding:8px 14px;background:var(--bg);border:1px solid var(--border);border-radius:8px;min-width:90px"><div style="font-size:22px;font-weight:700;font-family:var(--mono);color:${color}">${n}</div><div style="font-size:10px;color:var(--muted)">${lbl}</div></div>`;
}

async function openPendingReport(){
  if(!utilCacheLoaded){try{await loadUtilCache();}catch(e){}}
  if(!utilContacts||!utilContacts.length){try{await loadContacts();}catch(e){}}
  const stateFilterEl=document.getElementById('proj-state-filter');
  const stateFilter=(stateFilterEl?.value||'').trim().toUpperCase();
  const rows=_collectPendingRows(stateFilter);
  const totalTickets=rows.length;
  const actionTickets=rows.filter(r=>r.actionCount>0).length;
  const filtTag=stateFilter?' · '+stateFilter:'';

  let ov=document.getElementById('ov-pending-report');
  if(!ov){
    ov=document.createElement('div');ov.id='ov-pending-report';ov.className='overlay';
    ov.innerHTML='<div class="modal" style="max-width:920px"><div class="modal-header"><h3 id="pr-title"></h3><button class="modal-close" onclick="closeModal(\'ov-pending-report\')">×</button></div><div id="pr-body" style="padding:16px;max-height:72vh;overflow-y:auto"></div></div>';
    document.body.appendChild(ov);
  }
  document.getElementById('pr-title').textContent='📞 Pendências — tickets em aberto'+filtTag;

  const totalPend=rows.reduce((s,r)=>s+r.pendCount,0);
  let html='<div style="display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin-bottom:14px">'
    +_prStat(totalTickets,'tickets em aberto','var(--text)')
    +_prStat(totalPend,'utilities p/ ligar','#dc2626')
    +'<button class="btn" onclick="exportPendingReport()" style="margin-left:auto;background:#7c2d12;color:white;border:none;padding:8px 16px;font-weight:700;border-radius:6px">📥 Baixar Excel</button>'
    +'</div>'
    +'<div style="font-size:11px;color:var(--muted);margin-bottom:10px">🔴 Toda utility que <b>não está Clear</b> é pendência <b>pra ligar</b>. O rótulo indica o motivo (sem resposta, tardia, não marcado, sem acesso…).</div>';

  if(!rows.length){
    html+='<div style="color:var(--muted);font-size:13px;text-align:center;padding:30px">Nenhum ticket em aberto'+(stateFilter?' em '+stateFilter:'')+'. 🎉</div>';
  }else{
    let curState='',curProj='';
    for(const r of rows){
      if(r.state!==curState){curState=r.state;curProj='';
        html+=`<div style="margin-top:14px;font-weight:700;font-size:13px;color:var(--text);border-bottom:2px solid var(--border);padding-bottom:4px">${esc(r.state)}</div>`;}
      if(r.project!==curProj){curProj=r.project;
        html+=`<div style="margin-top:8px;font-size:12px;font-weight:600;color:var(--muted)">📁 ${esc(r.project)}</div>`;}
      const chips=r.utils.length?r.utils.map(u=>{
        const bg=u.acao?'#fef2f2':'#f3f4f6',bd=u.acao?'#fca5a5':'#d1d5db',fg=u.acao?'#b91c1c':'#4b5563';
        const respTxt=u.resp?' — '+esc(u.resp):'';
        const atTxt=u.at?` <span style="opacity:.7">· 📅 ${esc(u.at)}</span>`:'';
        const telTxt=(u.contacts&&u.contacts.length)
          ? u.contacts.map(ct=>'<div style="margin-top:2px">'+(ct.name?'<b>'+esc(ct.name)+'</b> — ':'')+'<a href="tel:'+esc(ct.phone)+'" style="color:#1d4ed8;font-weight:600;text-decoration:none">☎ '+esc(ct.phone)+'</a>'+(ct.email?' <span style="color:#6b7280">✉ '+esc(ct.email)+'</span>':'')+'</div>').join('')
          : '<span style="opacity:.5;font-style:italic">sem telefone cadastrado</span>';
        return`<div style="background:${bg};border:1px solid ${bd};border-radius:6px;padding:5px 8px;font-size:11px;margin:3px 0">`
          +`<div>${u.acao?'📞':'⏳'} <b style="color:${fg}">${esc(u.name)}</b>: ${esc(u.label)}${respTxt}${atTxt}</div>`
          +`<div style="margin-top:3px">${telTxt}</div></div>`;
      }).join(''):'<span style="font-size:11px;color:var(--muted)">— sem dados de utilities (rode o sync)</span>';
      const flag=r.actionCount>0?`<span style="background:#dc2626;color:white;font-size:10px;font-weight:700;padding:2px 7px;border-radius:10px;margin-left:6px">${r.actionCount} p/ ligar</span>`:'';
      html+=`<div style="border:1px solid var(--border);border-radius:8px;padding:10px;margin:6px 0${r.actionCount>0?';border-left:4px solid #dc2626':''}">`
        +`<div style="display:flex;justify-content:space-between;align-items:baseline;flex-wrap:wrap;gap:6px"><div><span style="font-family:var(--mono);font-weight:700;font-size:13px;cursor:pointer;color:var(--accent)" onclick="(function(){var x=tickets.find(t=>String(t.ticket).trim()==='${esc(String(r.ticket).trim())}');if(x){closeModal('ov-pending-report');openTicketDetail(x.id);}})()">${esc(r.ticket)}</span> ${flag} <span style="font-size:11px;color:var(--muted)">${esc(r.location)}</span></div><div style="font-size:11px;color:var(--muted)">expira ${esc(r.expire)} · ${r.footage.toLocaleString()} ft</div></div>`
        +`<div style="margin-top:6px">${chips}</div></div>`;
    }
  }
  document.getElementById('pr-body').innerHTML=html;
  openModal('ov-pending-report');
}

function exportPendingReport(){
  if(typeof XLSX==='undefined'){toast('XLSX não carregado','danger');return;}
  const stateFilterEl=document.getElementById('proj-state-filter');
  const stateFilter=(stateFilterEl?.value||'').trim().toUpperCase();
  const rows=_collectPendingRows(stateFilter);
  const wb=XLSX.utils.book_new();
  const headers=['Estado','Projeto','Ticket','Expira','Footage','Utility','Resposta','Respondido em','Pendência','Ação?','Telefone(s)','Email'];
  const data=[headers];
  let nAcao=0;
  for(const r of rows){
    if(!r.utils.length){data.push([r.state,r.project,r.ticket,r.expire,r.footage,'—','sem dados','','Aguardando sync','','','']);continue;}
    for(const u of r.utils){
      const tels=(u.contacts||[]).map(c=>(c.name?c.name+': ':'')+c.phone).join('  |  ');
      const mails=[...new Set((u.contacts||[]).map(c=>c.email).filter(Boolean))].join(' / ');
      data.push([r.state,r.project,r.ticket,r.expire,r.footage,u.name,u.resp||'—',u.at||'—',u.label,u.acao?'LIGAR':'',tels,mails]);
      if(u.acao)nAcao++;
    }
  }
  const ws=XLSX.utils.aoa_to_sheet(data);
  ws['!cols']=[{wch:8},{wch:34},{wch:16},{wch:12},{wch:9},{wch:30},{wch:34},{wch:18},{wch:30},{wch:8},{wch:32},{wch:24}];
  if(rows.length)ws['!autofilter']={ref:`A1:L${data.length}`};
  ws['!freeze']={xSplit:0,ySplit:1};
  const STATE_FILL={FL:'DDEEFF',IL:'FFF2CC',IN:'FFE0B2',WI:'DCEDC8'};
  const border={style:'thin',color:{rgb:'CCCCCC'}};
  const borderAll={top:border,bottom:border,left:border,right:border};
  for(let c=0;c<headers.length;c++){
    const ref=XLSX.utils.encode_cell({r:0,c});if(!ws[ref])continue;
    ws[ref].s={font:{bold:true,color:{rgb:'FFFFFF'},sz:11},fill:{patternType:'solid',fgColor:{rgb:'7C2D12'}},alignment:{horizontal:'center',vertical:'center'},border:borderAll};
  }
  ws['!rows']=[{hpt:22}];
  for(let r=1;r<data.length;r++){
    const acao=data[r][9]==='LIGAR';
    const fillRgb=STATE_FILL[(data[r][0]||'').toUpperCase()];
    for(let c=0;c<headers.length;c++){
      const ref=XLSX.utils.encode_cell({r,c});if(!ws[ref])continue;
      const s={border:borderAll};
      if(fillRgb)s.fill={patternType:'solid',fgColor:{rgb:fillRgb}};
      if(c===4){s.numFmt='#,##0';s.alignment={horizontal:'right'};}
      else if(c===2||c===3||c===7)s.alignment={horizontal:'center'};
      if(acao&&(c===8||c===9)){s.font={bold:true,color:{rgb:'B91C1C'}};s.fill={patternType:'solid',fgColor:{rgb:'FEE2E2'}};if(c===9)s.alignment={horizontal:'center'};}
      ws[ref].s=s;
    }
  }
  XLSX.utils.book_append_sheet(wb,ws,'Pendencias');
  const now=new Date();
  const ts=now.getFullYear()+String(now.getMonth()+1).padStart(2,'0')+String(now.getDate()).padStart(2,'0')+
           '_'+String(now.getHours()).padStart(2,'0')+String(now.getMinutes()).padStart(2,'0');
  const suffix=stateFilter?'_'+stateFilter:'';
  XLSX.writeFile(wb,`OneDrill_Pendencias${suffix}_${ts}.xlsx`);
  toast(`${rows.length} tickets em aberto · ${nAcao} utilities p/ ligar`,'success');
}

function showNoProjModal(){
  const dsf=typeof dashStateVal!=='undefined'?dashStateVal:'';
  const fT=filterTickets({state:dsf});
  const list=fT.filter(t=>!t.projectId&&t.status!=='Cancel'&&t.status!=='Closed')
    .sort((a,b)=>(a.expire||'').localeCompare(b.expire||''));
  let ov=document.getElementById('ov-no-proj');
  if(!ov){
    ov=document.createElement('div');ov.id='ov-no-proj';ov.className='overlay';
    ov.innerHTML='<div class="modal" style="max-width:650px"><div class="modal-header"><h3 id="np-title"></h3><button class="modal-close" onclick="closeModal(\'ov-no-proj\')">×</button></div><div id="np-body" style="padding:16px"></div></div>';
    document.body.appendChild(ov);
  }
  document.getElementById('np-title').textContent='Tickets sem projeto ('+list.length+')';
  let rows='';
  for(const t of list){
    const loc=t.location||t.address||'';
    const st=t.state||'';
    const exp=t.expire||'—';
    const stat=t.status||'';
    const clr=stat==='Open'?'var(--red)':stat==='Clear'?'var(--green)':'var(--amber)';
    rows+='<tr style="cursor:pointer;border-bottom:1px solid var(--border)" onclick="closeModal(\'ov-no-proj\');openTicketDetail('+t.id+')">'
      +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px">'+esc(t.ticket)+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(st)+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(loc)+'</td>'
      +'<td style="padding:5px 8px;font-size:11px;color:'+clr+'">'+esc(stat)+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(exp)+'</td></tr>';
  }
  const tbl=list.length?'<div style="max-height:400px;overflow-y:auto"><table style="width:100%;font-size:11px;border-collapse:collapse">'
    +'<thead><tr style="text-align:left;color:var(--muted);font-size:10px;text-transform:uppercase;border-bottom:2px solid var(--border)"><th style="padding:5px 8px">Ticket</th><th style="padding:5px 8px">UF</th><th style="padding:5px 8px">Local</th><th style="padding:5px 8px">Status</th><th style="padding:5px 8px">Expira</th></tr></thead>'
    +'<tbody>'+rows+'</tbody></table></div>'
    :'<div style="text-align:center;color:var(--muted);padding:20px">Todos os tickets estão alocados em projetos ✓</div>';
  document.getElementById('np-body').innerHTML=tbl;
  openModal('ov-no-proj');
}

function exportExpiring(){
  const days=_soonDays||10;
  const f=filterTickets({}).filter(t=>{
    if(!t.expire||t.expire==='—')return false;
    if(expireIsStale(t))return false;
    const d=_eod(t.expire);const diff=(d-Date.now())/86400000;
    return diff>=0&&diff<=days&&t.status!=='Closed'&&t.status!=='Cancel';
  });
  if(!f.length){toast('Nenhum ticket vencendo.','warn');return;}
  const wb=XLSX.utils.book_new();
  const rows=[['Ticket #','Cliente','Prime','Estado','Local','Status','Footage','Concluído','Expira','Tipo','Endereço','Job #','Projeto','Utilities Pendentes','Old Ticket #','Status Old','Expire Old','Pending (Old)','Utilities Pendentes (Old)','Respostas (Old)']];
  for(const t of f){
    const proj=projects.find(p=>p.id===t.projectId)?.name||'';
    const pends=getTicketPendingUtils(String(t.ticket).trim()).map(u=>u.utility_name).join(', ');
    const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
    let oldPendNames='',oldAllResps='',oldPendCount='';
    if(oldNum){
      const oldPends=getTicketPendingUtils(oldNum);
      oldPendNames=oldPends.map(p=>p.utility_name).join(', ');
      oldPendCount=oldPends.length?String(oldPends.length):'0';
      oldAllResps=getTicketUtils(oldNum).map(u=>u.utility_name+' ('+u.status+')').join(', ');
    }
    rows.push([t.ticket,t.client,t.prime,t.state,t.location,t.status,t.footage,t.completedFeet||0,t.expire,t.tipo,t.address,t.job,proj,pends,t.oldTicket2||'',t.statusOld||'',t.expireOld||'',oldPendCount,oldPendNames,oldAllResps]);
  }
  const ws=XLSX.utils.aoa_to_sheet(rows);
  ws['!cols']=[{wch:14},{wch:20},{wch:16},{wch:7},{wch:20},{wch:8},{wch:9},{wch:12},{wch:12},{wch:24},{wch:10},{wch:20},{wch:30},{wch:30},{wch:16},{wch:10},{wch:12},{wch:10},{wch:30},{wch:40}];
  XLSX.utils.book_append_sheet(wb,ws,'Vencendo');
  XLSX.writeFile(wb,'OneDrill_Vencendo_'+days+'dias_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(f.length+' tickets exportados','success');
}

function exportFiltered(){
  const sr=(document.getElementById('tbl-srch')?.value||'').toLowerCase();
  const st=document.getElementById('tbl-stat')?.value||'';
  const sta=document.getElementById('tbl-state')?.value||'';
  const pr=document.getElementById('tbl-proj')?.value||'';
  const pm=document.getElementById('tbl-prime')?.value||'';
  const ut=document.getElementById('tbl-util')?.value||'';
  const ed=document.getElementById('tbl-exp')?.value||'';
  const f=filterTickets({status:st,projectId:pr,prime:pm,state:sta,search:sr,utility:ut,expireDays:ed,onlyGrace:_graceFilterActive});
  if(!f.length){toast('Nenhum ticket para exportar com esses filtros.','warn');return;}
  const totalFt=f.reduce((s,t)=>s+(t.footage||0),0);
  const wb=XLSX.utils.book_new();
  const _doneFtExp=f.reduce((s,t)=>s+(t.completedFeet||0),0);
  const tData=[
    ['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Concluído','A realizar','Expira','Tipo','Endereço','Job #','Pending','Utilities Pendentes','Empresa','Old Ticket #','Status Old','Expire Old','Pending (Old)','Utilities Pendentes (Old)','Respostas (Old)'],
    ...f.map(t=>{
      const pends=getTicketPendingUtils(String(t.ticket).trim());
      const pendNames=pends.map(p=>p.utility_name).join(', ');
      // ── Dados do ticket antigo (renovação) ──
      const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
      let oldPendNames='',oldAllResps='',oldPendCount='';
      if(oldNum){
        const oldPends=getTicketPendingUtils(oldNum);
        oldPendNames=oldPends.map(p=>p.utility_name).join(', ');
        oldPendCount=oldPends.length?String(oldPends.length):'0';
        const oldAll=getTicketUtils(oldNum);
        oldAllResps=oldAll.map(u=>u.utility_name+' ('+u.status+')').join(', ');
      }
      return [t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.completedFeet||0,Math.max(0,(t.footage||0)-(t.completedFeet||0)),t.expire,t.tipo,t.address,t.job,t.pending,pendNames,t.company,t.oldTicket2||'',t.statusOld||'',t.expireOld||'',oldPendCount,oldPendNames,oldAllResps];
    }),
    ['','','','','','','TOTAL:',totalFt,_doneFtExp,Math.max(0,totalFt-_doneFtExp),'','','','','','','','','','','','','']
  ];
  const ws=XLSX.utils.aoa_to_sheet(tData);
  ws['!cols']=[{wch:14},{wch:20},{wch:16},{wch:16},{wch:7},{wch:20},{wch:8},{wch:9},{wch:12},{wch:11},{wch:12},{wch:24},{wch:30},{wch:10},{wch:8},{wch:30},{wch:20},{wch:16},{wch:10},{wch:12},{wch:10},{wch:30},{wch:40}];
  // Tarja amarela na coluna "A realizar" (col 9): header, dados e total
  for(let r=0;r<tData.length;r++){
    const ref=XLSX.utils.encode_cell({r,c:9});
    if(!ws[ref])ws[ref]={t:'s',v:''};
    ws[ref].s={fill:{patternType:'solid',fgColor:{rgb:'FFEB9C'}},font:{bold:r===0||r===tData.length-1},alignment:{horizontal:r===0?'center':'right'}};
    if(r>0)ws[ref].s.numFmt='#,##0';
  }
  XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  XLSX.writeFile(wb,'OneDrill_Filtrado_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast('Excel filtrado: '+f.length+' tickets · '+totalFt.toLocaleString()+' ft','success');
}

// ════════════════════════════════════════════════════════════════════════════
// BULK UPDATE — Atualizar projeto/footage de múltiplos tickets via planilha
// ════════════════════════════════════════════════════════════════════════════
// Fluxo: usuário cola CSV/TSV (do Excel) com colunas (ticket, footage, projeto).
// 1) openBulkUpdate    → abre o modal e limpa estado
// 2) previewBulkUpdate → parse + valida + mostra tabela de diff
// 3) applyBulkUpdate   → salva via saveTicketBatch e atualiza UI
//
// Aceita projeto por NOME (exato, case-insensitive) ou por ID (id interno).
// Campos vazios não mudam. "-" / "sem projeto" / "none" remove do projeto.

let __bulkUpdatePlan=[];  // resultado do preview, usado pelo apply

/** Parse uma linha de planilha. Detecta TAB, vírgula ou ponto-e-vírgula. */
function _bulkSplitRow(line){
  if(line.includes('\t'))return line.split('\t').map(s=>s.trim());
  if(line.includes(';'))return line.split(';').map(s=>s.trim());
  return line.split(',').map(s=>s.trim());
}

/** Normaliza header pra comparar (lowercase, sem espaço/acento). */
function _bulkNk(s){return String(s||'').toLowerCase().replace(/[^a-z0-9]/g,'');}

/** Resolve nome/ID de projeto pra projectId real (ou string vazia se remover).
 *  Retorna {change, projectId, projectName, partial?, error?}.
 *  Se houver múltiplos matches parciais, marca erro de ambiguidade (mais seguro). */
function _bulkResolveProject(raw){
  const v=String(raw||'').trim();
  if(!v)return{change:false};
  const low=v.toLowerCase();
  if(low==='-'||low==='—'||low==='none'||low==='null'||low==='sem projeto'||low==='nenhum'){
    return{change:true,projectId:''};
  }
  // 1) ID exato
  let p=projects.find(x=>x.id===v);
  if(p)return{change:true,projectId:p.id,projectName:p.name};
  // 2) Nome exato case-insensitive — preferido, único
  p=projects.find(x=>(x.name||'').toLowerCase()===low);
  if(p)return{change:true,projectId:p.id,projectName:p.name};
  // 3) Match parcial — só aceita se for ÚNICO. Múltiplos = ambíguo (erro).
  const partials=projects.filter(x=>(x.name||'').toLowerCase().includes(low));
  if(partials.length===1){
    return{change:true,projectId:partials[0].id,projectName:partials[0].name,partial:true};
  }
  if(partials.length>1){
    const names=partials.slice(0,4).map(p=>p.name).join(' | ');
    const more=partials.length>4?` (+${partials.length-4})`:'';
    return{change:false,error:`ambíguo — ${partials.length} projetos casam: ${names}${more}`};
  }
  return{change:false,error:'projeto não encontrado'};
}

function openBulkUpdate(){
  document.getElementById('bu-input').value='';
  document.getElementById('bu-preview-area').style.display='none';
  document.getElementById('bu-apply-btn').disabled=true;
  __bulkUpdatePlan=[];
  openModal('ov-bulk-update');
}

function clearBulkUpdate(){
  document.getElementById('bu-input').value='';
  document.getElementById('bu-preview-area').style.display='none';
  document.getElementById('bu-apply-btn').disabled=true;
  __bulkUpdatePlan=[];
}

function previewBulkUpdate(){
  const raw=document.getElementById('bu-input').value.trim();
  if(!raw){toast('Cole o conteúdo da planilha primeiro.','warn');return;}

  const lines=raw.split(/\r?\n/).filter(l=>l.trim());
  if(!lines.length){toast('Planilha vazia.','warn');return;}

  // Detecta header: 1ª linha tem palavras-chave (ticket/footage/projeto/cliente/prime) → trata como header
  const firstCells=_bulkSplitRow(lines[0]).map(_bulkNk);
  const hasHeader=firstCells.some(c=>c==='ticket'||c==='footage'||c==='feet'||c==='ft'||c==='projeto'||c==='project'||c==='cliente'||c==='client'||c==='prime'||c==='concluido'||c==='completedfeet'||c==='completed'||c==='done');
  let idxTicket=-1, idxFootage=-1, idxProject=-1, idxClient=-1, idxPrime=-1, idxCompleted=-1;
  if(hasHeader){
    idxTicket=firstCells.findIndex(c=>c==='ticket'||c==='tickets'||c==='ticketnum'||c==='ticketnumber');
    idxFootage=firstCells.findIndex(c=>c==='footage'||c==='feet'||c==='ft'||c==='comprimento');
    idxCompleted=firstCells.findIndex(c=>c==='concluido'||c==='completedfeet'||c==='completedfeet'||c==='completed'||c==='done'||c==='donefeet'||c==='campo');
    idxProject=firstCells.findIndex(c=>c==='projeto'||c==='project'||c==='projectid'||c==='projectname');
    idxClient=firstCells.findIndex(c=>c==='cliente'||c==='client'||c==='clientname');
    idxPrime=firstCells.findIndex(c=>c==='prime'||c==='primecontractor');
    lines.shift();
  }else{
    // Sem header: assume ordem ticket, footage, projeto (cliente/prime/concluido requerem header)
    idxTicket=0; idxFootage=1; idxProject=2;
  }

  if(idxTicket<0){toast('Coluna "ticket" não encontrada no cabeçalho.','danger');return;}

  const plan=[];
  const errors=[];
  const noChange=[];// tickets encontrados mas todos os campos já estão corretos
  for(let li=0;li<lines.length;li++){
    const cells=_bulkSplitRow(lines[li]);
    const tnumRaw=(cells[idxTicket]||'').trim();
    if(!tnumRaw)continue;

    const t=tickets.find(x=>String(x.ticket).trim()===tnumRaw);
    if(!t){errors.push({ticket:tnumRaw,error:'ticket não encontrado'});continue;}

    const row={ticket:tnumRaw,t,changes:[]};

    // Footage
    if(idxFootage>=0){
      const ftRaw=(cells[idxFootage]||'').trim();
      if(ftRaw!==''){
        const ft=parseFloat(ftRaw.replace(/[,.](?=\d{3}\b)/g,'').replace(',','.'));
        if(!isNaN(ft)&&ft>=0){
          if(ft!==(t.footage||0)){
            row.changes.push({field:'footage',from:t.footage||0,to:ft});
            row.newFootage=ft;
          }
        }else{
          row.warn=(row.warn||'')+'footage inválido; ';
        }
      }
    }

    // Concluído (baixa parcial)
    if(idxCompleted>=0){
      const cfRaw=(cells[idxCompleted]||'').trim();
      if(cfRaw!==''){
        const cf=parseFloat(cfRaw.replace(/[,.](?=\d{3}\b)/g,'').replace(',','.'));
        if(!isNaN(cf)&&cf>=0){
          if(cf!==(t.completedFeet||0)){
            row.changes.push({field:'concluído',from:t.completedFeet||0,to:cf});
            row.newCompletedFeet=cf;
          }
        }else{
          row.warn=(row.warn||'')+'concluído inválido; ';
        }
      }
    }

    // Projeto
    if(idxProject>=0){
      const projRaw=(cells[idxProject]||'').trim();
      const resolved=_bulkResolveProject(projRaw);
      if(resolved.change){
        const fromName=projects.find(p=>p.id===t.projectId)?.name||'(sem projeto)';
        const toName=resolved.projectId?resolved.projectName:'(remover)';
        if(resolved.projectId!==t.projectId){
          row.changes.push({field:'projeto',from:fromName,to:toName,partial:resolved.partial});
          row.newProjectId=resolved.projectId;
        }
      }else if(projRaw&&resolved.error){
        row.warn=(row.warn||'')+resolved.error+'; ';
      }
    }

    // Cliente (string simples)
    if(idxClient>=0){
      const clRaw=(cells[idxClient]||'').trim();
      if(clRaw!==''){
        if(clRaw!==(t.client||'')){
          row.changes.push({field:'cliente',from:t.client||'(vazio)',to:clRaw});
          row.newClient=clRaw;
        }
      }
    }

    // Prime (string simples)
    if(idxPrime>=0){
      const prRaw=(cells[idxPrime]||'').trim();
      if(prRaw!==''){
        if(prRaw!==(t.prime||'')){
          row.changes.push({field:'prime',from:t.prime||'(vazio)',to:prRaw});
          row.newPrime=prRaw;
        }
      }
    }

    if(row.changes.length||row.warn){
      plan.push(row);
    }else{
      // Encontrado no banco mas tudo já está como pedido — vai pro resumo "já corretos"
      noChange.push(tnumRaw);
    }
  }

  __bulkUpdatePlan=plan;

  // Renderiza preview
  const area=document.getElementById('bu-preview-area');
  const tbl=document.getElementById('bu-preview-table');
  const lbl=document.getElementById('bu-preview-label');
  const willChange=plan.filter(r=>r.changes.length).length;
  const errCount=errors.length+plan.filter(r=>r.warn&&!r.changes.length).length;
  const totalProcessed=willChange+errCount+noChange.length;
  let summary=`<strong>${totalProcessed}</strong> linha(s) processada(s) — `
    +`<span style="color:var(--accent)"><strong>${willChange}</strong> atualizar</span>`;
  if(noChange.length)summary+=` · <span style="color:var(--green)">${noChange.length} já correto(s)</span>`;
  if(errCount)summary+=` · <span style="color:var(--red)">${errCount} com erro</span>`;
  lbl.innerHTML=summary;

  let html='<table style="width:100%;border-collapse:collapse;font-size:11px"><thead><tr style="background:var(--bg);position:sticky;top:0">'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Ticket</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Footage</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Concluído</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Projeto</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Cliente</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Prime</th>'
    +'<th style="padding:6px 8px;text-align:left;border-bottom:1px solid var(--border)">Aviso</th>'
    +'</tr></thead><tbody>';

  // Erros primeiro
  for(const e of errors){
    html+=`<tr style="background:var(--red-bg)"><td style="padding:5px 8px;font-family:var(--mono)">${esc(e.ticket)}</td><td>—</td><td>—</td><td>—</td><td>—</td><td>—</td><td style="padding:5px 8px;color:var(--red)">⚠ ${esc(e.error)}</td></tr>`;
  }
  for(const r of plan){
    const ftChange=r.changes.find(c=>c.field==='footage');
    const cfChange=r.changes.find(c=>c.field==='concluído');
    const prChange=r.changes.find(c=>c.field==='projeto');
    const clChange=r.changes.find(c=>c.field==='cliente');
    const piChange=r.changes.find(c=>c.field==='prime');
    const ftCell=ftChange?`<span style="color:var(--muted)">${(ftChange.from||0).toLocaleString()}</span> → <strong>${ftChange.to.toLocaleString()}</strong>`:'—';
    const cfCell=cfChange?`<span style="color:var(--muted)">${(cfChange.from||0).toLocaleString()}</span> → <strong style="color:#3b82f6">${cfChange.to.toLocaleString()}</strong>`:'—';
    const prCell=prChange?`<span style="color:var(--muted)">${esc(prChange.from)}</span> → <strong>${esc(prChange.to)}</strong>${prChange.partial?' <span style="color:var(--amber);font-size:10px">(match parcial)</span>':''}`:'—';
    const clCell=clChange?`<span style="color:var(--muted)">${esc(clChange.from)}</span> → <strong>${esc(clChange.to)}</strong>`:'—';
    const piCell=piChange?`<span style="color:var(--muted)">${esc(piChange.from)}</span> → <strong>${esc(piChange.to)}</strong>`:'—';
    const warn=r.warn?`<span style="color:var(--amber)">⚠ ${esc(r.warn)}</span>`:'';
    html+=`<tr style="border-bottom:1px solid var(--border)"><td style="padding:5px 8px;font-family:var(--mono)">${esc(r.ticket)}</td><td style="padding:5px 8px">${ftCell}</td><td style="padding:5px 8px">${cfCell}</td><td style="padding:5px 8px">${prCell}</td><td style="padding:5px 8px">${clCell}</td><td style="padding:5px 8px">${piCell}</td><td style="padding:5px 8px">${warn}</td></tr>`;
  }
  html+='</tbody></table>';

  // Lista de "já corretos" — mostrada como nota colapsável no rodapé
  if(noChange.length){
    html+=`<details style="margin-top:8px;padding:6px 10px;background:var(--green-bg);border:1px solid var(--green-border);border-radius:var(--r)">`
      +`<summary style="cursor:pointer;font-size:11px;color:var(--green);font-weight:600">✓ ${noChange.length} ticket(s) já estavam com os valores corretos (clica pra ver)</summary>`
      +`<div style="margin-top:6px;font-family:var(--mono);font-size:11px;color:var(--muted);line-height:1.6;word-break:break-all">${noChange.map(esc).join(', ')}</div>`
      +`</details>`;
  }

  if(!plan.length&&!errors.length&&!noChange.length){
    html='<div style="padding:20px;text-align:center;color:var(--muted)">Nenhuma linha válida detectada.</div>';
  }
  tbl.innerHTML=html;
  area.style.display='block';

  document.getElementById('bu-apply-btn').disabled=!willChange;
}

async function applyBulkUpdate(){
  const plan=__bulkUpdatePlan.filter(r=>r.changes.length);
  if(!plan.length){toast('Nada pra aplicar.','warn');return;}
  if(!await requireAuth())return;

  const btn=document.getElementById('bu-apply-btn');
  btn.disabled=true;
  btn.textContent='Aplicando... 0/'+plan.length;

  // ── ESTRATÉGIA: UPDATE direto via id (PK, sempre tem unique constraint) ──
  // O onConflict:'ticket' do saveTicketBatch falha porque a coluna `ticket` não
  // tem unique constraint no banco (erro 42P10). UPDATE WHERE id=X funciona sempre
  // porque `id` é PK. Roda em chunks de 10 paralelos pra balancear velocidade vs
  // rate limit do Supabase.

  let saved=0;
  const failed=[];
  const CONCURRENT=10;

  for(let i=0;i<plan.length;i+=CONCURRENT){
    const chunk=plan.slice(i,i+CONCURRENT);
    const promises=chunk.map(async r=>{
      if(!r.t.id){
        failed.push({ticket:r.ticket,error:'ticket sem id no banco'});
        return;
      }

      // Atualiza objeto em memória primeiro
      if(r.newFootage!==undefined)r.t.footage=r.newFootage;
      if(r.newCompletedFeet!==undefined&&_hasCompletedFeetCol)r.t.completedFeet=r.newCompletedFeet;
      if(r.newProjectId!==undefined){
        r.t.projectId=r.newProjectId;
        r.t.project_locked=!!r.newProjectId;// alocação manual sempre tranca
      }
      if(r.newClient!==undefined)r.t.client=r.newClient;
      if(r.newPrime!==undefined)r.t.prime=r.newPrime;
      if(!r.t.history)r.t.history=[];
      const changesStr=r.changes.map(c=>`${c.field}: ${c.from} → ${c.to}`).join(' · ');
      r.t.history.push({ts:Date.now(),action:'Atualização em massa: '+changesStr,color:'#7c3aed'});

      // Monta payload com APENAS os campos que mudaram (mais seguro que update completo)
      const upd={history:r.t.history};
      if(r.newFootage!==undefined)upd.footage=r.newFootage;
      if(r.newCompletedFeet!==undefined&&_hasCompletedFeetCol)upd.completed_feet=Math.max(0,parseInt(r.newCompletedFeet)||0);
      if(r.newProjectId!==undefined){
        upd.project_id=r.newProjectId||null;
        upd.project_locked=!!r.newProjectId;
      }
      if(r.newClient!==undefined)upd.client=r.newClient;
      if(r.newPrime!==undefined)upd.prime=r.newPrime;

      const{error}=await sb.from('tickets').update(upd).eq('id',r.t.id);
      if(error){
        failed.push({ticket:r.ticket,error:error.message});
        console.error(`[BulkUpdate] ${r.ticket}:`,error);
      }else{
        saved++;
      }
    });

    await Promise.all(promises);
    btn.textContent=`Aplicando... ${saved+failed.length}/${plan.length}`;
  }

  btn.textContent='Aplicar';

  if(failed.length===0){
    closeModal('ov-bulk-update');
    syncAll();
    toast(`✓ ${saved} ticket(s) atualizado(s)`,'success');
  }else if(saved>0){
    syncAll();
    toast(`Parcial: ${saved} salvos, ${failed.length} falharam (ver console)`,'warn');
    console.error('[BulkUpdate] Tickets que falharam:',failed);
    btn.disabled=false;
  }else{
    btn.disabled=false;
    const msg=failed[0]?.error||'erro desconhecido';
    toast(`Erro: ${msg.slice(0,80)}`,'danger');
    console.error('[BulkUpdate] Falhou em todos. Primeiro erro:',failed[0]);
  }
}

function exportExcel(){
  const wb=XLSX.utils.book_new();
  const tData=[['Ticket #','Projeto','Cliente','Prime','Estado','Local','Status','Footage','Concluído','Expira','Tipo','Endereço','Job #','Pending','Empresa','Old Ticket #','Expire Old'],...tickets.map(t=>[t.ticket,projects.find(p=>p.id===t.projectId)?.name||'',t.client,t.prime,t.state,t.location,t.status,t.footage,t.completedFeet||0,t.expire,t.tipo,t.address,t.job,t.pending,t.company,t.oldTicket2||'',t.expireOld||''])];
  const ws=XLSX.utils.aoa_to_sheet(tData);XLSX.utils.book_append_sheet(wb,ws,'Tickets');
  const pData=[['Nome','Cliente','Estado','Status','Total Feet','Tickets'],...projects.map(p=>[p.name,p.client,p.state,p.status,p.totalFeet,tickets.filter(t=>t.projectId===p.id).length])];
  const wp=XLSX.utils.aoa_to_sheet(pData);XLSX.utils.book_append_sheet(wb,wp,'Projetos');
  XLSX.writeFile(wb,`OneDrill_${new Date().toISOString().slice(0,10)}.xlsx`);
  toast('Excel exportado!','success');
}

function exportUtilTickets(utilName){
  const openTickets=filterTickets({}).filter(t=>t.status==='Open'||t.status==='Damage'||t.status==='Clear');
  // Fix bug #8: String(...).trim() consistente com o resto do código.
  // Sem isso, tickets vindos de Excel com espaços extras não batem com o cache (que é trimado).
  const tks=openTickets.filter(t=>{const pends=getTicketPendingUtils(String(t.ticket).trim());return pends.some(p=>p.utility_name===utilName);});
  if(!tks.length){toast('Nenhum ticket pendente para '+utilName,'warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[['Ticket #','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Projeto','Old Ticket #','Expire Old'],...tks.map(t=>[t.ticket,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,projects.find(p=>p.id===t.projectId)?.name||'',t.oldTicket2||'',t.expireOld||''])];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Pendentes');
  XLSX.writeFile(wb,'OneDrill_'+utilName.replace(/[^a-zA-Z0-9]/g,'_')+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(tks.length+' tickets exportados — '+utilName,'success');
}

function exportAllPending(){
  if(!utilCacheLoaded){toast('Aguarde carregar utilities','warn');return;}
  const openTickets=filterTickets({}).filter(t=>t.status==='Open'||t.status==='Damage'||t.status==='Clear');
  const rows=[];
  for(const t of openTickets){
    // Fix bug #8: String(...).trim() consistente (mesma razão do export individual acima)
    const pends=getTicketPendingUtils(String(t.ticket).trim());if(!pends.length)continue;
    for(const p of pends)rows.push([t.ticket,p.utility_name,t.client,t.prime,t.state,t.location,t.status,t.footage,t.expire,t.tipo,t.address,projects.find(pr=>pr.id===t.projectId)?.name||'',t.oldTicket2||'',t.expireOld||'']);
  }
  if(!rows.length){toast('Nenhuma pendência','warn');return;}
  const wb=XLSX.utils.book_new();
  const data=[['Ticket #','Utility Pendente','Cliente','Prime','Estado','Local','Status','Footage','Expira','Tipo','Endereço','Projeto','Old Ticket #','Expire Old'],...rows];
  const ws=XLSX.utils.aoa_to_sheet(data);XLSX.utils.book_append_sheet(wb,ws,'Todas Pendentes');
  XLSX.writeFile(wb,'OneDrill_Pendentes_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(rows.length+' pendências exportadas','success');
}

function exportPrivateLocator(){
  if(!utilCacheLoaded){toast('Aguarde carregar dados','warn');return;}
  const active=filterTickets({}).filter(t=>t.status!=='Closed'&&t.status!=='Cancel');
  const rows=[['Ticket','Status','Local','Estado','Utility','Resposta','Expira','Old Ticket #','Expire Old']];
  for(const t of active){
    const tkey=String(t.ticket).trim();
    const utils=getTicketUtils(tkey);
    for(const u of utils){
      const rt=(u.response_text||'').toLowerCase();
      if(rt.includes('3h')||rt.includes('privately owned')||rt.includes('private facility owner')){
        rows.push([t.ticket,t.status,t.location,t.state,u.utility_name,u.response_text||'',t.expire||'',t.oldTicket2||'',t.expireOld||'']);
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
    // Fix bug #7: (t.ticket||'') evita crash se ticket vier null. Adiciona job também (bug #22).
    // Adiciona match em oldTicket2 (chain de tickets antigos renovados) — permite buscar pelo numero
    // antigo e cair no ticket NOVO que carrega aquele historico. (issue: ticket renovado 20261906533)
    const oldChain=(t.oldTicket2||t.old_ticket2||'').toLowerCase();
    const matchOld=oldChain&&oldChain.includes(q);
    if((t.ticket||'').toLowerCase().includes(q)||(t.client||'').toLowerCase().includes(q)||(t.address||'').toLowerCase().includes(q)||(t.prime||'').toLowerCase().includes(q)||(t.job||'').toLowerCase().includes(q)||matchOld){
      const oldNum=((t.oldTicket2||t.old_ticket2)||'').split(' → ')[0].trim();
      // Quando matchou pelo numero antigo, sub mostra "renovou ANTIGO → NOVO" pra deixar claro
      const sub=matchOld
        ?('🔄 renovou '+oldNum+' · '+(t.client||'')+' · '+(t.location||'')+' · '+effectiveStatus(t))
        :((t.client||'')+' · '+(t.location||'')+' · '+effectiveStatus(t));
      results.push({type:'ticket',id:t.id,title:t.ticket,sub,status:effectiveStatus(t)});
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
      if(!t.expire||t.expire==='—'||isSuperseded(t))return false;
      if(t.status==='Closed'||t.status==='Cancel')return false;
      if(isRenewed(t)&&isInRenewalGrace(t))return false;
      if(expireIsStale(t))return false;
      const d=_eod(t.expire);const diff=(d-now)/864e5;return diff>=0&&diff<=5;
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
    // ── Feature 6: Alerta urgente — tickets vencendo com campo pendente ──
    _buildUrgencyAlert(expiring);
  }catch(e){console.error('Notifications error:',e);}
}

/** Floating urgency alert + browser notification pra tickets vencendo com campo pendente */
function _buildUrgencyAlert(expiring){
  // Remove alert anterior se existir
  const old=document.getElementById('urgency-float');if(old)old.remove();
  // Filtra: vence em ≤3 dias E campo não concluído
  const critical=expiring.filter(t=>{
    const d=_eod(t.expire);const diff=(d-Date.now())/864e5;
    if(diff>3)return false;
    const ft=t.footage||0;const cf=t.completedFeet||0;
    return ft>0&&cf<ft;
  });
  if(!critical.length)return;
  // Floating banner
  const div=document.createElement('div');div.id='urgency-float';
  div.style.cssText='position:fixed;top:8px;right:60px;z-index:9999;background:#fef2f2;border:2px solid #fca5a5;border-radius:12px;padding:8px 14px;box-shadow:0 4px 20px rgba(0,0,0,.15);max-width:360px;cursor:pointer;animation:urgPulse 2s infinite;font-size:12px;display:flex;align-items:center;gap:8px';
  div.innerHTML='<span style="font-size:18px">🚨</span><div><div style="font-weight:700;color:#dc2626">'+critical.length+' ticket'+(critical.length>1?'s':'')+' urgente'+(critical.length>1?'s':'')+'</div>'
    +'<div style="font-size:10px;color:#991b1b">Vence em ≤3 dias · campo pendente</div></div>'
    +'<span onclick="event.stopPropagation();this.parentElement.remove()" style="margin-left:auto;color:#dc2626;font-size:16px;padding:0 4px;cursor:pointer">✕</span>';
  div.onclick=function(){nav('tickets');setTimeout(function(){document.getElementById('tbl-stat').value='Open';document.getElementById('tbl-exp').value='3';renderTable();},100);div.remove();};
  document.body.appendChild(div);
  // Injeta animation CSS se não existe
  if(!document.getElementById('urgency-css')){
    const st=document.createElement('style');st.id='urgency-css';
    st.textContent='@keyframes urgPulse{0%,100%{box-shadow:0 4px 20px rgba(220,38,38,.15)}50%{box-shadow:0 4px 20px rgba(220,38,38,.4)}}';
    document.head.appendChild(st);
  }
  // Browser Notification API
  _sendBrowserNotif(critical);
}

function _sendBrowserNotif(critical){
  if(!('Notification' in window))return;
  if(Notification.permission==='denied')return;
  const msg=critical.length+' ticket'+(critical.length>1?'s':'')+' vencendo em 3 dias com campo pendente!\n'+critical.slice(0,3).map(function(t){return t.ticket+' ('+t.expire+')';}).join(', ');
  if(Notification.permission==='granted'){
    try{new Notification('🚨 OneDrill 811 — Atenção',{body:msg,tag:'onedrill-urgency'});}catch(e){}
  }else{
    Notification.requestPermission().then(function(p){
      if(p==='granted'){try{new Notification('🚨 OneDrill 811 — Atenção',{body:msg,tag:'onedrill-urgency'});}catch(e){}}
    });
  }
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
        +'<span class="sbadge b-'+effStatusCls(t)+'" style="font-size:9px">'+esc(effStatusLabel(t))+'</span>'
        +'<div style="font-size:10px;color:var(--muted);margin-top:1px">'+esc(last?.action||'—')+'</div></div>';
    }
    h+='<div class="notif-section" style="margin-top:12px">ℹ️ Sistema</div><div style="font-size:12px;color:var(--text2);padding:6px 10px;line-height:1.8">🔵 Dados: Supabase<br>🟢 Sync: Automática<br>🗺 Mapa: Google Hybrid</div>';
    panel.innerHTML=h;
  }
}

/* ═══════════ 26. ANALYTICS ═══════════ */
function renderWeeklyProduction(fTickets){
  var now=Date.now();var _wks=[];
  for(var w=5;w>=0;w--){
    var wStart=now-(w+1)*7*864e5;var wEnd=now-w*864e5*7;
    var wCnt=0,wFt=0;
    for(var i=0;i<fTickets.length;i++){
      var t=fTickets[i];if(t.status!=='Clear'||!t.history)continue;
      for(var j=t.history.length-1;j>=0;j--){
        var a=(t.history[j].action||'').toLowerCase();
        var isC=a.indexOf('auto-clear')>=0||(a.indexOf('auto 811')>=0&&a.indexOf('revertido')<0&&a.indexOf('cancelado')<0)||(a.indexOf('status manual')>=0&&a.indexOf('→ clear')>=0);
        if(isC&&t.history[j].ts>=wStart&&t.history[j].ts<wEnd){wCnt++;wFt+=(t.footage||0);break;}
      }
    }
    var d2=new Date(wEnd);
    _wks.push({label:String(d2.getDate()).padStart(2,'0')+'/'+String(d2.getMonth()+1).padStart(2,'0'),cnt:wCnt,ft:wFt});
  }
  var _wkMax=Math.max.apply(null,_wks.map(function(w2){return w2.ft;}))||1;
  var totalWkFt=_wks.reduce(function(s,w2){return s+w2.ft;},0);
  var avgWk=Math.round(totalWkFt/_wks.length);
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">'
  +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
  +'<div class="dash-card-title" style="margin-bottom:0">📊 Produção semanal</div>'
  +'<span style="font-size:10px;color:var(--muted);font-family:var(--mono)">média '+avgWk.toLocaleString()+' ft/sem</span></div>'
  +'<div style="display:flex;align-items:flex-end;gap:8px;height:120px">'
  +_wks.map(function(w2){
    var bh=Math.max(w2.ft/_wkMax*80,3);
    return'<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px">'
    +'<div style="font-size:10px;font-weight:700;color:var(--green)">'+w2.cnt+'</div>'
    +'<div style="font-size:9px;color:var(--muted)">'+w2.ft.toLocaleString()+' ft</div>'
    +'<div style="width:100%;height:'+bh+'px;background:var(--green);border-radius:3px 3px 0 0;min-height:3px;transition:height .3s"></div>'
    +'<div style="font-size:9px;color:var(--muted)">'+w2.label+'</div></div>';
  }).join('')
  +'</div></div></div>';
}

function renderAnalytics(){
  const el=document.getElementById('analytics-content');if(!el)return;
  const states=[...new Set(tickets.map(t=>t.state).filter(Boolean))].sort();
  const dsf=dashStateVal;
  const scope=_analyticsScope||'all';
  const sf='<select class="fi" onchange="dashStateVal=this.value;renderAnalytics()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px"><option value="">Todos estados</option>'+states.map(s=>'<option value="'+esc(s)+'"'+(dsf===s?' selected':'')+'>'+esc(s)+'</option>').join('')+'</select>';
  const scopeSel='<select class="fi" onchange="_analyticsScope=this.value;renderAnalytics()" style="width:auto;min-width:120px;font-size:12px;padding:5px 8px">'
    +'<option value="all"'+(scope==='all'?' selected':'')+'>📊 Todos projetos</option>'
    +'<option value="active"'+(scope==='active'?' selected':'')+'>🟢 Só ativos</option>'
    +'<option value="completed"'+(scope==='completed'?' selected':'')+'>📁 Só concluídos</option>'
    +'</select>';
  const fT=filterTickets({state:dsf,excludeCompleted:false});
  const allProjs=dsf?projects.filter(p=>p.state===dsf):projects;
  const fP=scope==='active'?allProjs.filter(p=>p.status!=='Completed'):scope==='completed'?allProjs.filter(p=>p.status==='Completed'):allProjs;
  const now=Date.now();const week=7*86400000;
  const ps=fP.map(p=>{
    const ts=fT.filter(t=>t.projectId===p.id);
    const cf=ts.filter(t=>t.status==='Clear').reduce((s,t)=>s+(t.footage||0),0);
    const of2=ts.filter(t=>t.status==='Open').reduce((s,t)=>s+(t.footage||0),0);
    const con=ts.filter(t=>t.status==='Closed').reduce((s,t)=>s+(t.footage||0),0);
    const df=ts.filter(t=>t.status==='Damage').reduce((s,t)=>s+(t.footage||0),0);
    const tf=ts.reduce((s,t)=>s+(t.footage||0),0);const tot=p.totalFeet||tf||1;
    const allLocsX=[...new Set(ts.map(t=>t.location).filter(Boolean).map(l=>cleanLoc(l)))];
    const filtLocsX=allLocsX.filter(l=>l.toUpperCase()!==((p.state||'').toUpperCase()));
    const locs=(filtLocsX.length?filtLocsX:allLocsX).join(', ')||'';
    const c4w_bins=[0,0,0,0];
    for(const t2 of ts){
      if(!t2.history)continue;
      const clearEvt=t2.history.filter(h2=>{const a2=(h2.action||'').toLowerCase();return(a2.includes('auto 811')&&!a2.includes('revertido')&&!a2.includes('cancelado'))||a2.includes('auto-clear')||(a2.includes('status manual')&&a2.includes('→ clear'));}).pop();
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
    '<div class="page-title">Analytics <span style="font-size:13px;font-weight:400;color:var(--muted);font-family:var(--mono)">'+new Date().toLocaleDateString('pt-BR')+'</span>'+syncPill+'<span style="margin-left:auto;display:flex;gap:6px">'+scopeSel+sf+'</span></div>'
    +renderClearedStats(fT)
    +renderWeeklyProduction(fT)
    +renderWeeklyEvolution(fT)
    +renderProgressoFootage(fT,ps)
    +renderRiskAnalytics(fT)
    +renderVelocity(fT,ps)
    +renderClearTimeMetrics(fT)
    +renderUtilSummaryHtml()
    +renderRecentActivity(fT)
    +renderSyncHealthCard()
    +renderDamageAnalytics(fT,fP);
  loadLastSync();
}

// ═══════════ DAMAGE ANALYTICS (Fase 3 do refactor Damage) ═══════════
// 6 métricas pedidas pelo usuário:
//   1. Total empresa (soma de damage_count)
//   2. Por estado
//   3. Por projeto
//   4. % empresa por footage (footage de tickets com damage / footage total)
//   5. % por estado por footage
//   6. % por projeto por footage
//
// Conta damages de TODOS os tickets (inclui Closed/Cancel) porque damage é histórico.
function renderDamageAnalytics(fT,fP){
  // 1. Total empresa — soma de damage_count, considera TODOS tickets do scope (não respeita filtro de state/scope pra métrica empresa)
  // Mas pra coerência com os outros analytics que usam fT, vou usar fT (que já respeita o dropdown de estado).
  // O label deixa claro: "Empresa" significa "todos os tickets do escopo atual".
  const allT=fT;// tickets do escopo atual (inclui Closed/Cancel — damage é histórico)
  const withDmg=allT.filter(t=>(parseInt(t.damageCount)||0)>0);
  const totalDmg=allT.reduce((s,t)=>s+(parseInt(t.damageCount)||0),0);
  const ticketsWithDmgCount=withDmg.length;
  const allFootage=allT.reduce((s,t)=>s+(t.footage||0),0);
  const footageWithDmg=withDmg.reduce((s,t)=>s+(t.footage||0),0);
  const pctAllFt=allFootage>0?(footageWithDmg/allFootage*100):0;

  // 2. Por estado
  const byState={};
  for(const t of allT){
    const st=t.state||'—';
    if(!byState[st])byState[st]={damageCount:0,tickets:0,ticketsWithDmg:0,footage:0,footageWithDmg:0};
    byState[st].tickets++;
    byState[st].footage+=(t.footage||0);
    const dc=parseInt(t.damageCount)||0;
    if(dc>0){
      byState[st].damageCount+=dc;
      byState[st].ticketsWithDmg++;
      byState[st].footageWithDmg+=(t.footage||0);
    }
  }
  // 3. Por projeto
  const byProj={};
  for(const t of allT){
    const pid=t.projectId||'';
    if(!pid)continue;// skip tickets sem projeto
    if(!byProj[pid])byProj[pid]={damageCount:0,tickets:0,ticketsWithDmg:0,footage:0,footageWithDmg:0};
    byProj[pid].tickets++;
    byProj[pid].footage+=(t.footage||0);
    const dc=parseInt(t.damageCount)||0;
    if(dc>0){
      byProj[pid].damageCount+=dc;
      byProj[pid].ticketsWithDmg++;
      byProj[pid].footageWithDmg+=(t.footage||0);
    }
  }

  // Cards do topo
  const cardsHtml='<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:14px">'
    +'<div style="padding:14px;background:var(--amber-bg);border:1px solid var(--amber-border);border-radius:var(--r);text-align:center">'
    +'<div style="font-size:26px;font-weight:700;font-family:var(--mono);color:var(--amber)">'+totalDmg+'</div>'
    +'<div style="font-size:11px;color:var(--amber);text-transform:uppercase;letter-spacing:.03em">Total de danos</div>'
    +'<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:3px">em '+ticketsWithDmgCount+' ticket(s)</div>'
    +'</div>'
    +'<div style="padding:14px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);text-align:center">'
    +'<div style="font-size:26px;font-weight:700;font-family:var(--mono);color:var(--text)">'+footageWithDmg.toLocaleString()+' ft</div>'
    +'<div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.03em">Footage afetado</div>'
    +'<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:3px">de '+allFootage.toLocaleString()+' ft total</div>'
    +'</div>'
    +'<div style="padding:14px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);text-align:center">'
    +'<div style="font-size:26px;font-weight:700;font-family:var(--mono);color:var(--text)">'+pctAllFt.toFixed(2)+'%</div>'
    +'<div style="font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:.03em">% pelo footage</div>'
    +'<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:3px">footage com dano ÷ total</div>'
    +'</div>'
    +'</div>';

  // Tabela por estado
  const stateRows=Object.entries(byState)
    .filter(([_,d])=>d.damageCount>0)
    .sort((a,b)=>b[1].damageCount-a[1].damageCount);
  const stateTable=stateRows.length?'<div style="margin-bottom:14px">'
    +'<div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">Por estado</div>'
    +'<table style="width:100%;border-collapse:collapse;font-size:12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);overflow:hidden">'
    +'<thead><tr style="background:var(--bg-alt,var(--bg));border-bottom:2px solid var(--border)">'
    +'<th style="padding:8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Estado</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Danos</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Tickets c/ dano</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage afetado</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage total</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">% ft</th>'
    +'</tr></thead><tbody>'
    +stateRows.map(([st,d])=>{
      const pct=d.footage>0?(d.footageWithDmg/d.footage*100):0;
      return'<tr style="border-bottom:1px solid var(--border)">'
        +'<td style="padding:6px 8px;font-weight:700">'+esc(st)+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--amber);font-weight:700">'+d.damageCount+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono)">'+d.ticketsWithDmg+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono)">'+d.footageWithDmg.toLocaleString()+' ft</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--muted)">'+d.footage.toLocaleString()+' ft</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--amber);font-weight:700">'+pct.toFixed(2)+'%</td>'
        +'</tr>';
    }).join('')
    +'</tbody></table></div>':'';

  // Tabela por projeto
  const projRows=Object.entries(byProj)
    .filter(([_,d])=>d.damageCount>0)
    .map(([pid,d])=>{
      const p=projects.find(pp=>pp.id===pid);
      return{name:p?p.name:'(projeto removido)',client:p?p.client:'',state:p?p.state:'',data:d};
    })
    .sort((a,b)=>b.data.damageCount-a.data.damageCount);
  const projTable=projRows.length?'<div>'
    +'<div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">Por projeto</div>'
    +'<div style="overflow-x:auto;max-height:400px;overflow-y:auto;background:var(--bg);border:1px solid var(--border);border-radius:var(--r)">'
    +'<table style="width:100%;border-collapse:collapse;font-size:12px">'
    +'<thead style="position:sticky;top:0;background:var(--bg);z-index:1">'
    +'<tr style="border-bottom:2px solid var(--border)">'
    +'<th style="padding:8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Projeto</th>'
    +'<th style="padding:8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Estado</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Danos</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Tickets c/ dano</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage afetado</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage total</th>'
    +'<th style="padding:8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">% ft</th>'
    +'</tr></thead><tbody>'
    +projRows.map(r=>{
      const d=r.data;
      const pct=d.footage>0?(d.footageWithDmg/d.footage*100):0;
      return'<tr style="border-bottom:1px solid var(--border)">'
        +'<td style="padding:6px 8px;font-weight:600">'+esc(r.name)+(r.client?'<span style="font-size:10px;color:var(--muted);margin-left:6px">'+esc(r.client)+'</span>':'')+'</td>'
        +'<td style="padding:6px 8px;font-size:11px;color:var(--muted)">'+esc(r.state||'—')+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--amber);font-weight:700">'+d.damageCount+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono)">'+d.ticketsWithDmg+'</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono)">'+d.footageWithDmg.toLocaleString()+' ft</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--muted)">'+d.footage.toLocaleString()+' ft</td>'
        +'<td style="padding:6px 8px;text-align:right;font-family:var(--mono);color:var(--amber);font-weight:700">'+pct.toFixed(2)+'%</td>'
        +'</tr>';
    }).join('')
    +'</tbody></table></div></div>':'';

  // Estado vazio — se nada de damage foi registrado ainda
  if(totalDmg===0){
    return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">'
      +'<div class="dash-card-title" style="margin-bottom:10px">⚠ Damages</div>'
      +'<div style="padding:20px;text-align:center;color:var(--muted);font-size:13px">Nenhum dano registrado no escopo atual.<br><span style="font-size:11px">Registre danos pelo botão "⚠ Registrar dano" no modal de cada ticket.</span></div>'
      +'</div></div>';
  }

  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1">'
    +'<div class="dash-card-title" style="margin-bottom:10px">⚠ Damages <span style="font-size:12px;font-weight:400;color:var(--muted);margin-left:6px">(inclui tickets Closed/Cancel)</span></div>'
    +cardsHtml
    +stateTable
    +projTable
    +'</div></div>';
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
  // "Hoje" = meia-noite local (não 24h rolling)
  var todayMidnight=new Date();todayMidnight.setHours(0,0,0,0);var todayCutoff=todayMidnight.getTime();
  var cpf=_clearProjFilter||'';
  var ft2=cpf?fTickets.filter(function(t){return t.projectId===cpf;}):fTickets;
  function getTicketClearDate(t){
    if(!t.history||!t.history.length)return 0;
    for(var j=t.history.length-1;j>=0;j--){
      var a=(t.history[j].action||'').toLowerCase();
      // Reconhece: auto-clear, auto 811 (nao revertido, nao cancelado), ou clear manual
      var isAutoClear=a.indexOf('auto-clear')>=0;
      var isAuto811=a.indexOf('auto 811')>=0&&a.indexOf('revertido')<0&&a.indexOf('cancelado')<0;
      var isManualClear=a.indexOf('status manual')>=0&&a.indexOf('→ clear')>=0;
      if(isAutoClear||isAuto811||isManualClear){return t.history[j].ts;}
    }
    return 0;
  }
  /** Retorna YYYY-MM-DD em fuso LOCAL (não UTC) */
  function localDateKey(ts){
    var d=new Date(ts);
    return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
  }
  var cToday=[],c24=[],c7=[],c30=[],byU7={};
  var seenToday={},seen24={},seen7={},seen30={};
  for(var i=0;i<ft2.length;i++){
    var t=ft2[i];if(t.status!=='Clear')continue;
    var cd=getTicketClearDate(t);if(!cd)continue;
    var tk=t.ticket;
    if(cd>=todayCutoff&&!seenToday[tk]){cToday.push(t);seenToday[tk]=1;}
    if(cd>=day1&&!seen24[tk]){c24.push(t);seen24[tk]=1;}
    if(cd>=day7&&!seen7[tk]){c7.push(t);seen7[tk]=1;}
    if(cd>=day30&&!seen30[tk]){c30.push(t);seen30[tk]=1;}
  }
  if(utilCacheLoaded){for(var i2=0;i2<c7.length;i2++){var us=getTicketUtils(String(c7[i2].ticket).trim());for(var j=0;j<us.length;j++){if(us[j].status==='Clear'){if(!byU7[us[j].utility_name])byU7[us[j].utility_name]=0;byU7[us[j].utility_name]++;}}}}
  var ftToday=0,ft24=0,ft7=0,ft30=0;
  for(var iT=0;iT<cToday.length;iT++)ftToday+=(cToday[iT].footage||0);
  for(var i3=0;i3<c24.length;i3++)ft24+=(c24[i3].footage||0);
  for(var i4=0;i4<c7.length;i4++)ft7+=(c7[i4].footage||0);
  for(var i5=0;i5<c30.length;i5++)ft30+=(c30[i5].footage||0);

  var projOpts='<option value="">Todos projetos</option>'+projects.filter(function(p2){return p2.status!=='Completed';}).map(function(p2){return'<option value="'+p2.id+'"'+(cpf===p2.id?' selected':'')+'>'+esc(projDropLabel(p2))+'</option>';}).join('');
  var projSel='<select class="fi" onchange="_clearProjFilter=this.value;refreshDashOrAnalytics()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';

  var topUtils=Object.entries(byU7).sort(function(a,b){return b[1]-a[1];}).slice(0,12);

  // ── Bar chart: clareados por dia (últimos 7 dias) — FUSO LOCAL ──
  var dayBuckets={};
  var dayLabels=[];
  var dayNow=new Date();
  for(var d=6;d>=0;d--){
    var dd=new Date(dayNow);dd.setDate(dd.getDate()-d);
    var dk=localDateKey(dd.getTime());
    dayBuckets[dk]=0;
    var dias=['dom','seg','ter','qua','qui','sex','sáb'];
    dayLabels.push({key:dk,label:dias[dd.getDay()]+', '+String(dd.getDate()).padStart(2,'0')});
  }
  for(var ib=0;ib<c7.length;ib++){
    var cdt=getTicketClearDate(c7[ib]);
    if(!cdt)continue;
    var cdd=localDateKey(cdt);
    if(dayBuckets[cdd]!==undefined)dayBuckets[cdd]++;
  }
  var maxBar=Math.max.apply(null,dayLabels.map(function(d2){return dayBuckets[d2.key]||0;}))||1;
  var barHtml='<div style="display:flex;align-items:flex-end;gap:6px;height:120px;padding-top:20px">';
  for(var ib2=0;ib2<dayLabels.length;ib2++){
    var dLbl=dayLabels[ib2];
    var dv=dayBuckets[dLbl.key]||0;
    var bh=Math.max(dv/maxBar*100,2);
    // Só dias com >0 tickets são clicáveis (não faz sentido expandir vazio)
    var isClickable=dv>0;
    var isActive=_clearedExpandDay===dLbl.key;
    var clickAttr=isClickable?' onclick="toggleClearedDay(\''+dLbl.key+'\')" style="cursor:pointer;padding:2px;border-radius:4px;'+(isActive?'background:var(--green-bg);box-shadow:0 0 0 2px var(--green);':'')+'" onmouseover="this.style.opacity=.85" onmouseout="this.style.opacity=1" title="Clique para ver os '+dv+' ticket(s)"':' style="padding:2px"';
    barHtml+='<div style="flex:1;display:flex;flex-direction:column;align-items:center;gap:2px"'+clickAttr+'>'
      +'<div style="font-size:10px;font-weight:700;color:var(--green)">'+(dv||'')+'</div>'
      +'<div style="width:100%;height:'+bh+'px;background:var(--green);border-radius:3px 3px 0 0;min-height:2px"></div>'
      +'<div style="font-size:9px;color:var(--muted);white-space:nowrap">'+dLbl.label+'</div></div>';
  }
  barHtml+='</div>';
  // Dia selecionado NÃO é mais concatenado aqui — vira variável separada
  // pra renderizar em largura total, fora do grid de 2 colunas do chart+utilities.
  var dayExpandHtml='';
  if(_clearedExpandDay){
    dayExpandHtml=_renderClearedDayExpand(_clearedExpandDay,c7);
  }

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

  // ── Clareados hoje (meia-noite local, não 24h rolling) ──
  var todayHtml='';
  if(cToday.length){
    todayHtml='<div style="margin-top:10px"><div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:4px">Clareados hoje ('+cToday.length+')</div>'
      +'<div style="display:flex;flex-wrap:wrap;gap:4px">';
    for(var it=0;it<cToday.length;it++){
      todayHtml+='<span style="font-size:10px;padding:2px 8px;border-radius:10px;background:var(--green-bg);color:var(--green);border:1px solid var(--green-border);cursor:pointer;font-family:var(--mono)" onclick="openTicketDetail('+cToday[it].id+')">'+esc(cToday[it].ticket)+'</span>';
    }
    todayHtml+='</div></div>';
  }

  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px"><div class="dash-card-title" style="margin-bottom:0">✅ Tickets Clareados</div>'+projSel+'</div>'
    +'<div class="cleared-cards-grid">'
    +_renderClearedCard('today',cToday.length,ftToday,'hoje',_clearedExpand==='today')
    +_renderClearedCard('7d',c7.length,ft7,'últimos 7 dias',_clearedExpand==='7d')
    +_renderClearedCard('30d',c30.length,ft30,'últimos 30 dias',_clearedExpand==='30d')
    +'</div>'
    +_renderClearedExpand(_clearedExpand,cToday,c7,c30)
    +'<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:12px">'
    +'<div><div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em;margin-bottom:6px">Clareados por dia (últimos 7 dias)</div>'+barHtml+todayHtml+'</div>'
    +'<div>'+utilListHtml+'</div>'
    +'</div>'
    // Dia expandido vai em largura total, FORA do grid 1fr 1fr acima
    +dayExpandHtml
    +'</div></div>';
}

// ═══════════ CLEARED CARDS: toggle + expand inline (footage desc + export) ═══════════
// Renderiza um dos 3 cards (hoje/7d/30d) clicável. Card ativo ganha borda mais grossa.
function _renderClearedCard(key,count,ft,label,isActive){
  var isHoje=(key==='today');
  var isHighlight=(isHoje||key==='7d');
  var bg=isHighlight?'var(--green-bg)':'var(--bg)';
  var borderColor=isHighlight?'var(--green-border)':'var(--border)';
  var txtColor=isHighlight?'var(--green)':'var(--text)';
  var labelColor=isHighlight?'var(--green)':'var(--muted)';
  var activeStyle=isActive?'box-shadow:0 0 0 2px var(--green);transform:translateY(-1px);':'';
  return'<div onclick="toggleClearedExpand(\''+key+'\')" style="padding:12px;background:'+bg+';border:1px solid '+borderColor+';border-radius:var(--r);text-align:center;cursor:pointer;transition:all .15s;'+activeStyle+'" onmouseover="this.style.opacity=.85" onmouseout="this.style.opacity=1">'
    +'<div style="font-size:22px;font-weight:700;font-family:var(--mono);color:'+txtColor+'">'+count+'</div>'
    +'<div style="font-size:10px;color:'+labelColor+'">'+esc(label)+'</div>'
    +'<div style="font-size:10px;color:var(--muted);font-family:var(--mono);margin-top:2px">'+ft.toLocaleString()+' ft</div>'
    +'<div style="font-size:9px;color:var(--muted);margin-top:4px">'+(isActive?'▲ clique p/ recolher':'▼ clique p/ expandir')+'</div>'
    +'</div>';
}

// Toggle: clica de novo no mesmo → fecha. Clica em outro → troca.
// Mutex com _clearedExpandDay: abrir card fecha dia expandido (e vice-versa).
function toggleClearedExpand(key){
  _clearedExpand=(_clearedExpand===key)?null:key;
  if(_clearedExpand)_clearedExpandDay=null;// fecha dia se tinha aberto
  refreshDashOrAnalytics();
}

// Toggle do dia do bar chart — segue mesmo padrão dos cards.
function toggleClearedDay(dayKey){
  _clearedExpandDay=(_clearedExpandDay===dayKey)?null:dayKey;
  if(_clearedExpandDay)_clearedExpand=null;// fecha card se tinha aberto
  refreshDashOrAnalytics();
}

// Seção expandida: tabela dos tickets do período selecionado, ordenada por footage DESC.
function _renderClearedExpand(key,cToday,c7,c30){
  if(!key)return'';
  var list=key==='today'?cToday:key==='7d'?c7:c30;
  var periodLabel=key==='today'?'hoje':key==='7d'?'últimos 7 dias':'últimos 30 dias';
  if(!list.length){
    return'<div style="margin-top:12px;padding:12px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);text-align:center;color:var(--muted);font-size:12px">Nenhum ticket clareado '+esc(periodLabel)+'.</div>';
  }
  // Ordena por footage desc (tie-breaker: ticket asc)
  var sorted=list.slice().sort(function(a,b){
    var fa=a.footage||0,fb=b.footage||0;
    if(fb!==fa)return fb-fa;
    return String(a.ticket||'').localeCompare(String(b.ticket||''));
  });
  var totalFt=0;for(var x=0;x<sorted.length;x++)totalFt+=(sorted[x].footage||0);
  // Tabela
  var rows='';
  for(var i=0;i<sorted.length;i++){
    var t=sorted[i];
    var projName=(projects.find(function(p){return p.id===t.projectId;})||{}).name||'—';
    var clearTs=_getTicketClearDateForExpand(t);
    var clearDateStr='—';
    if(clearTs){
      var dd=new Date(clearTs);
      clearDateStr=String(dd.getMonth()+1).padStart(2,'0')+'/'+String(dd.getDate()).padStart(2,'0')+'/'+dd.getFullYear();
    }
    rows+='<tr style="border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail('+t.id+')" onmouseover="this.style.background=\'var(--bg)\'" onmouseout="this.style.background=\'\'">'
      +'<td style="padding:6px 8px;font-family:var(--mono);font-size:11px">'+esc(t.ticket||'')+'</td>'
      +'<td style="padding:6px 8px;font-size:11px">'+esc(t.state||'')+'</td>'
      +'<td style="padding:6px 8px;font-size:11px">'+esc(t.location||'')+'</td>'
      +'<td style="padding:6px 8px;font-size:11px">'+esc(projName)+'</td>'
      +'<td style="padding:6px 8px;font-size:11px;font-family:var(--mono);color:var(--green);font-weight:700;text-align:right">'+(t.footage||0).toLocaleString()+' ft</td>'
      +'<td style="padding:6px 8px;font-size:11px;font-family:var(--mono);color:var(--muted)">'+esc(clearDateStr)+'</td>'
      +'</tr>';
  }
  return'<div style="margin-top:12px;padding:12px;background:var(--bg-alt,var(--bg));border:1px solid var(--green-border);border-radius:var(--r)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
    +'<div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em">Tickets clareados '+esc(periodLabel)+' ('+sorted.length+' · '+totalFt.toLocaleString()+' ft total)</div>'
    +'<button class="btn btn-sm" onclick="exportClearedTickets(\''+key+'\')" style="font-size:11px">📊 Exportar Excel</button>'
    +'</div>'
    +'<div style="overflow-x:auto;max-height:400px;overflow-y:auto">'
    +'<table style="width:100%;border-collapse:collapse;font-size:11px">'
    +'<thead style="position:sticky;top:0;background:var(--bg);z-index:1">'
    +'<tr style="border-bottom:2px solid var(--border)">'
    +'<th style="padding:6px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Ticket</th>'
    +'<th style="padding:6px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Estado</th>'
    +'<th style="padding:6px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Location</th>'
    +'<th style="padding:6px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Projeto</th>'
    +'<th style="padding:6px 8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage ↓</th>'
    +'<th style="padding:6px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Liberado em</th>'
    +'</tr></thead>'
    +'<tbody>'+rows+'</tbody>'
    +'</table></div></div>';
}

// Helper standalone pra extração de data — duplica lógica de getTicketClearDate que é local à função.
// Precisa ser acessível fora do escopo original do renderDashboard.
function _getTicketClearDateForExpand(t){
  if(!t.history||!t.history.length)return 0;
  for(var j=t.history.length-1;j>=0;j--){
    var a=(t.history[j].action||'').toLowerCase();
    var isAutoClear=a.indexOf('auto-clear')>=0;
    var isAuto811=a.indexOf('auto 811')>=0&&a.indexOf('revertido')<0&&a.indexOf('cancelado')<0;
    var isManualClear=a.indexOf('status manual')>=0&&a.indexOf('→ clear')>=0;
    if(isAutoClear||isAuto811||isManualClear){return t.history[j].ts;}
  }
  return 0;
}

// Converte timestamp → YYYY-MM-DD em fuso LOCAL (match com localDateKey do renderDashboard).
function _localDateKeyForExpand(ts){
  var d=new Date(ts);
  return d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
}

// Seção expandida de um DIA específico do bar chart.
// Filtra c7 pelos tickets cuja data de clear bate no dayKey (YYYY-MM-DD local).
function _renderClearedDayExpand(dayKey,c7){
  if(!dayKey)return'';
  var list=[];
  var seen={};
  for(var i=0;i<c7.length;i++){
    var t=c7[i];
    var cd=_getTicketClearDateForExpand(t);if(!cd)continue;
    if(_localDateKeyForExpand(cd)!==dayKey)continue;
    if(seen[t.ticket])continue;
    seen[t.ticket]=1;
    list.push(t);
  }
  // Label amigável do dia ("qui, 16/04/2026")
  var parts=dayKey.split('-');
  var dt=new Date(parseInt(parts[0]),parseInt(parts[1])-1,parseInt(parts[2]));
  var diasSemana=['dom','seg','ter','qua','qui','sex','sáb'];
  var dayLabel=diasSemana[dt.getDay()]+', '+String(dt.getDate()).padStart(2,'0')+'/'+String(dt.getMonth()+1).padStart(2,'0')+'/'+dt.getFullYear();

  if(!list.length){
    return'<div style="margin-top:10px;padding:10px;background:var(--bg);border:1px solid var(--border);border-radius:var(--r);text-align:center;color:var(--muted);font-size:11px">Nenhum ticket clareado em '+esc(dayLabel)+'.</div>';
  }
  // Ordena por footage desc
  list.sort(function(a,b){
    var fa=a.footage||0,fb=b.footage||0;
    if(fb!==fa)return fb-fa;
    return String(a.ticket||'').localeCompare(String(b.ticket||''));
  });
  var totalFt=0;for(var x=0;x<list.length;x++)totalFt+=(list[x].footage||0);
  var rows='';
  for(var i2=0;i2<list.length;i2++){
    var tk=list[i2];
    var projName=(projects.find(function(p){return p.id===tk.projectId;})||{}).name||'—';
    rows+='<tr style="border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail('+tk.id+')" onmouseover="this.style.background=\'var(--bg)\'" onmouseout="this.style.background=\'\'">'
      +'<td style="padding:5px 8px;font-family:var(--mono);font-size:11px">'+esc(tk.ticket||'')+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(tk.state||'')+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(tk.location||'')+'</td>'
      +'<td style="padding:5px 8px;font-size:11px">'+esc(projName)+'</td>'
      +'<td style="padding:5px 8px;font-size:11px;font-family:var(--mono);color:var(--green);font-weight:700;text-align:right">'+(tk.footage||0).toLocaleString()+' ft</td>'
      +'</tr>';
  }
  return'<div style="margin-top:10px;padding:10px;background:var(--bg-alt,var(--bg));border:1px solid var(--green-border);border-radius:var(--r)">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;flex-wrap:wrap;gap:6px">'
    +'<div style="font-size:11px;font-weight:700;color:var(--text2);text-transform:uppercase;letter-spacing:.04em">📅 '+esc(dayLabel)+' ('+list.length+' · '+totalFt.toLocaleString()+' ft)</div>'
    +'<button class="btn btn-sm" onclick="exportClearedDay(\''+dayKey+'\')" style="font-size:11px">📊 Exportar Excel</button>'
    +'</div>'
    +'<div style="overflow-x:auto;max-height:280px;overflow-y:auto">'
    +'<table style="width:100%;border-collapse:collapse;font-size:11px">'
    +'<thead style="position:sticky;top:0;background:var(--bg);z-index:1">'
    +'<tr style="border-bottom:2px solid var(--border)">'
    +'<th style="padding:5px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Ticket</th>'
    +'<th style="padding:5px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Estado</th>'
    +'<th style="padding:5px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Location</th>'
    +'<th style="padding:5px 8px;text-align:left;font-size:10px;color:var(--text2);text-transform:uppercase">Projeto</th>'
    +'<th style="padding:5px 8px;text-align:right;font-size:10px;color:var(--text2);text-transform:uppercase">Footage ↓</th>'
    +'</tr></thead>'
    +'<tbody>'+rows+'</tbody>'
    +'</table></div></div>';
}

// Exporta tickets de um dia específico.
function exportClearedDay(dayKey){
  var cpf=_clearProjFilter||'';
  var source=cpf?tickets.filter(function(t){return t.projectId===cpf;}):tickets;
  var list=[],seen={};
  for(var i=0;i<source.length;i++){
    var t=source[i];if(t.status==='Cancel')continue;
    var cd=_getTicketClearDateForExpand(t);if(!cd)continue;
    if(_localDateKeyForExpand(cd)!==dayKey)continue;
    if(seen[t.ticket])continue;
    seen[t.ticket]=1;
    list.push(t);
  }
  if(!list.length){toast('Nenhum ticket clareado nesse dia','warn');return;}
  list.sort(function(a,b){return(b.footage||0)-(a.footage||0);});

  var rows=[['Ticket','Cliente','Prime','Estado','Location','Projeto','Footage','Liberado em','Expira','Tipo','Endereço']];
  for(var r=0;r<list.length;r++){
    var tk=list[r];
    var projName=(projects.find(function(p){return p.id===tk.projectId;})||{}).name||'';
    var cdRT=_getTicketClearDateForExpand(tk);
    var cdStr='';
    if(cdRT){
      var dd=new Date(cdRT);
      cdStr=String(dd.getMonth()+1).padStart(2,'0')+'/'+String(dd.getDate()).padStart(2,'0')+'/'+dd.getFullYear();
    }
    rows.push([tk.ticket||'',tk.client||'',tk.prime||'',tk.state||'',tk.location||'',projName,tk.footage||0,cdStr,tk.expire||'',tk.tipo||'',tk.address||'']);
  }
  var wb=XLSX.utils.book_new();
  var ws=XLSX.utils.aoa_to_sheet(rows);
  XLSX.utils.book_append_sheet(wb,ws,'Clareados '+dayKey);
  XLSX.writeFile(wb,'OneDrill_Clareados_'+dayKey+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(list.length+' tickets exportados','success');
}

// Exporta lista de tickets clareados do período atual pra Excel.
// Reusa pattern de exportUtilTickets — com coluna "Liberado em" extra.
function exportClearedTickets(key){
  // Recalcula as listas (não guardamos em estado porque poderia ficar stale)
  var now=Date.now();
  var today=new Date();today.setHours(0,0,0,0);
  var todayCutoff=today.getTime();
  var day7=now-7*86400000;
  var day30=now-30*86400000;
  var cpf=_clearProjFilter||'';
  var source=cpf?tickets.filter(function(t){return t.projectId===cpf;}):tickets;

  var list=[],seen={};
  var cutoff=key==='today'?todayCutoff:key==='7d'?day7:day30;
  for(var i=0;i<source.length;i++){
    var t=source[i];if(t.status==='Cancel')continue;
    var cd=_getTicketClearDateForExpand(t);
    if(!cd||cd<cutoff)continue;
    if(seen[t.ticket])continue;
    seen[t.ticket]=1;
    list.push(t);
  }
  if(!list.length){toast('Nenhum ticket clareado no período','warn');return;}
  // Ordena por footage desc
  list.sort(function(a,b){return(b.footage||0)-(a.footage||0);});

  var periodLabel=key==='today'?'Hoje':key==='7d'?'7dias':'30dias';
  var rows=[['Ticket','Cliente','Prime','Estado','Location','Projeto','Footage','Liberado em','Expira','Tipo','Endereço']];
  for(var r=0;r<list.length;r++){
    var tk=list[r];
    var projName=(projects.find(function(p){return p.id===tk.projectId;})||{}).name||'';
    var cdRT=_getTicketClearDateForExpand(tk);
    var cdStr='';
    if(cdRT){
      var dd=new Date(cdRT);
      cdStr=String(dd.getMonth()+1).padStart(2,'0')+'/'+String(dd.getDate()).padStart(2,'0')+'/'+dd.getFullYear();
    }
    rows.push([tk.ticket||'',tk.client||'',tk.prime||'',tk.state||'',tk.location||'',projName,tk.footage||0,cdStr,tk.expire||'',tk.tipo||'',tk.address||'']);
  }
  var wb=XLSX.utils.book_new();
  var ws=XLSX.utils.aoa_to_sheet(rows);
  XLSX.utils.book_append_sheet(wb,ws,'Clareados '+periodLabel);
  XLSX.writeFile(wb,'OneDrill_Clareados_'+periodLabel+'_'+new Date().toISOString().slice(0,10)+'.xlsx');
  toast(list.length+' tickets exportados','success');
}

function renderWatchAndProtectAlert(fTickets){
  if(!utilCacheLoaded)return'';
  const wpTickets=[];
  const active=fTickets.filter(t=>t.status!=='Closed'&&t.status!=='Cancel'&&!isSuperseded(t));
  for(const t of active){
    const tkey=String(t.ticket).trim();
    const utils=getTicketUtils(tkey);
    const wpUtils=[];
    for(const u of utils){
      const rt=(u.response_text||'').toLowerCase();
      if(rt.includes('watch and protect'))wpUtils.push(u.utility_name);
    }
    const pending=(t.pending||'').toLowerCase();
    if(pending.includes('watch & protect')&&!wpUtils.length)wpUtils.push('(ver pending)');
    if(wpUtils.length)wpTickets.push({t,utils:wpUtils});
  }
  if(!wpTickets.length)return'';
  return'<div style="background:#fef2f2;border:2px solid #fca5a5;border-radius:var(--r-lg);padding:12px 14px;margin-bottom:14px">'
    +'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">'
    +'<span style="font-size:12px;font-weight:700;color:#dc2626">⚠️ Watch & Protect — Representante Obrigatório ('+wpTickets.length+' ticket'+(wpTickets.length>1?'s':'')+')</span>'
    +'</div>'
    +'<div style="font-size:10px;color:#991b1b;margin-bottom:8px">Estes tickets têm utilities com instalação <strong>CRÍTICA</strong>. Um representante da utility <strong>DEVE</strong> estar presente durante toda a escavação. NÃO inicie sem a presença do técnico.</div>'
    +'<div style="display:flex;flex-wrap:wrap;gap:6px">'
    +wpTickets.map(({t,utils})=>{
      const loc=esc((t.location||'').replace(/\s*(Inside|Near).*/i,'').split(',')[0].trim());
      return'<div style="background:white;border:1px solid #fca5a5;border-radius:var(--r);padding:8px 10px;cursor:pointer;min-width:220px;flex:1;max-width:320px" onclick="openTicketDetail('+t.id+')">'
        +'<div style="display:flex;justify-content:space-between;align-items:center">'
        +'<span style="font-family:var(--mono);font-weight:700;font-size:11px;color:var(--text)">'+esc(t.ticket)+'</span>'
        +'<span class="sbadge b-'+effStatusCls(t)+'" style="font-size:9px">'+esc(effStatusLabel(t))+'</span></div>'
        +'<div style="font-size:10px;color:var(--muted);margin-top:2px">'+loc+', '+esc(t.state)+'</div>'
        +'<div style="font-size:9px;color:#dc2626;margin-top:3px;font-weight:600">'+utils.map(esc).join(', ')+'</div>'
        +'</div>';
    }).join('')
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
        +'<span class="sbadge b-'+effStatusCls(t)+'" style="font-size:9px">'+esc(effStatusLabel(t))+'</span></div>'
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
    function isClear(h){const a=(h.action||'').toLowerCase();return(a.includes('auto 811')&&!a.includes('revertido')&&!a.includes('cancelado'))||a.includes('auto-clear')||(a.includes('status manual')&&a.includes('→ clear'));}
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
    // Fix bug #25: usar a variável local _progProjFilter (declarada com `let` na linha 89).
    // `let` no top-level NÃO cria propriedade em window (ES6), então window._progProjFilter
    // sempre era undefined → filtro nunca funcionava. Agora lê e escreve na mesma variável.
    const pf=_progProjFilter||'';
    const projOpts='<option value="">Todos (agrupado)</option>'+projStats.map(p=>'<option value="'+p.id+'"'+(pf===p.id?' selected':'')+'>'+(p.locs?esc(p.locs)+' ('+esc(p.name)+')':esc(p.name))+'</option>').join('');
    const projSel='<select class="fi" onchange="_progProjFilter=this.value;refreshDashOrAnalytics()" style="width:auto;min-width:160px;font-size:11px;padding:4px 6px">'+projOpts+'</select>';
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
  const exp=active.filter(t=>t.expire&&t.expire!=='—'&&t.status==='Open'&&_eod(t.expire)<new Date()&&!expireIsStale(t));
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
  var mprf=_metricPrimeFilter||'';
  var ft3=fTickets.filter(function(t){return (!mpf||t.projectId===mpf)&&(!mprf||(t.prime||'').trim()===mprf);});
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
  var projOpts='<option value="">Todos projetos</option>'+projects.filter(function(p){return p.status!=='Completed';}).map(function(p){return'<option value="'+p.id+'"'+(mpf===p.id?' selected':'')+'>'+esc(projDropLabel(p))+'</option>';}).join('');
  var projSel='<select class="fi" onchange="_metricProjFilter=this.value;refreshDashOrAnalytics()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px" title="Filtrar por projeto">'+projOpts+'</select>';
  // Dropdown por PRIME (compõe com o de projeto) — primes distintos dos tickets visíveis.
  var primesAll=[];var _seenP={};
  for(var pi=0;pi<fTickets.length;pi++){var pn=(fTickets[pi].prime||'').trim();if(pn&&!_seenP[pn]){_seenP[pn]=1;primesAll.push(pn);}}
  primesAll.sort(function(a,b){return a.localeCompare(b);});
  var primeOpts='<option value="">Todos primes</option>'+primesAll.map(function(pn){return'<option value="'+esc(pn)+'"'+(mprf===pn?' selected':'')+'>'+esc(pn)+'</option>';}).join('');
  var primeSel='<select class="fi" onchange="_metricPrimeFilter=this.value;refreshDashOrAnalytics()" style="width:auto;min-width:140px;font-size:11px;padding:4px 6px" title="Filtrar por prime">'+primeOpts+'</select>';
  if(!utilAvg.length){
    if(!mpf&&!mprf)return'';   // sem filtro e sem dados → painel some (comportamento original)
    // filtro ativo sem dados: mostra só o cabeçalho + dropdowns (senão o filtro fica preso)
    return '<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap"><div class="dash-card-title" style="margin-bottom:0">⏱ Tempo médio para Clear</div><div style="display:flex;align-items:center;gap:8px"><span style="font-size:12px;color:var(--muted)">sem dados de clear pra este filtro</span>'+projSel+primeSel+'</div></div></div></div>';
  }
  var globalAvg=utilAvg.reduce(function(s,u){return s+u.avg*u.count;},0)/utilAvg.reduce(function(s,u){return s+u.count;},0);
  var h='<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px"><div class="dash-card-title" style="margin-bottom:0">⏱ Tempo médio para Clear</div><div style="display:flex;align-items:center;gap:12px"><span style="font-size:20px;font-weight:700;font-family:var(--mono);color:'+(globalAvg<=3?'var(--green)':globalAvg<=6?'var(--amber)':'var(--red)')+'">'+globalAvg.toFixed(1)+' dias</span>'+projSel+primeSel+'</div></div>';
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
    +sorted.slice(0,15).map(([name,count])=>'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 10px;background:var(--bg);border-radius:var(--r);border:1px solid var(--border)"><span style="font-size:11px;color:var(--text2);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px" title="'+esc(name)+'">'+esc(name)+'</span><div style="display:flex;gap:6px;align-items:center"><span style="font-size:12px;font-weight:700;font-family:var(--mono);color:var(--red)">'+count+'</span><button class="btn btn-sm" onclick="exportUtilTickets(decodeURIComponent(\''+encodeURIComponent(name).replace(/\x27/g,"%27")+'\'))" style="font-size:9px;padding:2px 6px">↓</button></div></div>').join('')
    +'</div><button class="btn btn-sm" onclick="exportAllPending()" style="margin-top:8px;font-size:11px">↓ Exportar todas pendências</button></div></div>';
}

function renderRecentActivity(fT){
  const recent=[...fT].filter(t=>t.history&&t.history.length).sort((a,b)=>(b.history[b.history.length-1]?.ts||0)-(a.history[a.history.length-1]?.ts||0)).slice(0,15);
  if(!recent.length)return'';
  return'<div class="dash-row"><div class="dash-card" style="grid-column:1/-1"><div class="dash-card-title">🕐 Atividade Recente</div><div style="max-height:300px;overflow-y:auto">'
    +recent.map(t=>{const last=t.history[t.history.length-1];const es=effectiveStatus(t);return'<div style="display:flex;justify-content:space-between;align-items:center;padding:6px 0;border-bottom:1px solid var(--border);cursor:pointer" onclick="openTicketDetail('+t.id+')"><div><span style="font-family:var(--mono);font-weight:600;font-size:12px">'+esc(t.ticket)+'</span> <span class="sbadge b-'+es.toLowerCase()+'" style="font-size:9px">'+esc(es)+'</span><div style="font-size:10px;color:var(--muted)">'+esc(last?.action||'—')+'</div></div><span style="font-size:10px;color:var(--muted);white-space:nowrap">'+fmtDt(last?.ts||0)+'</span></div>';}).join('')
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
    const by={IN:[],FL:[],IL:[],WI:[]};
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
    for(const st of ['IN','FL','IL','WI']){
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
  for(const st of ['IN','FL','IL','WI']){
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
  const active=projects.filter(p=>p.status!=='Completed');
  const completed=projects.filter(p=>p.status==='Completed');
  const mkOpts=(label)=>'<option value="">'+label+'</option>'
    +active.map(p=>`<option value="${p.id}">${esc(projDropLabel(p))}</option>`).join('')
    +(completed.length?'<optgroup label="── Concluídos ──">'+completed.map(p=>`<option value="${p.id}">📁 ${esc(projDropLabel(p))}</option>`).join('')+'</optgroup>':'');
  const pf=document.getElementById('proj-filter');if(pf){const pv=pf.value;pf.innerHTML=mkOpts('Todos os projetos');if(pv)pf.value=pv;}
  const tp=document.getElementById('tbl-proj');if(tp){const tv=tp.value;tp.innerHTML=mkOpts('Todos projetos');if(tv)tp.value=tv;}
  const tm=document.getElementById('tm-proj');if(tm){const mv=tm.value;tm.innerHTML='<option value="">Sem projeto</option>'+projects.map(p=>`<option value="${p.id}">${esc(projDropLabel(p))}</option>`).join('');if(mv)tm.value=mv;}
}
function syncPrimes(){
  const primes=[...new Set(tickets.map(t=>t.prime).filter(Boolean))].sort();
  ['tbl-prime'].forEach(id=>{const el=document.getElementById(id);if(el){const pv=el.value;el.innerHTML='<option value="">Todos primes</option>'+primes.map(c=>`<option>${esc(c)}</option>`).join('');if(pv)el.value=pv;}});
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
  if(el){const pv=el.value;el.innerHTML='<option value="">Todos locais</option>'+locs.map(l=>`<option>${esc(l)}</option>`).join('');if(pv)el.value=pv;}
}
/* ═══════════ TIMELINE / DIG WINDOW ═══════════ */
function renderTimeline(){
  const el=document.getElementById('timeline-content');if(!el)return;
  const now=new Date();
  const active=tickets.filter(t=>{
    const s=(t.status||'').toLowerCase();
    return s==='open'||s==='damage'||s==='clear';
  });
  const withExpire=active.map(t=>{
    const ed=t.expire?_eod(t.expire):null;
    return{t,ed};
  }).filter(x=>x.ed&&!isNaN(x.ed.getTime())).sort((a,b)=>a.ed-b.ed);

  if(!withExpire.length){el.innerHTML='<div style="text-align:center;padding:40px;color:var(--muted)">Nenhum ticket com data de vencimento</div>';return;}

  // Range: 14 days back → 45 days ahead
  const rangeStart=new Date(now);rangeStart.setDate(rangeStart.getDate()-14);
  const rangeEnd=new Date(now);rangeEnd.setDate(rangeEnd.getDate()+45);
  const totalDays=Math.round((rangeEnd-rangeStart)/(86400000));

  // Stats
  const expired=withExpire.filter(x=>x.ed<now).length;
  const soon7=withExpire.filter(x=>{const d=(x.ed-now)/86400000;return d>=0&&d<=7;}).length;
  const soon14=withExpire.filter(x=>{const d=(x.ed-now)/86400000;return d>7&&d<=14;}).length;

  // Filters
  const stateOpts=[...new Set(withExpire.map(x=>x.t.state).filter(Boolean))].sort();
  const projOpts=[...new Set(withExpire.map(x=>x.t.projectId).filter(Boolean))].map(pid=>{const p=projects.find(x=>x.id===pid);return p?{id:pid,name:p.name}:null;}).filter(Boolean).sort((a,b)=>a.name.localeCompare(b.name));

  let h='<div style="margin-bottom:16px">';
  h+='<div style="display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:12px">';
  h+='<h2 style="margin:0;font-size:18px;font-weight:700">📅 Timeline — Dig Window</h2>';
  h+='<select class="fi" id="tl-state" onchange="renderTimeline()" style="width:auto;min-width:100px;font-size:12px;padding:4px 8px"><option value="">Todos estados</option>'+stateOpts.map(s=>'<option>'+esc(s)+'</option>').join('')+'</select>';
  h+='<select class="fi" id="tl-proj" onchange="renderTimeline()" style="width:auto;min-width:140px;font-size:12px;padding:4px 8px"><option value="">Todos projetos</option>'+projOpts.map(p=>'<option value="'+esc(p.id)+'">'+esc(p.name)+'</option>').join('')+'</select>';
  h+='<select class="fi" id="tl-status" onchange="renderTimeline()" style="width:auto;font-size:12px;padding:4px 8px"><option value="">Todos status</option><option>Open</option><option>Clear</option><option>Damage</option></select>';
  h+='</div>';

  // Stat pills
  h+='<div style="display:flex;gap:8px;margin-bottom:14px;flex-wrap:wrap">';
  h+='<span style="padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;background:#fef2f2;color:#dc2626;border:1px solid #fecaca">⛔ '+expired+' vencido'+(expired!==1?'s':'')+'</span>';
  h+='<span style="padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;background:#fffbeb;color:#d97706;border:1px solid #fde68a">⚠ '+soon7+' em 7d</span>';
  h+='<span style="padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;background:#eff6ff;color:#2563eb;border:1px solid #bfdbfe">📋 '+soon14+' em 14d</span>';
  h+='<span style="padding:4px 10px;border-radius:20px;font-size:11px;font-weight:600;background:#f0fdf4;color:#16a34a;border:1px solid #bbf7d0">✅ '+(withExpire.length-expired-soon7-soon14)+' ok</span>';
  h+='</div></div>';

  // Get active filters (preserve across re-renders)
  const curState=document.getElementById('tl-state')?.value||'';
  const curProj=document.getElementById('tl-proj')?.value||'';
  const curStatus=document.getElementById('tl-status')?.value||'';

  let filtered=withExpire;
  if(curState)filtered=filtered.filter(x=>x.t.state===curState);
  if(curProj)filtered=filtered.filter(x=>x.t.projectId===curProj);
  if(curStatus)filtered=filtered.filter(x=>effectiveStatus(x.t)===curStatus);

  // Date headers
  h+='<div style="position:relative;overflow-x:auto;padding-bottom:12px">';
  h+='<div style="display:flex;align-items:center;height:24px;border-bottom:1px solid var(--border);margin-left:200px;position:relative;min-width:'+(totalDays*24)+'px">';
  for(let i=0;i<=totalDays;i+=7){
    const d=new Date(rangeStart);d.setDate(d.getDate()+i);
    const isToday=d.toDateString()===now.toDateString();
    const leftPx=Math.round((i/totalDays)*totalDays*24);
    h+='<span style="position:absolute;left:'+leftPx+'px;font-size:9px;font-family:var(--mono);color:'+(isToday?'var(--red)':'var(--muted)')+'">'+d.toLocaleDateString('pt-BR',{day:'2-digit',month:'2-digit'})+'</span>';
  }
  h+='</div>';

  // Today line position
  const todayPct=Math.max(0,Math.min(100,((now-rangeStart)/(rangeEnd-rangeStart))*100));

  // Bars
  const maxRows=Math.min(filtered.length,150);
  for(let i=0;i<maxRows;i++){
    const{t:tk,ed}=filtered[i];
    const es=effectiveStatus(tk);
    const daysLeft=Math.round((ed-now)/86400000);
    const isExp=ed<now;
    const proj=projects.find(p=>p.id===tk.projectId);

    // Bar position
    const barEnd=Math.max(0,Math.min(100,((ed-rangeStart)/(rangeEnd-rangeStart))*100));
    const barStart=Math.max(0,barEnd-8);// Approximate 8% width per ticket
    const barW=Math.max(2,barEnd-barStart);

    const bc=isExp?'#dc2626':es==='Clear'?'#16a34a':es==='Damage'?'#db2777':'#2563eb';
    const bgc=isExp?'#fef2f2':es==='Clear'?'#f0fdf4':es==='Damage'?'#fffbeb':'#eff6ff';

    h+='<div style="display:flex;align-items:center;height:26px;border-bottom:1px solid var(--border)" onclick="openTicketDetail('+tk.id+')" title="'+esc(tk.ticket)+' — '+esc(es)+' — vence '+esc(tk.expire||'?')+'" class="tl-row">';
    // Label
    h+='<div style="width:200px;min-width:200px;padding:0 8px;display:flex;align-items:center;gap:6px;overflow:hidden">';
    h+='<span style="font-family:var(--mono);font-size:11px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:'+bc+'">'+esc(tk.ticket)+'</span>';
    h+='<span style="font-size:9px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis">'+esc(proj?proj.name:tk.client)+'</span>';
    h+='</div>';
    // Bar area
    h+='<div style="flex:1;position:relative;height:100%;min-width:'+(totalDays*24)+'px">';
    h+='<div style="position:absolute;left:'+barStart+'%;width:'+barW+'%;top:4px;height:18px;background:'+bc+';border-radius:3px;opacity:0.85"></div>';
    // Expire label on bar
    h+='<span style="position:absolute;left:'+(barEnd+0.5)+'%;top:5px;font-size:9px;font-family:var(--mono);color:var(--text2);white-space:nowrap">'+(isExp?'⛔ '+Math.abs(daysLeft)+'d atrás':(daysLeft<=7?'⚠ '+daysLeft+'d':daysLeft+'d'))+'</span>';
    h+='</div>';
    h+='</div>';
  }

  if(filtered.length>maxRows)h+='<div style="text-align:center;padding:8px;color:var(--muted);font-size:11px">... +'+(filtered.length-maxRows)+' tickets</div>';

  // Today vertical line (overlay)
  h+='<div style="position:absolute;top:0;bottom:0;left:calc(200px + '+todayPct+'%);width:2px;background:var(--red);opacity:0.5;pointer-events:none;z-index:1"></div>';
  h+='</div>';

  el.innerHTML=h;

  // Restore filter values after innerHTML
  const tlS=document.getElementById('tl-state');if(tlS&&curState)tlS.value=curState;
  const tlP=document.getElementById('tl-proj');if(tlP&&curProj)tlP.value=curProj;
  const tlSt=document.getElementById('tl-status');if(tlSt&&curStatus)tlSt.value=curStatus;
}

function syncAll(){
  rebuildSupersededSet();syncProjectSelects();syncPrimes();syncLocations();updateCompletedSidebar();
  if(utilCacheLoaded){syncUtilFilter();syncMapUtilFilter();}
  const ap=document.querySelector('.page.active')?.id;
  if(ap==='pg-map'){renderList();renderMap();}
  else if(ap==='pg-tickets')renderTable();
  else if(ap==='pg-proj')renderProjects();
  else if(ap==='pg-dash')renderDash();
  else if(ap==='pg-contacts')renderContacts();
  else if(ap==='pg-analytics')renderAnalytics();
  else if(ap==='pg-completed')renderCompletedPage();
  else renderDash();
}

async function manualRefresh(){
  setSyncStatus(true,'Atualizando...');
  try{
    const{data:p}=await sb.from('projects').select('*').order('name');
    let _mrt=[];let _mf2=0;
    while(true){const{data:pg}=await sb.from('tickets').select('*').order('ticket').range(_mf2,_mf2+999);if(!pg||!pg.length)break;_mrt=_mrt.concat(pg);if(pg.length<1000)break;_mf2+=1000;}
    if(p)projects=p.map(dbToProject);
    if(_mrt.length)tickets=_mrt.map(dbToTicket);
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
// Fix bug #33: listeners de offline/online devem ser registrados no TOP LEVEL,
// não dentro do load handler. Antes: funcionavam por acaso (load só dispara 1x), mas
// se algum erro síncrono acontecesse antes do addEventListener, os listeners nunca
// eram registrados. Registrando aqui, são garantidamente ativos desde o parse do script.
window.addEventListener('offline',()=>{toast('⚠ Sem conexão — alterações não serão salvas','danger');setSyncStatus(false,'Offline');});
window.addEventListener('online',()=>{toast('✅ Conexão restaurada','success');setSyncStatus(true,'Online');});

window.addEventListener('load',async()=>{
  document.querySelector('#loading-screen div:last-child').textContent='Conectando ao Supabase...';
  const ok=await initSupabase();
  document.getElementById('loading-screen').style.display='none';
  if(!ok){document.getElementById('login-screen').classList.remove('hidden');document.getElementById('login-screen').style.display='flex';setTimeout(()=>toast('Aviso: erro ao conectar ao banco.','warn'),500);return;}
  if(checkProjectUrl())return;
  try{
    const{data:{session}}=await sb.auth.getSession();
    if(session){
      await resolveRole(session.user);
      enterApp();
      return;
    }
  }catch(e){console.log('[Auth] No session:',e);}
  document.getElementById('login-screen').classList.remove('hidden');
  document.getElementById('login-screen').style.display='flex';
});
