/* ============================================================================
   JARVIS — assistente de voz READ-ONLY do OneDrill
   ----------------------------------------------------------------------------
   Campo de busca por voz + texto. Responde perguntas sobre projetos e tickets
   e FALA a resposta. Seletor de voz no painel (prioriza pt-BR / Antônio).

   ⚠️ 100% READ-ONLY POR CONSTRUÇÃO:
      - Só LÊ os arrays globais `projects` e `tickets` (já carregados pelo app.js)
      - Só chama funções de leitura (`effectiveStatus`) e de navegação
      - NÃO existe aqui NENHUMA chamada de escrita:
        nada de saveTicketToDb / saveProjectToDb / applyBulkUpdate / .update /
        .delete / .upsert / syncAll. Este arquivo não pode alterar nada.

   Carregado por: <script src="jarvis.js?v=..."></script> no index.html,
   depois do app.js (precisa dos globais dele).
============================================================================ */
(function () {
  'use strict';

  // ── Helpers de texto ──────────────────────────────────────────────────
  const norm = (s) => (s || '')
    .toString()
    .toLowerCase()
    .normalize('NFD').replace(/[̀-ͯ]/g, '') // tira acentos
    .replace(/\s+/g, ' ')
    .trim();

  const fmt = (n) => (Math.round(n || 0)).toLocaleString('pt-BR');

  // Acesso seguro aos globais do app (podem não existir ainda)
  const getProjects = () => (typeof projects !== 'undefined' && Array.isArray(projects)) ? projects : [];
  const getTickets  = () => (typeof tickets  !== 'undefined' && Array.isArray(tickets))  ? tickets  : [];
  const effStatus   = (t) => (typeof effectiveStatus === 'function') ? effectiveStatus(t) : (t.status || '');

  // Referências de UI — declarado no topo porque o código de voz (abaixo) já
  // referencia `el` antes do bloco de UI. Sem isto = ReferenceError (TDZ).
  const el = {};

  // ── Voz (fala) ─────────────────────────────────────────────────────────
  const hasTTS = ('speechSynthesis' in window);
  let VOICES = [];
  let CHOSEN_VOICE = null;   // objeto SpeechSynthesisVoice
  let SPEAK_ON = true;       // botão de mudo
  const LS_KEY = 'jarvis-voice';
  // "Jarvis" em fonética inglesa: a voz pt-BR leria "jarviz"; "Djárvis" soa como o "JAR-vis" inglês.
  // Só afeta a FALA — o texto na tela continua "Jarvis" / "J.A.R.V.I.S.".
  const SPOKEN_NAME = 'Djárvis';

  const allVoices = () => hasTTS ? (window.speechSynthesis.getVoices() || []) : [];
  const isPt = (v) => /^pt/i.test((v && v.lang) || '');

  function scoreVoice(v) {
    const n = (v.name || '').toLowerCase();
    let s = 0;
    if (n.includes('antonio')) s += 100;         // voz aprovada (Edge Natural)
    if (n.includes('natural')) s += 50;
    if (n.includes('online'))  s += 40;
    if (n.includes('google'))  s += 30;          // Chrome: "Google português do Brasil"
    if (/pt-br/i.test(v.lang || '')) s += 20;    // BR antes de PT
    return s;
  }

  function bestPtVoice() {
    const pt = allVoices().filter(isPt).sort((a, b) => scoreVoice(b) - scoreVoice(a));
    return pt[0] || null;
  }

  // Restaura a escolha salva pelo usuário (por nome), senão a melhor pt-BR
  function restoreChosen() {
    const saved = (() => { try { return localStorage.getItem(LS_KEY); } catch (e) { return null; } })();
    if (saved) {
      const v = allVoices().find(x => x.name === saved);
      if (v) { CHOSEN_VOICE = v; return; }
    }
    CHOSEN_VOICE = bestPtVoice();
  }

  function refreshVoices() {
    VOICES = allVoices();
    if (!CHOSEN_VOICE || !VOICES.includes(CHOSEN_VOICE)) restoreChosen();
    populateVoiceSelect();
  }

  if (hasTTS) {
    refreshVoices();
    window.speechSynthesis.onvoiceschanged = refreshVoices;
    // Alguns navegadores só populam depois de um tempinho:
    setTimeout(refreshVoices, 400);
    setTimeout(refreshVoices, 1500);
  }

  // ── Voz NEURAL (Antônio) via BACKEND (Supabase Edge Function "tts") ──
  // O navegador NÃO consegue chamar o motor Edge TTS direto (a Microsoft recusa origem
  // de site — testado). Então o áudio é gerado no SERVIDOR e o site só TOCA o mp3.
  // MESMA voz em qualquer navegador. Se o backend não responder → voz do navegador (fallback).
  const TTS_FN_URL = 'https://ofbqtaulvzeltfpqcjhh.supabase.co/functions/v1/bright-api';
  const _ANON = ((document.querySelector('meta[name="supabase-anon-key"]') || {}).content) || '';
  let NEURAL_ON = true;
  let _curAudio = null;
  async function speakNeural(text) {
    const r = await fetch(`${TTS_FN_URL}?rate=%2B6%25&text=${encodeURIComponent(text)}`, {
      cache: 'no-store',
      headers: _ANON ? { 'apikey': _ANON, 'Authorization': 'Bearer ' + _ANON } : {},
    });
    if (!r.ok) throw new Error('tts ' + r.status);
    const blob = await r.blob();
    if (!blob || !blob.size || /json/.test(blob.type)) throw new Error('sem áudio');
    return blob;
  }

  // Transforma o texto pra FALA (fonética "Jarvis", IDs dígito a dígito, estados por extenso).
  function transformSpoken(text) {
    let s = text.replace(/j\.\s*a\.\s*r\.\s*v\.\s*i\.\s*s\.?/gi, SPOKEN_NAME).replace(/jarvis/gi, SPOKEN_NAME);
    s = s.replace(/\d{6,}/g, m => m.split('').join(' '));
    s = s.replace(/\b(FL|IN|IL|WI)\b/g, m => ({ FL: 'Flórida', IN: 'Indiana', IL: 'Illinois', WI: 'Wisconsin' }[m]));
    return s;
  }
  function _speakBrowser(spoken) {
    if (!hasTTS) return;
    try {
      window.speechSynthesis.cancel();
      if (!CHOSEN_VOICE) restoreChosen();
      if (!CHOSEN_VOICE || !isPt(CHOSEN_VOICE)) { const alt = bestPtVoice(); if (alt) CHOSEN_VOICE = alt; }
      if (!CHOSEN_VOICE || !isPt(CHOSEN_VOICE)) { if (el.status) el.status.textContent = '⚠ Sem voz pt-BR neste navegador.'; return; }
      const u = new SpeechSynthesisUtterance(spoken);
      u.voice = CHOSEN_VOICE; u.lang = CHOSEN_VOICE.lang || 'pt-BR'; u.rate = 1.05; u.pitch = 1.0;
      window.speechSynthesis.speak(u);
    } catch (e) { /* silencioso */ }
  }
  function _stopSpeaking() {
    try { window.speechSynthesis && window.speechSynthesis.cancel(); } catch (e) {}
    if (_curAudio) { try { _curAudio.pause(); } catch (e) {} _curAudio = null; }
  }
  async function speak(text) {
    if (!SPEAK_ON || !text) return;
    const spoken = transformSpoken(text);
    if (NEURAL_ON) {
      try {
        const blob = await speakNeural(spoken);
        if (!SPEAK_ON) return;            // mutado enquanto buscava
        _stopSpeaking();
        _curAudio = new Audio(URL.createObjectURL(blob));
        _curAudio.play().catch(() => {});
        return;
      } catch (e) {
        // motor neural indisponível neste navegador → desliga e usa a voz do navegador
        NEURAL_ON = false;
        if (el.status) el.status.textContent = 'Voz neural indisponível aqui — usando a voz do navegador.';
      }
    }
    _speakBrowser(spoken);
  }

  // ── Localizar projeto/ticket na pergunta ────────────────────────────────
  function findProject(q) {
    const qn = norm(q);
    // números "de projeto" citados (4 a 8 dígitos). Mínimo 4 pra não confundir
    // metragem/contagem ("222 pés") com número de projeto; exclui IDs longos de ticket.
    const qNums = (qn.match(/\d{4,}/g) || []).filter(n => n.length <= 8);
    let best = null, bestScore = 0;
    for (const p of getProjects()) {
      const name = norm(p.name), desc = norm(p.desc);
      const pNums = (name + ' ' + desc).match(/\d{3,}/g) || [];
      // Pontuação por NÚMERO (forte — e desempata projetos de mesmo nome)
      let numScore = 0;
      for (const qNum of qNums) for (const pNum of pNums) {
        if (pNum === qNum) numScore = Math.max(numScore, 100 + qNum.length);
        else if (pNum.endsWith(qNum) || qNum.endsWith(pNum)) numScore = Math.max(numScore, 60 + qNum.length);
        else if (pNum.includes(qNum)) numScore = Math.max(numScore, 40 + qNum.length);
      }
      // Pontuação por NOME/palavra
      let nameScore = 0;
      if (name && qn.includes(name)) nameScore = Math.max(nameScore, name.length);
      if (desc && qn.includes(desc)) nameScore = Math.max(nameScore, desc.length);
      for (const tok of name.split(/[^a-z0-9]+/)) {
        if (tok.length >= 4 && !/^\d+$/.test(tok) && qn.includes(tok)) nameScore = Math.max(nameScore, tok.length);
      }
      const score = numScore + nameScore;   // número + nome (soma → desempata)
      if (score > bestScore) { bestScore = score; best = p; }
    }
    return best;
  }

  function findTicket(q) {
    const m = String(q).match(/\d{6,}/g);
    if (!m) return null;
    const tks = getTickets();
    for (const num of m) {
      const hit = tks.find(t => String(t.ticket || '').replace(/\D/g, '') === num.replace(/\D/g, ''));
      if (hit) return hit;
    }
    for (const num of m) {
      const hit = tks.find(t => String(t.ticket || '').includes(num));
      if (hit) return hit;
    }
    return null;
  }

  // IMPORTANTE: usa a MESMA filterTickets do resto da UI (card, tabela, mapa).
  // Ela exclui tickets SUPERSEDED (antigos/renovados) e de projetos Completed —
  // senão o Jarvis conta tickets escondidos e infla open/clear (bug reportado).
  const _ft = () => (typeof filterTickets === 'function') ? filterTickets : null;
  const ticketsOf = (pid) => { const f = _ft(); return f ? f({ projectId: pid }) : getTickets().filter(t => t.projectId === pid); };
  const visibleTickets = () => { const f = _ft(); return f ? f({}) : getTickets(); };

  // Feet liberado que ainda dá pra trabalhar (mesma fórmula do card, linha 3145)
  function clearAvailableFt(ts) {
    return ts.filter(t => effStatus(t) === 'Clear')
             .reduce((s, t) => s + Math.max(0, (t.footage || 0) - (t.completedFeet || 0)), 0);
  }

  function traduzStatus(s) {
    const m = { Clear: 'liberado, clear', Open: 'em aberto', Damage: 'com dano', Closed: 'concluído', Cancel: 'cancelado', Pending: 'pendente', Private: 'particular', Marked: 'marcada', Unmarked: 'não marcada' };
    return m[s] || (s || 'sem status');
  }

  // ── Contexto conversacional ──
  let lastProject = null;   // último projeto perguntado
  let lastScope   = null;   // último status em foco ('Open'/'Clear'/'Damage'/null)
  let lastList    = [];     // últimos tickets listados (pra "o primeiro", "deles")
  let lastTicket  = null;   // último ticket específico
  let lastIntent  = null;   // última intenção ('util', ...) pra follow-ups tipo "e do segundo"
  let lastState   = null;   // último ESTADO em foco (fl/in/il/wi) — de relatório de estado

  const stLabel = (s) => s === 'Open' ? 'em aberto' : s === 'Clear' ? 'clear' : s === 'Damage' ? 'com dano' : s === 'Closed' ? 'concluídos' : '';
  const listWords = (a) => a.length <= 1 ? (a[0] || '') : a.slice(0, -1).join(', ') + ' e ' + a[a.length - 1];
  const nTk = (n) => n + (n === 1 ? ' ticket' : ' tickets');   // pluralização correta

  const ORD = { primeiro:0, primeira:0, segundo:1, segunda:1, terceiro:2, terceira:2, quarto:3, quarta:3, quinto:4, quinta:4, sexto:5, sexta:5 };
  function resolveOrdinal(qn) {
    if (/\bultim[oa]/.test(qn)) return 'last';
    for (const k in ORD) if (qn.includes(k)) return ORD[k];
    const m = qn.match(/\b(\d+)\s*[oaº°]\b/);
    if (m) return parseInt(m[1], 10) - 1;
    return null;
  }
  function statusFromQuery(qn) {
    if (/aberto|\bopen|pendente|preso/.test(qn)) return 'Open';
    if (/clear|liberad|disponivel/.test(qn)) return 'Clear';
    if (/damage|dano/.test(qn)) return 'Damage';
    if (/concluid|closed|fechad/.test(qn)) return 'Closed';
    if (/todos|todas/.test(qn)) return null;
    return undefined; // não especificado
  }
  function ticketsByStatus(proj, status) {
    const ts = ticketsOf(proj.id);
    if (!status) return ts;
    if (status === 'Closed') return ts.filter(t => t.status === 'Closed');
    return ts.filter(t => effStatus(t) === status);
  }
  // Resolve QUAL ticket a pergunta se refere: número explícito, ordinal
  // ("o primeiro"/"o último") sobre a última lista, ou o último ticket citado.
  function resolveTicketRef(qn) {
    const byNum = findTicket(qn);
    if (byNum) return byNum;
    const ord = resolveOrdinal(qn);
    if (ord !== null) {
      let list = (lastList && lastList.length) ? lastList : (lastProject ? ticketsByStatus(lastProject, 'Open') : []);
      if (list.length) { const i = ord === 'last' ? list.length - 1 : ord; if (i >= 0 && i < list.length) return list[i]; }
    }
    return lastTicket;
  }
  const setCtx = (status, list) => { lastScope = status; lastList = list ? list.slice() : []; lastTicket = null; };

  // Monta a resposta de UM projeto conforme o sub-intento da pergunta.
  function answerProject(proj, has) {
    const ts = ticketsOf(proj.id);
    const openTs = ts.filter(t => effStatus(t) === 'Open');
    const clearTs = ts.filter(t => effStatus(t) === 'Clear');
    const damageTs = ts.filter(t => effStatus(t) === 'Damage');
    const closedC = ts.filter(t => t.status === 'Closed').length;
    const clearFt = clearAvailableFt(ts);
    const openFt  = openTs.reduce((s, t) => s + (t.footage || 0), 0);
    const totFt   = ts.reduce((s, t) => s + (t.footage || 0), 0);

    if (has('disponivel', 'liberad', 'trabalhar', 'trabalho', 'cavar', 'pes', 'feet', 'footage', 'metragem')) {
      setCtx('Clear', clearTs);
      let txt = `No projeto ${proj.name}, você tem ${fmt(clearFt)} pés liberados pra trabalhar agora`;
      txt += clearTs.length ? `, distribuídos em ${clearTs.length} ${clearTs.length === 1 ? 'ticket' : 'tickets'} clear.` : '.';
      if (openFt) txt += ` Ainda tem ${fmt(openFt)} pés presos em ${openTs.length} ${openTs.length === 1 ? 'ticket' : 'tickets'} em aberto, esperando liberação.`;
      return txt;
    }
    if (has('aberto', 'open', 'pendente', 'pendencia', 'falta', 'preso')) {
      setCtx('Open', openTs);
      return `O projeto ${proj.name} tem ${openTs.length} ${openTs.length === 1 ? 'ticket aberto' : 'tickets abertos'}, somando ${fmt(openFt)} pés esperando liberação${damageTs.length ? `, e ${damageTs.length} com dano` : ''}.`;
    }
    if (has('clear')) {
      setCtx('Clear', clearTs);
      return `O projeto ${proj.name} tem ${clearTs.length} ${clearTs.length === 1 ? 'ticket clear' : 'tickets clear'}, com ${fmt(clearFt)} pés liberados pra trabalhar.`;
    }
    if (has('damage', 'dano')) {
      setCtx('Damage', damageTs);
      return `O projeto ${proj.name} tem ${damageTs.length} ${damageTs.length === 1 ? 'ticket com dano' : 'tickets com dano'}.`;
    }
    setCtx(null, null);
    return `Projeto ${proj.name}, cliente ${proj.client || 'não informado'}, no estado ${proj.state || '—'}. `
      + `São ${ts.length} tickets no total, ${clearTs.length} clear, ${openTs.length} abertos${damageTs.length ? `, ${damageTs.length} com dano` : ''}${closedC ? ` e ${closedC} concluídos` : ''}. `
      + `${fmt(clearFt)} pés liberados pra trabalhar de ${fmt(totFt)} pés no total.`;
  }

  // Agrega utilities pendentes de TODOS os tickets do projeto (o que está segurando).
  function aggregatePendingUtils(proj) {
    const counts = {};
    ticketsOf(proj.id).forEach(t => {
      ((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).forEach(u => {
        const n = u.utility_name || u.name || u.utility; if (n) counts[n] = (counts[n] || 0) + 1;
      });
    });
    const arr = Object.entries(counts).sort((a, b) => b[1] - a[1]);
    if (!arr.length) return `No projeto ${proj.name} não há utilities pendentes registradas.`;
    const parts = arr.slice(0, 10).map(([n, c]) => `${n} (${c})`);
    let txt = `No projeto ${proj.name}, ${arr.length === 1 ? 'falta responder 1 utility' : `faltam responder ${arr.length} utilities`}: ${parts.join(', ')}`;
    if (arr.length > 10) txt += `, e mais ${arr.length - 10}`;
    return txt + '.';
  }

  // Tokens genéricos que NÃO identificam uma utility sozinhos (evita falso-positivo)
  const _UTIL_STOP = new Set(['utilities','utility','energy','county','water','gas','distribution','communications','communication','fiber','commission','peoples','llc','inc','the','and','orlando','st','city','of','north','south']);
  // Acha uma utility citada na pergunta, entre as que têm PENDÊNCIA no projeto.
  function matchUtilityInQuery(proj, qn) {
    const names = new Set();
    ticketsOf(proj.id).forEach(t => ((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).forEach(u => { const n = u.utility_name || u.name; if (n) names.add(n); }));
    let best = null, bestLen = 0;
    for (const n of names) for (const tok of norm(n).split(/[^a-z0-9]+/)) {
      if (tok.length >= 3 && !_UTIL_STOP.has(tok) && qn.includes(tok) && tok.length > bestLen) { best = n; bestLen = tok.length; }
    }
    return best;
  }
  // Lista os tickets do projeto que têm ESSA utility pendente.
  function ticketsPendingUtility(proj, utilName) {
    return ticketsOf(proj.id).filter(t => ((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).some(u => (u.utility_name || u.name) === utilName));
  }

  // Dias até vencer (reusa o helper do app; negativo = vencido, null = sem data)
  const daysToExpire = (t) => (typeof _daysToEffExpire === 'function') ? _daysToEffExpire(t) : null;
  const _dexp = (t) => { const d = daysToExpire(t); return d === null ? 99999 : d; };
  // Escopo atual: projeto em foco → estado em foco → sistema inteiro.
  function currentScope() {
    if (lastProject) return { ts: ticketsOf(lastProject.id), label: `no projeto ${lastProject.name}` };
    if (lastState) return { ts: visibleTickets().filter(t => norm(t.state) === lastState), label: `em ${lastState.toUpperCase()}` };
    return { ts: visibleTickets(), label: 'no sistema' };
  }
  // Acha um cliente/prime citado na pergunta.
  const _CP_STOP = new Set(['energy','the','and','of','llc','inc','company','co','services','construction','group','utilities','communications','tel','com']);
  function matchClientOrPrime(qn) {
    const set = new Set();
    getTickets().forEach(t => { if (t.client) set.add(t.client); if (t.prime) set.add(t.prime); });
    let best = null, bestLen = 0;
    for (const n of set) for (const tok of norm(n).split(/[^a-z0-9]+/)) {
      if (tok.length >= 3 && !_CP_STOP.has(tok) && qn.includes(tok) && tok.length > bestLen) { best = n; bestLen = tok.length; }
    }
    return best;
  }

  // Pés já produzidos (Closed = footage inteira; senão o parcial) e o RESTANTE a cavar.
  const _effDone = (t) => t.status === 'Closed' ? (t.footage || 0) : (t.completedFeet || 0);
  const remainingFt = (t) => Math.max(0, (t.footage || 0) - _effDone(t));
  // Similaridade de string (0..1) via Levenshtein — robustez a nomes mal transcritos por voz.
  function _lev(a, b) {
    const m = a.length, n = b.length; if (!m) return n; if (!n) return m;
    let prev = Array.from({ length: n + 1 }, (_, j) => j), cur = new Array(n + 1);
    for (let i = 1; i <= m; i++) {
      cur[0] = i;
      for (let j = 1; j <= n; j++) cur[j] = Math.min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (a[i - 1] === b[j - 1] ? 0 : 1));
      [prev, cur] = [cur, prev];
    }
    return prev[n];
  }
  const _sim = (a, b) => { const L = Math.max(a.length, b.length); return L ? 1 - _lev(a, b) / L : 1; };
  // "Você quis dizer": melhor projeto por similaridade fonética (só sugestão, não auto-corrige).
  function fuzzyProjectSuggestion(qn) {
    const toks = qn.split(/[^a-z0-9]+/).filter(w => w.length >= 4 && !/^\d+$/.test(w));
    if (!toks.length) return null;
    let best = null, bestSim = 0;
    for (const p of getProjects()) {
      for (const nt of norm(p.name).split(/[^a-z0-9]+/)) {
        if (nt.length < 4 || /^\d+$/.test(nt)) continue;
        for (const qt of toks) { const s = _sim(qt, nt); if (s > bestSim) { bestSim = s; best = p; } }
      }
    }
    return bestSim >= 0.72 ? best : null;
  }

  // ── Helpers do LOTE 2 ──
  // Contatos (telefone/email) de uma utility, opcionalmente filtrando por estado.
  function utilContactsFor(utilName, state) {
    const un = norm(utilName);
    const arr = (typeof utilContacts !== 'undefined' && Array.isArray(utilContacts)) ? utilContacts : [];
    const em = (c) => (typeof _contactEmail === 'function') ? _contactEmail(c) : (c.email || '');
    return arr.filter(c => {
      const cn = norm(c.utility_name || '');
      const nameOk = cn && (cn === un || cn.includes(un) || un.includes(cn));
      const stOk = !state || !c.state || norm(c.state) === norm(state);
      return nameOk && stOk;
    }).map(c => ({ utility: c.utility_name, contact: c.contact_name || '', phones: [c.phone_main, c.phone_alt, c.phone_emergency].filter(Boolean), email: em(c) }));
  }
  const fmtContact = (c) => `${c.utility}${c.contact ? ` (${c.contact})` : ''}: ${c.phones.length ? c.phones.join(' / ') : 'sem telefone'}${c.email ? `, ${c.email}` : ''}`;
  // Nome de utility citado na pergunta, entre TODAS as utilities conhecidas (contatos + pendências).
  function matchAnyUtility(qn) {
    const names = new Set();
    (typeof utilContacts !== 'undefined' ? utilContacts : []).forEach(c => c.utility_name && names.add(c.utility_name));
    getTickets().forEach(t => ((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).forEach(u => { const n = u.utility_name || u.name; if (n) names.add(n); }));
    let best = null, bl = 0;
    for (const n of names) for (const tok of norm(n).split(/[^a-z0-9]+/)) {
      if (tok.length >= 3 && !_UTIL_STOP.has(tok) && qn.includes(tok) && tok.length > bl) { best = n; bl = tok.length; }
    }
    return best;
  }
  const isFiber = (name) => (typeof isFiberUtility === 'function') ? isFiberUtility(name, '') : false;
  // Tickets clareados numa janela de dias (0=hoje) via _clearEventTimes (fonte dos cards).
  function clearedInWindow(days) {
    if (typeof _clearEventTimes !== 'function') return [];
    const now = new Date(); const start = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime() - days * 86400000;
    return getTickets().filter(t => (_clearEventTimes(t) || []).some(ts => ts >= start));
  }

  // ── Saudação personalizada ──
  let userName = ''; try { userName = localStorage.getItem('jarvis-username') || ''; } catch (e) {}
  let greeted = false, greetScheduled = false, awaitingName = false;
  const greetTime = () => { const h = new Date().getHours(); return h < 12 ? 'Bom dia' : h < 18 ? 'Boa tarde' : 'Boa noite'; };
  const capName = (s) => {
    let n = norm(s).replace(/^(meu nome (?:e|eh)|me chame de|pode me chamar de|sou o |sou a |eu sou )\s*/, '').trim();
    n = (n.split(' ')[0] || '').trim();
    return n ? n.charAt(0).toUpperCase() + n.slice(1) : '';
  };
  function systemOverview() {
    const vt = visibleTickets();
    const openN = vt.filter(t => effStatus(t) === 'Open').length;
    const clearN = vt.filter(t => effStatus(t) === 'Clear').length;
    const activeP = getProjects().filter(p => p.status !== 'Completed').length;
    return `Temos ${activeP} projetos ativos, ${openN} tickets em aberto e ${clearN} clear, com ${fmt(clearAvailableFt(vt))} pés liberados pra trabalhar. Quer o detalhe de algum estado ou projeto?`;
  }
  function doGreeting() {
    if (greeted) return;
    greeted = true;
    // Relê o nome do localStorage (pode ter sido salvo DEPOIS do módulo carregar)
    if (!userName) { try { userName = localStorage.getItem('jarvis-username') || ''; } catch (e) {} }
    // Se ainda não há nome, deriva do e-mail do usuário logado (login por conta).
    if (!userName && typeof currentUserEmail === 'string' && currentUserEmail) {
      const base = currentUserEmail.split('@')[0].split(/[._+\-]/)[0];
      if (base && !/^engineering$/i.test(base)) userName = base.charAt(0).toUpperCase() + base.slice(1);
    }
    if (el.panel) el.panel.classList.add('jv-open');
    let txt;
    if (userName) { txt = `${greetTime()}, ${userName}. Gostaria de uma atualização dos projetos?`; lastIntent = 'greet'; }
    else { awaitingName = true; txt = `${greetTime()}. Como devo chamá-lo?`; }
    push(txt, 'bot'); speak(txt);
  }

  // ── Cérebro read-only: interpreta a pergunta e monta a resposta ─────────
  function answer(q) {
    const qn = norm(q);
    if (!qn) return { text: 'Diga o que você quer saber, senhor.' };
    const has = (...ws) => ws.some(w => qn.includes(w));

    // Definir/trocar o nome do usuário ("me chame de X", "meu nome é X")
    if (/meu nome|me chame|chamar de/.test(qn)) {
      const nm = capName(q);
      if (nm) { userName = nm; try { localStorage.setItem('jarvis-username', nm); } catch (e) {} return { text: `Anotado, ${nm}. Como posso ajudar com os projetos?` }; }
    }

    const namedProj = findProject(qn);
    if (namedProj) lastProject = namedProj;   // memoriza pra follow-ups

    // Resposta afirmativa logo após a saudação → panorama do sistema
    if (lastIntent === 'greet' && /^(sim|quero|pode|claro|isso|manda|bora|vai|gostaria|por favor|pode ser|quero sim|atualiza)/.test(qn)) {
      lastIntent = null;
      return { text: systemOverview() };
    }
    // Panorama geral pedido explicitamente (sem projeto/estado específico)
    if (has('atualizacao', 'novidade', 'panorama', 'visao geral', 'como esta o sistema', 'como estao os projetos')
        && !namedProj && !/\b(fl|in|il|wi)\b/.test(qn)) {
      return { text: systemOverview() };
    }

    // NAVEGAÇÃO (read-only — só troca de tela, não altera nada): "abra a página de tickets"
    if (typeof nav === 'function' && /\b(abra|abre|abrir|va|vai|ir|mostra|mostrar|navega|navegar|leva|pagina|tela|aba|abrir a)\b/.test(qn)) {
      const PAGES = [
        { kw: ['ticket'], page: 'tickets', nome: 'Tickets' },
        { kw: ['projeto'], page: 'proj', nome: 'Projetos' },
        { kw: ['dashboard', 'painel', 'inicio', 'home'], page: 'dash', nome: 'Dashboard' },
        { kw: ['mapa'], page: 'map', nome: 'Mapa' },
        { kw: ['contato'], page: 'contacts', nome: 'Contatos' },
        { kw: ['analytics', 'analise', 'grafico', 'estatistic'], page: 'analytics', nome: 'Analytics' },
        { kw: ['historico', 'concluid'], page: 'completed', nome: 'Histórico' },
      ];
      for (const P of PAGES) {
        if (P.kw.some(k => qn.includes(k))) {
          // "abrir PROJETO X" (específico) não é a página de Projetos → deixa o dispatch de projeto tratar
          if (P.page === 'proj' && namedProj) break;
          // "abre o TICKET 123" (nº específico) → deep-link (L17), não a página de Tickets
          if (P.page === 'tickets' && findTicket(qn)) continue;
          try { nav(P.page); } catch (e) {}
          return { text: `Pronto, abri a página de ${P.nome}, senhor.` };
        }
      }
    }

    // ═══ KPIs / consultas amplas ═══

    // K1) Progresso / percentual do projeto
    if ((has('progresso', 'percentual', 'porcentagem', 'por cento', 'adiantad', 'quanto ja', 'quanto do projeto', 'quanto foi feito', 'quanto ta feito') || /quanto falta.*(terminar|concluir|acabar|pronto|100|fechar)/.test(qn)) && (lastProject || namedProj)) {
      const p = namedProj || lastProject;
      const ts = ticketsOf(p.id);
      const total = p.totalFeet || ts.reduce((s, t) => s + (t.footage || 0), 0) || 1;
      const campoFt = ts.reduce((s, t) => s + (t.completedFeet || 0), 0);
      const clearFt = clearAvailableFt(ts);
      const openFt = ts.filter(t => effStatus(t) === 'Open').reduce((s, t) => s + (t.footage || 0), 0);
      const pctCampo = Math.round(campoFt / total * 100), pctClear = Math.round(clearFt / total * 100);
      return { text: `Projeto ${p.name}: ${fmt(total)} pés no total. Já produzidos em campo ${fmt(campoFt)} pés, ${pctCampo}%. Liberados aguardando execução ${fmt(clearFt)} pés, ${pctClear}%. Ainda em aberto ${fmt(openFt)} pés.` };
    }

    // K2) Ranking / superlativo entre projetos ("qual projeto tem mais pés liberados")
    if (!namedProj && /\bprojeto\b/.test(qn) && has('mais', 'maior', 'top', 'menos', 'menor', 'ranking')) {
      const asc = has('menos', 'menor');
      let metric, mlabel, fmtV;
      if (has('liberad', 'clear', 'disponivel', 'trabalhar')) { metric = p => clearAvailableFt(ticketsOf(p.id)); mlabel = 'pés liberados'; fmtV = v => fmt(v) + ' pés'; }
      else if (has('aberto', 'open', 'pendente')) { metric = p => ticketsOf(p.id).filter(t => effStatus(t) === 'Open').length; mlabel = 'tickets abertos'; fmtV = v => v + ' abertos'; }
      else if (has('dano', 'damage')) { metric = p => ticketsOf(p.id).filter(t => effStatus(t) === 'Damage').length; mlabel = 'tickets com dano'; fmtV = v => v + ' com dano'; }
      else if (has('adiantad', 'progresso', 'concluid', 'pronto')) { metric = p => { const ts = ticketsOf(p.id); const tot = p.totalFeet || ts.reduce((s, t) => s + (t.footage || 0), 0) || 1; return ts.reduce((s, t) => s + (t.completedFeet || 0), 0) / tot; }; mlabel = 'progresso'; fmtV = v => Math.round(v * 100) + '%'; }
      else if (has('atrasad')) { metric = p => ticketsOf(p.id).filter(t => effStatus(t) === 'Open').reduce((s, t) => s + (t.footage || 0), 0); mlabel = 'pés em aberto'; fmtV = v => fmt(v) + ' pés em aberto'; }
      else if (has('ticket')) { metric = p => ticketsOf(p.id).length; mlabel = 'tickets'; fmtV = v => v + ' tickets'; }
      else { metric = p => ticketsOf(p.id).reduce((s, t) => s + (t.footage || 0), 0); mlabel = 'pés'; fmtV = v => fmt(v) + ' pés'; }
      const proj = getProjects().filter(p => p.status !== 'Completed' && (!lastState || norm(p.state) === lastState));
      if (!proj.length) return { text: 'Não há projetos ativos pra comparar, senhor.' };
      const ranked = proj.map(p => ({ p, v: metric(p) })).sort((a, b) => asc ? a.v - b.v : b.v - a.v).slice(0, 3);
      const top = ranked[0];
      const rest = ranked.slice(1).filter(r => r.v > 0 || asc).map(r => `${r.p.name} (${fmtV(r.v)})`);
      let txt = `${asc ? 'Menor' : 'Maior'} em ${mlabel}${lastState ? ' em ' + lastState.toUpperCase() : ''}: ${top.p.name}, com ${fmtV(top.v)}.`;
      if (rest.length) txt += ` Depois: ${rest.join(', ')}.`;
      return { text: txt };
    }

    // K3) Cliente / Prime ("quantos tickets da Duke Energy", "projetos do cliente X")
    {
      const cp = matchClientOrPrime(qn);
      if (cp && !has('falta', 'pendente', 'utilit', 'liberar', 'vence', 'vencimento')
          && (has('cliente', 'client', 'prime', 'quantos') || (has('ticket', 'projeto') && /\b(da|do|de|dos|das)\b/.test(qn)))) {
        const sc = currentScope();
        const tks = sc.ts.filter(t => t.client === cp || t.prime === cp);
        const scopeTxt = sc.label !== 'no sistema' ? ' ' + sc.label : '';
        if (has('projeto') && !has('ticket')) {
          const projs = [...new Set(tks.map(t => t.projectId))].map(id => getProjects().find(p => p.id === id)).filter(Boolean);
          return { text: `${cp} aparece em ${projs.length} ${projs.length === 1 ? 'projeto' : 'projetos'}${scopeTxt}${projs.length ? ': ' + projs.slice(0, 10).map(p => p.name).join(', ') : ''}.` };
        }
        const openN = tks.filter(t => effStatus(t) === 'Open').length, clearN = tks.filter(t => effStatus(t) === 'Clear').length;
        return { text: `${cp}: ${nTk(tks.length)}${scopeTxt}, sendo ${openN} em aberto e ${clearN} clear.` };
      }
    }

    // K4) Localização / endereço de um ticket (\bonde\b pra não casar em "resp-onde-u")
    if (/\bonde\b|endereco|localizacao|\blocal\b|\bfica\b|em que (cidade|lugar|rua)/.test(qn)) {
      const tk = findTicket(qn) || lastTicket;
      if (tk) {
        lastTicket = tk;
        const loc = tk.location || '', addr = tk.address || '';
        if (!loc && !addr) return { text: `O ticket ${tk.ticket} não tem localização cadastrada, senhor.` };
        return { text: `O ticket ${tk.ticket} fica em ${[addr, loc].filter(Boolean).join(', ')}.` };
      }
    }

    // K5) TODAS as utilities de um ticket (não só as pendentes)
    if (has('utilit', 'utilidade') && has('todas', 'respond', 'marcad', 'ja responder', 'lista as', 'quais as', 'situacao das')) {
      const tk = findTicket(qn) || lastTicket;
      if (tk) {
        lastTicket = tk;
        const us = (typeof getTicketUtils === 'function') ? getTicketUtils(tk.ticket) : [];
        if (!us.length) return { text: `O ticket ${tk.ticket} não tem utilities registradas no sistema.` };
        const parts = us.slice(0, 15).map(u => `${u.utility_name || u.name}: ${traduzStatus(u.status)}`);
        let txt = `Ticket ${tk.ticket}, ${us.length} utilities: ${parts.join('; ')}`;
        if (us.length > 15) txt += `; e mais ${us.length - 15}`;
        return { text: txt + '.' };
      }
    }

    // K6) Vencimento por JANELA / vencidos ("quais tickets vencem em 7 dias", "tickets vencidos")
    {
      const expired = has('vencido', 'vencidos', 'atrasad', 'venceram', 'ja venceu');
      const clearOnly = /\bclear\b|liberad|pra cavar|pra trabalhar/.test(qn);
      let win = null;
      const dm = qn.match(/(\d+)\s*dias?/); if (dm) win = parseInt(dm[1], 10);
      else if (has('semana')) win = 7; else if (has('mes')) win = 30; else if (/amanha/.test(qn)) win = 1; else if (/\bhoje\b/.test(qn)) win = 0;
      const superl = /primeiro a vencer|proximo a vencer|proximo vencimento|vence primeiro|mais proximo de vencer|mais perto de vencer|qual vence primeiro|primeiro que vence/.test(qn);
      const wantWindow = has('vencend', 'a vencer', 'vao vencer', 'vencem', 'expiram', 'vencer', 'vao expirar');
      // Superlativo: qual vence primeiro (o mais próximo, ainda não vencido)
      if (superl) {
        const sc = currentScope();
        const fut = sc.ts.filter(t => { const d = daysToExpire(t); return d !== null && d >= 0 && effStatus(t) !== 'Closed' && (!clearOnly || effStatus(t) === 'Clear'); }).sort((a, b) => _dexp(a) - _dexp(b));
        if (!fut.length) return { text: `Nenhum ticket a vencer ${sc.label}, senhor.` };
        const t = fut[0], d = daysToExpire(t);
        lastTicket = t;
        return { text: `O próximo a vencer ${sc.label} é o ticket ${t.ticket}, ${d === 0 ? 'que vence hoje' : `em ${d} dias`} (${t.expire}).` };
      }
      if (expired || wantWindow || win !== null) {
        const sc = currentScope();
        let sel;
        if (expired) sel = sc.ts.filter(t => { const d = daysToExpire(t); return d !== null && d < 0 && effStatus(t) !== 'Closed' && (!clearOnly || effStatus(t) === 'Clear'); });
        else { const N = win === null ? 7 : win; sel = sc.ts.filter(t => { const d = daysToExpire(t); return d !== null && d >= 0 && d <= N && effStatus(t) !== 'Closed' && (!clearOnly || effStatus(t) === 'Clear'); }); }
        sel.sort((a, b) => _dexp(a) - _dexp(b));
        lastList = sel.slice(); lastIntent = 'list';
        const cl = clearOnly ? 'clear ' : '';
        if (!sel.length) return { text: expired ? `Nenhum ticket ${cl}vencido ${sc.label}, senhor.` : `Nenhum ticket ${cl}vencendo ${sc.label} nesse prazo.` };
        const parts = sel.slice(0, 12).map(t => { const d = daysToExpire(t); return `${t.ticket} (${d < 0 ? 'venceu há ' + Math.abs(d) + 'd' : d === 0 ? 'hoje' : 'em ' + d + 'd'})`; });
        let txt = expired ? `${nTk(sel.length)} ${cl}vencido(s) ${sc.label}: ${parts.join('; ')}` : `${nTk(sel.length)} ${cl}vencendo ${sc.label} em ${win === null ? 7 : win} dias: ${parts.join('; ')}`;
        if (sel.length > 12) txt += `; e mais ${sel.length - 12}`;
        return { text: txt + '.' };
      }
    }

    // K7) Veredito "posso cavar?" (Clear + válido + sem pendência num só sim/não)
    if (/posso (cavar|escavar)|pode (cavar|escavar)|\bja pode\b|pode meter|coberto pra|liberado pra (escavar|cavar)|da pra (cavar|escavar)/.test(qn)) {
      const t = findTicket(qn) || lastTicket;
      if (!t) return { text: 'De qual ticket, senhor? Diga o número.' };
      lastTicket = t;
      const st = effStatus(t), days = daysToExpire(t);
      const pn = [...new Set(((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).map(u => u.utility_name || u.name).filter(Boolean))];
      if (st === 'Clear' && (days === null || days >= 0) && !pn.length) {
        return { text: `Sim, senhor. Pode cavar o ticket ${t.ticket} — está liberado${days !== null ? (days === 0 ? ', mas vence hoje' : `, vence em ${days} dias`) : ''}.` };
      }
      const motivos = [];
      if (st !== 'Clear') motivos.push(`está ${traduzStatus(st)}`);
      if (pn.length) motivos.push(pn.length === 1 ? `falta a ${pn[0]}` : `faltam ${pn.length} utilities`);
      if (days !== null && days < 0) motivos.push(`venceu há ${Math.abs(days)} dias`);
      return { text: `Não, senhor. O ticket ${t.ticket} ${motivos.join(', ') || 'ainda não está liberado'}.` };
    }

    // K8) Quanto falta CAVAR neste ticket (footage − concluído = restante)
    if ((has('cavar', 'produzir', 'executar', 'restante') || /falta.*(pe|pes|footage|metragem|cavar|produzir)/.test(qn))
        && (findTicket(qn) || (lastTicket && !findProject(qn)))) {
      const t = findTicket(qn) || lastTicket;
      lastTicket = t;
      const rem = remainingFt(t), done = _effDone(t), tot = t.footage || 0;
      if (!tot) return { text: `O ticket ${t.ticket} não tem metragem cadastrada, senhor.` };
      if (rem === 0) return { text: `O ticket ${t.ticket} já está todo produzido, senhor — ${fmt(tot)} pés concluídos.` };
      return { text: `No ticket ${t.ticket} faltam ${fmt(rem)} pés pra cavar, de ${fmt(tot)} no total${done ? ` (${fmt(done)} já feitos)` : ''}.` };
    }

    // K9) Campos do ticket: dono, notas, linha particular, trava, resposta de utility
    {
      const t = findTicket(qn) || lastTicket;
      if (t) {
        if (has('de quem', 'cliente', 'prime', 'contratada', 'empresa executora') && !findProject(qn)) {
          lastTicket = t;
          return { text: `O ticket ${t.ticket} é do cliente ${t.client || 'não informado'}${t.prime ? `, prime ${t.prime}` : ''}${t.company ? `, empresa ${t.company}` : ''}.` };
        }
        if (has('nota', 'notas', 'observ', 'anotac', 'escrito no')) {
          lastTicket = t;
          return { text: t.notes ? `Nota do ticket ${t.ticket}: ${t.notes}` : `O ticket ${t.ticket} não tem nota registrada, senhor.` };
        }
        if (/particular|privada|\bprivate\b|linha propria/.test(qn)) {
          lastTicket = t;
          const priv = ((typeof getTicketUtils === 'function') ? getTicketUtils(t.ticket) : []).filter(u => u.status === 'Private').map(u => u.utility_name || u.name);
          return { text: priv.length ? `O ticket ${t.ticket} tem ${priv.length === 1 ? '1 linha particular' : priv.length + ' linhas particulares'}: ${priv.join(', ')}.` : `O ticket ${t.ticket} não tem linhas particulares marcadas, senhor.` };
        }
        if (/travad|bloquead|trava manual|preso manual|cadeado/.test(qn)) {
          lastTicket = t;
          return { text: t.status_locked ? `Sim, o ticket ${t.ticket} está travado manualmente 🔒 — a automação 811 não sobrescreve.` : `Não, o ticket ${t.ticket} não está travado — segue a automação normal.` };
        }
        if (has('respond', 'disse', 'falou', 'resposta')) {
          const us = (typeof getTicketUtils === 'function') ? getTicketUtils(t.ticket) : [];
          let u = null, bl = 0;
          for (const x of us) for (const tok of norm(x.utility_name || x.name || '').split(/[^a-z0-9]+/)) {
            if (tok.length >= 3 && !_UTIL_STOP.has(tok) && qn.includes(tok) && tok.length > bl) { u = x; bl = tok.length; }
          }
          if (u) { lastTicket = t; return { text: `No ticket ${t.ticket}, a ${u.utility_name || u.name} está ${traduzStatus(u.status)}${u.response_text ? `: ${u.response_text}` : ''}.` }; }
        }
      }
    }

    // K10) Busca por CONDADO / cidade
    {
      let name = null, isCounty = false, bl = 0;
      const CG_STOP = new Set(['county', 'condado', 'cidade', 'the', 'of']);
      for (const t of getTickets()) for (const [val, kind] of [[t.county, true], [t.location, false]]) {
        if (!val) continue;
        const nv = norm(val);
        if (nv.length >= 3 && !CG_STOP.has(nv) && qn.includes(nv) && nv.length > bl) { name = val; isCounty = kind; bl = nv.length; }
      }
      if (name && !has('atende', 'atendem', 'cobre', 'cobrem', 'responde por') && (has('county', 'condado', 'cidade') || has('quantos', 'quais', 'pes', 'liberad', 'aberto', 'tickets', 'o que'))) {
        const ts = visibleTickets().filter(t => isCounty ? t.county === name : t.location === name);
        const openN = ts.filter(t => effStatus(t) === 'Open').length, clearN = ts.filter(t => effStatus(t) === 'Clear').length;
        return { text: `Em ${name}${isCounty ? ' county' : ''}: ${nTk(ts.length)}, ${openN} em aberto e ${clearN} clear, com ${fmt(clearAvailableFt(ts))} pés liberados pra trabalhar.` };
      }
    }

    // K11) Ajuda / capacidades
    if (/o que voce (sabe|faz|pode)|quais perguntas|me ajuda|como te (uso|usar)|o que da pra perguntar|o que posso perguntar|\bcomandos\b/.test(qn)) {
      return { text: 'Posso te dizer, senhor: quanto tem liberado pra trabalhar num projeto, quantos tickets abertos ou clear, o status, a metragem e o que falta cavar de um ticket, quais utilities faltam, o que vence e o que já venceu, se pode cavar num ticket, quem é o cliente, notas, e relatórios por projeto, estado ou cliente. É só perguntar por voz ou digitar.' };
    }

    // ═══ LOTE 2 ═══

    // L1/L2) Contatos de utility / "pra quem ligar pra destravar este ticket"
    if (has('telefone', 'contato', 'ligar', 'numero da', 'email', 'e-mail', 'fone') || /pra quem (eu )?ligo/.test(qn)) {
      const tRef = findTicket(qn);
      if ((/destrav|pra liberar|pendencia|pendente|segura/.test(qn) || (tRef && !matchAnyUtility(qn))) && (tRef || lastTicket)) {
        const t = tRef || lastTicket; lastTicket = t;
        const pend = [...new Set(((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).map(u => u.utility_name || u.name).filter(Boolean))];
        if (!pend.length) return { text: `O ticket ${t.ticket} não tem utilities pendentes, senhor — nada pra cobrar.` };
        const lines = pend.map(n => { const cs = utilContactsFor(n, t.state); return cs.length ? fmtContact(cs[0]) : `${n}: sem contato cadastrado`; });
        return { text: `Pra destravar o ticket ${t.ticket}, ligue pra: ${lines.join('; ')}.` };
      }
      const uName = matchAnyUtility(qn);
      if (uName) {
        const st = (qn.match(/\b(fl|in|il|wi)\b/) || [])[1];
        const cs = utilContactsFor(uName, st);
        if (!cs.length) return { text: `Não achei contato cadastrado pra ${uName}${st ? ' em ' + st.toUpperCase() : ''}, senhor.` };
        return { text: cs.slice(0, 3).map(fmtContact).join('. ') + '.' };
      }
    }

    // L18) Cobertura utility×county ("quais utilities atendem Orange county")
    if ((has('atende', 'atendem', 'cobre', 'cobrem', 'responde por', 'quem cobre')) && typeof utilCoverage !== 'undefined' && utilCoverage.length) {
      let cty = null, bl = 0;
      for (const c of utilCoverage) { const nv = norm(c.county || ''); if (nv.length >= 3 && qn.includes(nv) && nv.length > bl) { cty = c.county; bl = nv.length; } }
      if (cty) {
        const st = (qn.match(/\b(fl|in|il|wi)\b/) || [])[1];
        const us = [...new Set(utilCoverage.filter(c => c.county === cty && (!st || norm(c.state) === st)).map(c => c.utility_name).filter(Boolean))];
        return { text: us.length ? `${us.length} utilities atendem ${cty} county: ${us.slice(0, 12).join(', ')}${us.length > 12 ? ', e mais ' + (us.length - 12) : ''}.` : `Não tenho cobertura cadastrada pra ${cty} county, senhor.` };
      }
    }

    // L6) No-show / 2º aviso
    if (/no.?show|segundo aviso|\b2.? aviso|nao apareceu|nao compareceu/.test(qn)) {
      const single = findTicket(qn) || (lastTicket && !/quais|quantos|todos|lista/.test(qn) ? lastTicket : null);
      if (single) {
        lastTicket = single;
        const rel = (typeof _isNoShowReleased === 'function') && _isNoShowReleased(single);
        const sns = (typeof getSecondNotices === 'function') ? getSecondNotices(single) : [];
        if (rel) return { text: `Sim, o ticket ${single.ticket} está liberado por no-show (fibra não compareceu).` };
        if (sns.length) return { text: `O ticket ${single.ticket} tem ${sns.length} no-show(s): ${[...new Set(sns.map(s => s.utility))].join(', ')}.` };
        return { text: `O ticket ${single.ticket} não tem no-show registrado, senhor.` };
      }
      const sc = currentScope();
      const rel = sc.ts.filter(x => (typeof _isNoShowReleased === 'function') && _isNoShowReleased(x));
      lastList = rel.slice(); lastIntent = 'list';
      return { text: rel.length ? `${nTk(rel.length)} liberado(s) por no-show ${sc.label}: ${rel.slice(0, 12).map(x => x.ticket).join(', ')}${rel.length > 12 ? ', e mais ' + (rel.length - 12) : ''}.` : `Nenhum ticket liberado por no-show ${sc.label}, senhor.` };
    }

    // L7) Carência de renovação
    if (/carencia|em graca|periodo de graca|\bgrace\b/.test(qn)) {
      const single = findTicket(qn) || (lastTicket && !/quais|quantos|todos|lista/.test(qn) ? lastTicket : null);
      if (single) {
        lastTicket = single;
        const inG = (typeof isInRenewalGrace === 'function') && isInRenewalGrace(single);
        if (!inG) return { text: `O ticket ${single.ticket} não está em carência, senhor.` };
        const cut = (typeof graceCutoverDate === 'function') ? graceCutoverDate(single) : '—';
        return { text: `Sim, o ticket ${single.ticket} está em carência de renovação${cut && cut !== '—' ? `, até ${cut}` : ''}.` };
      }
      const sc = currentScope();
      const inG = sc.ts.filter(x => (typeof isInRenewalGrace === 'function') && isInRenewalGrace(x));
      lastList = inG.slice(); lastIntent = 'list';
      return { text: inG.length ? `${nTk(inG.length)} em carência ${sc.label}: ${inG.slice(0, 12).map(x => x.ticket).join(', ')}${inG.length > 12 ? ', e mais ' + (inG.length - 12) : ''}.` : `Nenhum ticket em carência ${sc.label}, senhor.` };
    }

    // L8) Renovação — ticket antigo
    if (/renovad|renova[cç]ao|numero antigo|ticket antigo|veio de qual|era qual ticket/.test(qn)) {
      const t = findTicket(qn) || lastTicket;
      if (t) {
        lastTicket = t;
        const old = ((t.oldTicket2 || t.old_ticket2) || '').split(' → ')[0].trim();
        if (!old) return { text: `O ticket ${t.ticket} não é uma renovação, senhor.` };
        const so = (t.statusOld || t.status_old || '').trim();
        return { text: `Sim, o ticket ${t.ticket} é renovação do antigo ${old}${so ? ` (estava ${traduzStatus(so)})` : ''}.` };
      }
    }

    // L9) Main line vs Service
    if (/main.?line|linha principal|\bservice\b|servi[cç]o de linha/.test(qn) && !/lista de servi|ordem de servi/.test(qn)) {
      const single = findTicket(qn) || (lastTicket && has('esse', 'este') ? lastTicket : null);
      if (single && has('esse', 'este', 'e main', 'e service', 'que tipo')) { lastTicket = single; return { text: `O ticket ${single.ticket} é ${single.tipo || 'sem tipo definido'}.` }; }
      const wantMain = /main.?line|linha principal/.test(qn);
      const sc = currentScope();
      const sel = sc.ts.filter(t => { const tp = norm(t.tipo); return wantMain ? /main/.test(tp) : /service|servico/.test(tp); });
      lastList = sel.slice(); lastIntent = 'list';
      return { text: `${nTk(sel.length)} de ${wantMain ? 'main line' : 'service'} ${sc.label}, somando ${fmt(sel.reduce((s, t) => s + (t.footage || 0), 0))} pés.` };
    }

    // L10) Job number (busca por job alfanumérico OU job de um ticket)
    if (/\bjob\b|numero do job/.test(qn)) {
      const byNum = findTicket(qn);
      const jm = qn.match(/\bjob\s*#?\s*([a-z]?\d[a-z0-9]*)/);
      if (jm && !byNum) {
        const jn = jm[1]; const t = getTickets().find(x => norm(x.job) === jn || norm(x.job).includes(jn));
        return { text: t ? `O job ${jn.toUpperCase()} está no ticket ${t.ticket}.` : `Não achei o job ${jn.toUpperCase()}, senhor.` };
      }
      const t = byNum || lastTicket;
      if (t) { lastTicket = t; return { text: t.job ? `O job do ticket ${t.ticket} é ${t.job}.` : `O ticket ${t.ticket} não tem job cadastrado, senhor.` }; }
    }

    // L11) Enumerar clientes / primes
    if (/quais (os |as )?(clientes|primes)|lista(r)? (os|as) (clientes|primes)|meus clientes|quantos clientes|quais primes/.test(qn)) {
      const isPrime = /prime/.test(qn);
      const set = [...new Set(getTickets().map(t => isPrime ? t.prime : t.client).filter(Boolean))].sort();
      return { text: set.length ? `${set.length} ${isPrime ? 'primes' : 'clientes'}: ${set.slice(0, 15).join(', ')}${set.length > 15 ? ', e mais ' + (set.length - 15) : ''}.` : `Nenhum ${isPrime ? 'prime' : 'cliente'} cadastrado, senhor.` };
    }

    // L12) Rollup consolidado do sistema
    if (/(progresso|panorama|resumo|visao) (geral|do sistema|da empresa)|quantos projetos|empresa toda|no sistema todo/.test(qn) && !findProject(qn)) {
      const vt = visibleTickets();
      const active = getProjects().filter(p => p.status !== 'Completed').length, done = getProjects().filter(p => p.status === 'Completed').length;
      const openN = vt.filter(t => effStatus(t) === 'Open').length;
      return { text: `Sistema: ${active} projetos ativos${done ? ` e ${done} concluídos` : ''}. ${openN} tickets em aberto com ${fmt(vt.filter(t => effStatus(t) === 'Open').reduce((s, t) => s + (t.footage || 0), 0))} pés, e ${fmt(clearAvailableFt(vt))} pés liberados pra trabalhar.` };
    }

    // L13) Ranking entre ESTADOS
    if (/\bestado\b/.test(qn) && has('mais', 'maior', 'menos', 'menor')) {
      const asc = has('menos', 'menor');
      const clr = has('liberad', 'clear', 'trabalhar'), op = has('aberto', 'open'), dm = has('dano', 'damage');
      const metric = clr ? (ts) => clearAvailableFt(ts) : op ? (ts) => ts.filter(t => effStatus(t) === 'Open').length : dm ? (ts) => ts.filter(t => effStatus(t) === 'Damage').length : (ts) => ts.length;
      const lbl = clr ? 'pés liberados' : op ? 'tickets abertos' : dm ? 'com dano' : 'tickets';
      const ranked = ['FL', 'IN', 'IL', 'WI', 'KY', 'GA'].map(st => ({ st, v: metric(visibleTickets().filter(t => norm(t.state) === norm(st))) })).sort((a, b) => asc ? a.v - b.v : b.v - a.v);
      return { text: `Em ${lbl}: ${ranked.map(r => `${r.st} ${fmt(r.v)}`).join(', ')}.` };
    }

    // L14) Ranking de clientes / primes
    if (/\b(cliente|prime)s?\b/.test(qn) && has('mais', 'maior', 'menos', 'menor')) {
      const isPrime = /prime/.test(qn), asc = has('menos', 'menor');
      const groups = {};
      visibleTickets().forEach(t => { const k = isPrime ? t.prime : t.client; if (k) (groups[k] = groups[k] || []).push(t); });
      const dm = has('dano', 'damage'), clr = has('liberad', 'clear'), op = has('entregar', 'aberto', 'falta');
      const metric = dm ? (ts) => ts.filter(t => effStatus(t) === 'Damage').length : clr ? (ts) => clearAvailableFt(ts) : op ? (ts) => ts.filter(t => effStatus(t) === 'Open').reduce((s, t) => s + (t.footage || 0), 0) : (ts) => ts.reduce((s, t) => s + (t.footage || 0), 0);
      const ranked = Object.entries(groups).map(([k, ts]) => ({ k, v: metric(ts) })).sort((a, b) => asc ? a.v - b.v : b.v - a.v).slice(0, 3);
      if (ranked.length) return { text: `${asc ? 'Menor' : 'Maior'} ${isPrime ? 'prime' : 'cliente'}: ${ranked.map(r => `${r.k} (${fmt(r.v)})`).join(', ')}.` };
    }

    // L15) Gargalo: utility que mais atrasa no escopo
    if (/(qual|que).*(utilit|utilidade).*(mais|atrasa|segura|trava)|gargalo|utility que mais|mais atrasa|segura mais|trava mais/.test(qn)) {
      const sc = currentScope();
      const counts = {};
      sc.ts.forEach(t => ((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : []).forEach(u => { const n = u.utility_name || u.name; if (n) counts[n] = (counts[n] || 0) + 1; }));
      const arr = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 5);
      return { text: arr.length ? `Quem mais atrasa ${sc.label}: ${arr.map(([n, c]) => `${n} (${c})`).join(', ')}.` : `Nenhuma utility pendente ${sc.label}, senhor.` };
    }

    // L16) Ranking de vencimento por projeto
    if (/(qual|que) projeto.*(vence|venc|risco|prazo)|onde.*(risco|vencer)|mais (ticket|coisa) vencendo/.test(qn)) {
      const proj = getProjects().filter(p => p.status !== 'Completed' && (!lastState || norm(p.state) === lastState));
      const ranked = proj.map(p => ({ p, v: ticketsOf(p.id).filter(t => { const d = daysToExpire(t); return d !== null && d >= 0 && d <= 7 && effStatus(t) !== 'Closed'; }).length })).filter(r => r.v > 0).sort((a, b) => b.v - a.v).slice(0, 3);
      return { text: ranked.length ? `Mais tickets vencendo em 7 dias: ${ranked.map(r => `${r.p.name} (${r.v})`).join(', ')}.` : `Nenhum projeto com tickets vencendo nos próximos 7 dias, senhor.` };
    }

    // L4) O que renovar / obra em risco
    if (/o que.*renovar|preciso renovar|obra em risco|risco de (perder|vencer)|em risco de prazo|tickets? em risco/.test(qn)) {
      const sc = currentScope();
      const N = (qn.match(/(\d+)\s*dias?/) || [])[1] ? parseInt(qn.match(/(\d+)\s*dias?/)[1], 10) : (has('semana') ? 7 : 14);
      const sel = sc.ts.filter(t => { const d = daysToExpire(t); return d !== null && d >= 0 && d <= N && effStatus(t) !== 'Closed' && effStatus(t) !== 'Clear' && remainingFt(t) > 0; }).sort((a, b) => _dexp(a) - _dexp(b));
      lastList = sel.slice(); lastIntent = 'list';
      if (!sel.length) return { text: `Nada em risco ${sc.label} nos próximos ${N} dias, senhor.` };
      return { text: `${nTk(sel.length)} em risco ${sc.label} (vence em ${N}d, ainda em aberto): ${sel.slice(0, 10).map(t => `${t.ticket} (${daysToExpire(t)}d)`).join(', ')}${sel.length > 10 ? ', e mais ' + (sel.length - 10) : ''}. ${fmt(sel.reduce((s, t) => s + remainingFt(t), 0))} pés em risco.` };
    }

    // L3) Lista de serviço / rota do dia
    if (/lista de servi[cç]o|rota do dia|o que.*(cavar|fazer) (hoje|primeiro)|worklist|onde trabalhar hoje/.test(qn)) {
      const sc = currentScope();
      const sel = sc.ts.filter(t => effStatus(t) === 'Clear' && remainingFt(t) > 0).sort((a, b) => _dexp(a) - _dexp(b));
      lastList = sel.slice(); lastIntent = 'list';
      if (!sel.length) return { text: `Nenhum ticket liberado com pé pra cavar ${sc.label}, senhor.` };
      const parts = sel.slice(0, 8).map(t => { const d = daysToExpire(t); return `${t.ticket}${t.address ? ' — ' + t.address : ''}${t.location ? ', ' + t.location : ''} (${fmt(remainingFt(t))} pés${d !== null ? ', vence em ' + d + 'd' : ''})`; });
      return { text: `Lista de serviço ${sc.label}, por vencimento: ${parts.join('; ')}${sel.length > 8 ? '; e mais ' + (sel.length - 8) : ''}.` };
    }

    // L5) Clareados por período
    if (/(clarea|clearar|liberou|liberamos|libera[cç]|produziu|produzimos).*(hoje|semana|30|mes)|quanto.*(clarea|liberou|liberamos).*(hoje|semana|mes)|clareado/.test(qn)) {
      const days = /30|mes/.test(qn) ? 30 : has('semana') ? 7 : 0;
      const lbl = days === 0 ? 'hoje' : days === 7 ? 'nos últimos 7 dias' : 'nos últimos 30 dias';
      const cl = clearedInWindow(days);
      return { text: `${nTk(cl.length)} clareado(s) ${lbl}, somando ${fmt(cl.reduce((s, t) => s + Math.max(0, (t.footage || 0) - (t.completedFeet || 0)), 0))} pés liberados.` };
    }

    // L20) Dano — lista no escopo
    if (/quais|quantos|lista|tem/.test(qn) && has('dano', 'damage') && !/(qual|que) projeto/.test(qn)) {
      const sc = currentScope();
      const sel = sc.ts.filter(t => effStatus(t) === 'Damage');
      lastList = sel.slice(); lastIntent = 'list';
      return { text: sel.length ? `${nTk(sel.length)} com dano ${sc.label}: ${sel.slice(0, 12).map(t => t.ticket).join(', ')}${sel.length > 12 ? ', e mais ' + (sel.length - 12) : ''}.` : `Nenhum ticket com dano ${sc.label}, senhor.` };
    }

    // L17) Abrir um ticket específico (deep-link)
    if (/^(abr|abre|abrir|mostra|mostrar|ver o|abre o|mostra o)\b/.test(qn) && findTicket(qn) && typeof openTicketDetail === 'function') {
      const t = findTicket(qn); lastTicket = t;
      try { openTicketDetail(t.id); } catch (e) {}
      return { text: `Abri o ticket ${t.ticket}, senhor.` };
    }

    // A) Utilities pendentes — de UM ticket específico OU agregado do PROJETO
    //    Gatilhos: "utility/utilidade/util", "faltando", "o que falta pra liberar",
    //    "pendente"; continuidade "e do segundo".
    const utilWord = has('utilit', 'utilidade') || /\butil\b/.test(qn) || /\buteis\b/.test(qn);
    const missingPhrase = has('faltando')
      || /o que .*falt|falta (pra|para|de|em)? ?liberar|falta liberar|liberar (esse|este|o ticket|esse ticket)/.test(qn)
      || has('pendente', 'pendencia');
    const explicitTk = findTicket(qn) || resolveOrdinal(qn) !== null;
    const namedUtil = lastProject ? matchUtilityInQuery(lastProject, qn) : null;
    const utilTrig = utilWord
      || (missingPhrase && (explicitTk || lastTicket || lastProject))
      || (!!namedUtil && (has('ticket', 'espera', 'aguard', 'quais', 'liberar', 'pendente', 'falta') || explicitTk))
      || (lastIntent === 'util' && resolveOrdinal(qn) !== null);
    if (utilTrig) {
      // Alvo = ticket explícito (número/ordinal)?
      let t = findTicket(qn);
      if (!t) {
        const ord = resolveOrdinal(qn);
        if (ord !== null) {
          const wantOpen = /ser liberad|a liberar|pra liberar|para liberar|em aberto|\baberto|pendente|preso/.test(qn);
          const list = (wantOpen && lastProject) ? ticketsByStatus(lastProject, 'Open')
                     : (lastList && lastList.length) ? lastList
                     : (lastProject ? ticketsByStatus(lastProject, 'Open') : []);
          if (list.length) { const i = ord === 'last' ? list.length - 1 : ord; if (i >= 0 && i < list.length) t = list[i]; }
        }
      }
      // Citou um número de ticket (6+ dígitos) que NÃO existe → avisa (não usa o último ticket).
      if (!t) {
        const explicitNum = (qn.match(/\d{6,}/) || [])[0];
        if (explicitNum) {
          const asProj = findProject(explicitNum);
          if (asProj) return { text: `${explicitNum} é o número de um projeto (${asProj.name}), não de um ticket, senhor. Se quiser, pergunte "o que falta liberar no projeto ${asProj.name}".` };
          return { text: `Não encontrei o ticket ${explicitNum}, senhor.` };
        }
      }
      // Sem ticket explícito → escopo de PROJETO (agregar / filtrar por utility)
      if (!t && lastProject) {
        const uName = matchUtilityInQuery(lastProject, qn);
        const onlyWord = /\b(somente|apenas)\b|\bso\b|so a |so falta|so uma/.test(qn);
        const oneUtil = /(somente|so|apenas) (uma|1)\b|uma (so |unica )?utilit|falta (uma|so uma)\b|so falta uma|a uma utilit/.test(qn);
        const projScope = !!uName || oneUtil || has('projeto', 'abertos', 'aberto', 'todos', 'todas', 'segurando', 'faltam') || !lastTicket;
        if (projScope) {
          // (1) utility específica citada ("o que falta só a TECO")
          if (uName) {
            let tks = ticketsPendingUtility(lastProject, uName);
            if (onlyWord) tks = tks.filter(x => { const s = new Set(((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(x.ticket) : []).map(u => u.utility_name || u.name)); return s.size === 1 && s.has(uName); });
            lastList = tks.slice(); lastIntent = 'list';
            const nums = tks.map(x => x.ticket);
            if (!tks.length) return { text: onlyWord ? `Nenhum ticket do projeto ${lastProject.name} está esperando só a ${uName}.` : `Nenhum ticket do projeto ${lastProject.name} está esperando a ${uName}.` };
            let txt = `${nums.length === 1 ? '1 ticket' : `${nums.length} tickets`} ${onlyWord ? (nums.length === 1 ? 'falta' : 'faltam') + ` só a ${uName}` : (nums.length === 1 ? 'espera' : 'esperam') + ` a ${uName}`}: ${nums.slice(0, 15).join(', ')}`;
            if (nums.length > 15) txt += `, e mais ${nums.length - 15}`;
            return { text: txt + '.' };
          }
          // (2) "faltam só UMA utility" (qualquer) → tickets a 1 utility de liberar
          if (oneUtil) {
            const near = ticketsOf(lastProject.id).map(x => ({ t: x, p: [...new Set(((typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(x.ticket) : []).map(u => u.utility_name || u.name))] })).filter(o => o.p.length === 1);
            lastList = near.map(o => o.t); lastIntent = 'list';
            if (!near.length) return { text: `Nenhum ticket do projeto ${lastProject.name} está a apenas uma utility de liberar.` };
            const parts = near.slice(0, 12).map(o => `${o.t.ticket} (falta ${o.p[0]})`);
            let txt = `${near.length === 1 ? '1 ticket está' : `${near.length} tickets estão`} a uma utility de liberar: ${parts.join('; ')}`;
            if (near.length > 12) txt += `; e mais ${near.length - 12}`;
            return { text: txt + '.' };
          }
          // (3) agregado geral do projeto
          lastIntent = 'utilagg';
          return { text: aggregatePendingUtils(lastProject) };
        }
      }
      if (!t) t = lastTicket;
      if (!t) return { text: 'De qual ticket ou projeto, senhor? Diga o número do ticket, ou "quais utilidades faltam no projeto".' };
      lastTicket = t; lastIntent = 'util';
      // Chamar SEM o objeto — passar o objeto força getMergedUtils() (só serve renovados) e zera.
      const pend = (typeof getTicketPendingUtils === 'function') ? getTicketPendingUtils(t.ticket) : [];
      const names = [...new Set(pend.map(u => u.utility_name || u.name || u.utility).filter(Boolean))];
      if (!names.length) {
        const est = effStatus(t);
        if (est === 'Clear') return { text: `O ticket ${t.ticket} já está liberado, senhor. Nenhuma utility pendente.` };
        return { text: `O ticket ${t.ticket} está ${traduzStatus(est)}, mas nenhuma utility está marcada como pendente no sistema — provavelmente as utilities ainda não responderam.` };
      }
      if (names.length === 1) return { text: `No ticket ${t.ticket} falta a ${names[0]}.` };
      return { text: `No ticket ${t.ticket} faltam ${names.length} utilities: ${listWords(names)}.` };
    }

    // B) Vencimento / expiração
    if (has('vencimento', 'vence', 'vencem', 'expira', 'expiracao', 'validade', 'prazo')) {
      const tById = findTicket(qn);
      if (tById) {
        lastTicket = tById;
        if (!tById.expire) return { text: `O ticket ${tById.ticket} não tem vencimento cadastrado, senhor.` };
        const d = daysToExpire(tById);
        const ds = d === null ? '' : d < 0 ? `, venceu há ${Math.abs(d)} dias` : d === 0 ? ', vence hoje' : `, em ${d} dias`;
        return { text: `O ticket ${tById.ticket} vence em ${tById.expire}${ds}.` };
      }
      const st = statusFromQuery(qn);
      let set = null, lbl = '';
      if (st !== undefined && lastProject) { set = ticketsByStatus(lastProject, st); lbl = stLabel(st); }
      else if (lastList && lastList.length) { set = lastList; lbl = stLabel(lastScope); }
      else if (lastProject) { set = ticketsByStatus(lastProject, 'Clear'); lbl = 'clear'; }
      if (!set || !set.length) return { text: 'De quais tickets, senhor? Me diga um projeto ou status primeiro.' };
      lastList = set.slice();
      const withExp = set.filter(t => t.expire);
      if (!withExp.length) return { text: `Nenhum desses tickets ${lbl} tem vencimento cadastrado.` };
      const parts = withExp.slice(0, 12).map(t => `${t.ticket} vence em ${t.expire}`);
      let txt = `Vencimento de ${nTk(withExp.length)} ${lbl}: ${parts.join('; ')}`;
      if (withExp.length > 12) txt += `; e mais ${withExp.length - 12}`;
      return { text: txt + '.' };
    }

    // C) Listar os números dos tickets ("quais tickets são?", "os 3 primeiros")
    //    Escopo = projeto em foco OU, se não houver, o estado em foco.
    if (has('quais', 'lista', 'listar', 'numeros')) {
      let st = statusFromQuery(qn);
      if (st === undefined) st = lastScope || 'Open';
      let set, scopeLbl;
      if (lastProject) { set = ticketsByStatus(lastProject, st); scopeLbl = `do projeto ${lastProject.name}`; }
      else if (lastState) {
        set = visibleTickets().filter(t => norm(t.state) === lastState && (st === 'Closed' ? t.status === 'Closed' : effStatus(t) === st));
        scopeLbl = `em ${lastState.toUpperCase()}`;
      } else {
        // Sem projeto/estado em foco → sistema todo
        set = visibleTickets().filter(t => st === 'Closed' ? t.status === 'Closed' : effStatus(t) === st);
        scopeLbl = 'no sistema';
      }
      // "os 3 primeiros" / "últimos 5" → limita a quantidade
      const wantLast = /ultim/.test(qn);
      const numMatch = qn.match(/\b(\d{1,3})\b/);
      let limitN = null;
      if (numMatch && (/primeir|ultim/.test(qn) || /\bos\s+\d/.test(qn) || /^\s*\d/.test(qn))) limitN = parseInt(numMatch[1], 10);
      const full = set.length;
      if (limitN && limitN > 0 && limitN < full) set = wantLast ? set.slice(-limitN) : set.slice(0, limitN);
      lastList = set.slice(); lastScope = st; lastIntent = 'list';
      if (!set.length) return { text: `Não há tickets ${stLabel(st)} ${scopeLbl}.` };
      const nums = set.map(t => t.ticket);
      let lead;
      if (limitN && limitN < full) lead = `${wantLast ? 'Os últimos' : 'Os primeiros'} ${nums.length} tickets ${stLabel(st)} ${scopeLbl} são:`;
      else lead = set.length === 1 ? `Há 1 ticket ${stLabel(st)} ${scopeLbl}:` : `Os ${set.length} tickets ${stLabel(st)} ${scopeLbl} são:`;
      let txt = `${lead} ${nums.slice(0, 15).join(', ')}`;
      if (nums.length > 15) txt += `, e mais ${nums.length - 15}`;
      return { text: txt + '.' };
    }

    // D) Ticket específico (status pelo número)
    const tk = findTicket(qn);
    if (tk && (has('ticket', 'status') || !namedProj)) {
      lastTicket = tk;
      const st = effStatus(tk);
      const proj = getProjects().find(p => p.id === tk.projectId);
      const ft = tk.footage ? `${fmt(tk.footage)} pés` : 'sem footage cadastrada';
      const done = tk.completedFeet ? `, ${fmt(tk.completedFeet)} já concluídos` : '';
      const exp = tk.expire ? `, expira em ${tk.expire}` : '';
      const pj = proj ? `, do projeto ${proj.name}` : '';
      return { text: `O ticket ${tk.ticket} está ${traduzStatus(st)}${pj}, com ${ft}${done}${exp}.` };
    }

    // D2) Relatório consolidado por ESTADO ("relatório de todos os projetos de IN")
    const stMatch = (qn.match(/\b(fl|in|il|wi)\b/) || [])[1]
      || (/\bindiana\b/.test(qn) ? 'in' : /\bflorida\b/.test(qn) ? 'fl' : /\billinois\b/.test(qn) ? 'il' : /\bwisconsin\b/.test(qn) ? 'wi' : null);
    if (stMatch && !namedProj && (has('relatorio', 'resumo', 'report', 'panorama', 'consolidad', 'status', 'situacao', 'como esta', 'como estao', 'visao') || (has('todos') && has('projeto')) || has('projetos'))) {
      const ST = stMatch.toUpperCase();
      const projs = getProjects().filter(p => norm(p.state) === stMatch && p.status !== 'Completed');
      if (!projs.length) return { text: `Não encontrei projetos ativos em ${ST}, senhor.` };
      let totFt = 0, closedC = 0, openC = 0, openFt = 0, clearC = 0, clearFt = 0;
      projs.forEach(p => {
        const ts = ticketsOf(p.id);
        totFt += ts.reduce((s, t) => s + (t.footage || 0), 0);
        closedC += ts.filter(t => t.status === 'Closed').length;
        const o = ts.filter(t => effStatus(t) === 'Open'); openC += o.length; openFt += o.reduce((s, t) => s + (t.footage || 0), 0);
        clearC += ts.filter(t => effStatus(t) === 'Clear').length; clearFt += clearAvailableFt(ts);
      });
      const names = projs.map(p => p.name);
      // Novo contexto = ESTE estado. Limpa projeto/lista/ticket antigos (evita vazamento entre estados).
      lastState = stMatch; lastProject = null; lastList = []; lastTicket = null; lastScope = null; lastIntent = 'statereport';
      return { text: `Estado de ${ST}: ${projs.length} projetos, ${fmt(totFt)} pés no total. `
        + `${closedC} tickets concluídos. ${openC} tickets em aberto com ${fmt(openFt)} pés. ${clearC} tickets clear com ${fmt(clearFt)} pés liberados. `
        + `Os projetos são: ${names.slice(0, 15).join(', ')}${names.length > 15 ? `, e mais ${names.length - 15}` : ''}. `
        + `Quer detalhes de algum, senhor?` };
    }

    // D3) Status dos tickets já listados ("status desses 2 tickets", "e o status deles")
    if (has('status', 'situacao') && lastList && lastList.length && !namedProj && !findTicket(qn)
        && (has('desses', 'deles', 'esses', 'aqueles', 'desse', 'delas', 'listados') || lastIntent === 'list')) {
      const parts = lastList.slice(0, 12).map(t => `${t.ticket}: ${traduzStatus(effStatus(t))}`);
      let txt = `Status de ${nTk(lastList.length)}: ${parts.join('; ')}`;
      if (lastList.length > 12) txt += `; e mais ${lastList.length - 12}`;
      return { text: txt + '.' };
    }

    // E) Projeto — nomeado na frase OU herdado do contexto (pergunta anterior)
    const globalScope = has('sistema', 'geral', 'todos os projetos', 'todos projetos', 'em tudo', 'no geral')
      || /\b(fl|in|il|wi)\b/.test(qn);
    const wantsData = has('quanto', 'quantos', 'total', 'aberto', 'open', 'pendente', 'pendencia',
      'clear', 'liberad', 'damage', 'dano', 'ticket', 'pes', 'feet', 'footage', 'metragem',
      'trabalhar', 'disponivel', 'falta', 'preso', 'resumo', 'status');
    if (namedProj && has('abrir', 'mostra', 'ver', 'ir', 'entra') && typeof openProjectMap === 'function') {
      try { openProjectMap(namedProj.id); } catch (e) {}
    }
    if (namedProj) return { text: answerProject(namedProj, has) };
    if (!globalScope && lastProject && wantsData) return { text: answerProject(lastProject, has) };

    // 3) Contagens globais (só quando NÃO há projeto em foco, ou pediu sistema/estado)
    if (has('quantos', 'total', 'aberto', 'clear', 'damage', 'dano', 'tickets')) {
      const all = visibleTickets();
      const forceGlobal = has('sistema', 'geral', 'todos os projetos', 'em tudo', 'no geral');
      const state = (qn.match(/\b(fl|in|il|wi)\b/) || [])[1] || (forceGlobal ? null : lastState);
      const scope = state ? all.filter(t => norm(t.state) === state) : all;
      const label = state ? ` em ${state.toUpperCase()}` : '';
      if (has('aberto', 'open', 'pendente')) {
        const c = scope.filter(t => effStatus(t) === 'Open').length;
        return { text: `Tem ${c} ${c === 1 ? 'ticket aberto' : 'tickets abertos'}${label} no momento.` };
      }
      if (has('clear', 'liberad')) {
        const c = scope.filter(t => effStatus(t) === 'Clear').length;
        return { text: `${c} tickets clear${label}, com ${fmt(clearAvailableFt(scope))} pés liberados pra trabalhar.` };
      }
      if (has('damage', 'dano')) {
        const c = scope.filter(t => effStatus(t) === 'Damage').length;
        return { text: `${c} ${c === 1 ? 'ticket com dano' : 'tickets com dano'}${label}.` };
      }
      return { text: `São ${scope.length} tickets${label} no sistema, em ${getProjects().length} projetos.` };
    }

    // "Você quis dizer": sugere projeto por similaridade (nome mal transcrito por voz)
    const sugg = fuzzyProjectSuggestion(qn);
    if (sugg) return { text: `Não achei exatamente, senhor. Você quis dizer o projeto ${sugg.name}?` };
    return {
      text: 'Não peguei essa, senhor. Pode perguntar coisas como: quantos pés disponíveis no projeto tal, quantos tickets abertos, o status ou o que falta cavar num ticket, ou o que vence essa semana.'
    };
  }

  // ── Reconhecimento de voz (fala → texto) ────────────────────────────────
  const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
  let recog = null, listening = false;
  function makeRecog() {
    if (!SR) return null;
    const r = new SR();
    r.lang = 'pt-BR';
    r.continuous = false;
    r.interimResults = false;
    r.maxAlternatives = 1;
    r.onstart  = () => { listening = true;  el.mic.classList.add('jv-listening'); el.status.textContent = 'Ouvindo...'; };
    r.onend    = () => { listening = false; el.mic.classList.remove('jv-listening'); if (el.status.textContent === 'Ouvindo...') el.status.textContent = ''; };
    r.onerror  = (e) => { listening = false; el.mic.classList.remove('jv-listening'); el.status.textContent = e.error === 'not-allowed' ? 'Microfone bloqueado — libere nas permissões.' : ''; };
    r.onresult = (e) => { const txt = e.results[0][0].transcript; el.input.value = txt; ask(txt); };
    return r;
  }
  function toggleListen() {
    if (!SR) { el.status.textContent = 'Este navegador não reconhece voz — digite a pergunta.'; return; }
    if (!recog) recog = makeRecog();
    if (listening) { try { recog.stop(); } catch (e) {} return; }
    try { window.speechSynthesis && window.speechSynthesis.cancel(); recog.start(); } catch (e) {}
  }

  // ── UI ───────────────────────────────────────────────────────────────────
  function populateVoiceSelect() {
    if (!el.voice) return;
    const cur = CHOSEN_VOICE ? CHOSEN_VOICE.name : '';
    const pt = VOICES.filter(isPt).sort((a, b) => scoreVoice(b) - scoreVoice(a));
    const others = VOICES.filter(v => !isPt(v));
    let html = '';
    if (pt.length) {
      html += '<optgroup label="Português (recomendado)">'
        + pt.map(v => `<option value="${v.name}">${v.name}</option>`).join('') + '</optgroup>';
    }
    if (others.length) {
      html += '<optgroup label="Outras (não recomendado p/ português)">'
        + others.map(v => `<option value="${v.name}">${v.name} [${v.lang}]</option>`).join('') + '</optgroup>';
    }
    if (!VOICES.length) html = '<option value="">(nenhuma voz detectada ainda...)</option>';
    el.voice.innerHTML = html;
    if (cur) el.voice.value = cur;
  }

  function buildUI() {
    const style = document.createElement('style');
    style.textContent = `
      #jv-fab{position:fixed;right:20px;bottom:20px;width:58px;height:58px;border-radius:50%;
        border:none;cursor:pointer;z-index:9998;display:none;align-items:center;justify-content:center;
        background:radial-gradient(circle at 50% 40%,#5ad0ff 0%,#1a6cf0 55%,#0a2a6b 100%);
        box-shadow:0 0 0 3px rgba(90,208,255,.25),0 6px 22px rgba(10,42,107,.55);
        color:#fff;font-weight:700}
      #jv-fab:hover{transform:scale(1.06)}
      #jv-fab .jv-core{width:20px;height:20px;border-radius:50%;background:#eafaff;
        box-shadow:0 0 10px #bfefff,0 0 18px #5ad0ff}
      @keyframes jv-pulse{0%,100%{box-shadow:0 0 0 3px rgba(90,208,255,.25),0 6px 22px rgba(10,42,107,.55)}
        50%{box-shadow:0 0 0 7px rgba(90,208,255,.08),0 6px 26px rgba(10,42,107,.65)}}
      #jv-panel{position:fixed;right:20px;bottom:88px;width:340px;max-width:calc(100vw - 40px);
        max-height:74vh;display:none;flex-direction:column;z-index:9999;border-radius:16px;overflow:hidden;
        background:var(--bg,#0d1117);border:1px solid rgba(90,208,255,.35);box-shadow:0 18px 50px rgba(0,0,0,.5)}
      #jv-panel.jv-open{display:flex}
      .jv-head{padding:12px 14px;display:flex;align-items:center;gap:9px;background:linear-gradient(90deg,#0a2a6b,#1a6cf0);color:#fff}
      .jv-head .jv-dot{width:10px;height:10px;border-radius:50%;background:#5ad0ff;box-shadow:0 0 8px #5ad0ff}
      .jv-head b{letter-spacing:2px;font-size:13px;flex:1}
      .jv-head button{background:rgba(255,255,255,.15);border:none;color:#fff;width:26px;height:26px;border-radius:7px;cursor:pointer;font-size:13px}
      .jv-voicebar{display:flex;align-items:center;gap:6px;padding:8px 12px;background:var(--bg2,#161b22);border-bottom:1px solid var(--border,#30363d)}
      .jv-voicebar label{font-size:10px;color:var(--muted,#8b949e);white-space:nowrap}
      .jv-voicebar select{flex:1;min-width:0;background:var(--bg,#0d1117);color:var(--text,#e6edf3);border:1px solid var(--border,#30363d);border-radius:8px;padding:5px 7px;font-size:11px}
      .jv-voicebar button{background:#1a6cf0;border:none;color:#fff;border-radius:8px;padding:5px 8px;font-size:11px;cursor:pointer;white-space:nowrap}
      .jv-log{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:9px;background:var(--bg,#0d1117);color:var(--text,#e6edf3);font-size:13px;line-height:1.5}
      .jv-msg{padding:9px 12px;border-radius:12px;max-width:88%}
      .jv-msg.jv-user{align-self:flex-end;background:#1a6cf0;color:#fff;border-bottom-right-radius:4px}
      .jv-msg.jv-bot{align-self:flex-start;background:var(--bg2,#161b22);border:1px solid var(--border,#30363d);border-bottom-left-radius:4px}
      .jv-status{font-size:11px;color:var(--muted,#8b949e);min-height:15px;padding:2px 12px}
      .jv-inrow{display:flex;gap:7px;padding:10px 12px;border-top:1px solid var(--border,#30363d);background:var(--bg,#0d1117)}
      .jv-inrow input{flex:1;background:var(--bg2,#161b22);border:1px solid var(--border,#30363d);border-radius:10px;color:var(--text,#e6edf3);padding:9px 11px;font-size:13px;outline:none}
      .jv-inrow input:focus{border-color:#5ad0ff}
      .jv-inrow button{border:none;border-radius:10px;cursor:pointer;width:40px;font-size:16px;color:#fff}
      .jv-mic{background:#1a6cf0}.jv-mic.jv-listening{background:#e5484d;animation:jv-pulse 1s infinite}
      .jv-send{background:#238636}
      .jv-mute{background:var(--bg2,#161b22)!important;border:1px solid var(--border,#30363d)!important;color:var(--muted,#8b949e)!important}
      .jv-mute.jv-off{opacity:.5}
      @media(max-width:600px){#jv-fab{right:14px;bottom:78px}#jv-panel{right:10px;bottom:136px}}
    `;
    document.head.appendChild(style);

    const fab = document.createElement('button');
    fab.id = 'jv-fab';
    fab.title = 'Assistente OneDrill (voz)';
    fab.innerHTML = '<span class="jv-core"></span>';
    document.body.appendChild(fab);

    const panel = document.createElement('div');
    panel.id = 'jv-panel';
    panel.innerHTML = `
      <div class="jv-head">
        <span class="jv-dot"></span>
        <b>J.A.R.V.I.S.</b>
        <button class="jv-mute" title="Mudo">🔊</button>
        <button class="jv-x" title="Fechar">×</button>
      </div>
      <div class="jv-voicebar">
        <label>Voz:</label>
        <select id="jv-voice"><option value="">(carregando...)</option></select>
        <button class="jv-test" title="Testar voz">▶ Testar</button>
      </div>
      <div class="jv-log" id="jv-log">
        <div class="jv-msg jv-bot">Às ordens, senhor. Pergunte por voz 🎙 ou digite — ex.: <i>"quantos pés disponíveis pra trabalhar no projeto tal"</i>.</div>
      </div>
      <div class="jv-status" id="jv-status"></div>
      <div class="jv-inrow">
        <input id="jv-input" placeholder="Pergunte algo..." autocomplete="off">
        <button class="jv-mic" title="Falar">🎙</button>
        <button class="jv-send" title="Enviar">➤</button>
      </div>`;
    document.body.appendChild(panel);

    el.fab = fab; el.panel = panel;
    el.log = panel.querySelector('#jv-log');
    el.status = panel.querySelector('#jv-status');
    el.input = panel.querySelector('#jv-input');
    el.mic = panel.querySelector('.jv-mic');
    el.send = panel.querySelector('.jv-send');
    el.mute = panel.querySelector('.jv-mute');
    el.close = panel.querySelector('.jv-x');
    el.voice = panel.querySelector('#jv-voice');
    el.test = panel.querySelector('.jv-test');

    populateVoiceSelect();

    fab.addEventListener('click', () => {
      panel.classList.toggle('jv-open');
      if (panel.classList.contains('jv-open')) { refreshVoices(); el.input.focus(); }
    });
    el.close.addEventListener('click', () => panel.classList.remove('jv-open'));
    el.send.addEventListener('click', () => ask(el.input.value));
    el.input.addEventListener('keydown', (e) => { if (e.key === 'Enter') ask(el.input.value); });
    el.mic.addEventListener('click', toggleListen);
    el.mute.addEventListener('click', () => {
      SPEAK_ON = !SPEAK_ON;
      el.mute.classList.toggle('jv-off', !SPEAK_ON);
      el.mute.textContent = SPEAK_ON ? '🔊' : '🔇';
      if (!SPEAK_ON) _stopSpeaking();
    });
    el.voice.addEventListener('change', () => {
      const v = allVoices().find(x => x.name === el.voice.value);
      if (v) {
        CHOSEN_VOICE = v;
        try { localStorage.setItem(LS_KEY, v.name); } catch (e) {}
        el.status.textContent = isPt(v) ? '' : '⚠ Essa voz não é português — vai soar errado com texto em PT.';
        const on = SPEAK_ON; SPEAK_ON = true;
        speak('Perfeito, senhor. Essa é a minha voz.');
        SPEAK_ON = on;
      }
    });
    el.test.addEventListener('click', () => {
      const on = SPEAK_ON; SPEAK_ON = true;
      speak('Olha só, senhor. É assim que eu vou falar as respostas do sistema.');
      SPEAK_ON = on;
    });

    // Só aparece dentro do app admin (não no login nem no shared view)
    const shell = document.getElementById('app-shell');
    const sync = () => {
      const show = shell && !shell.classList.contains('hidden');
      fab.style.display = show ? 'flex' : 'none';
      if (!show) panel.classList.remove('jv-open');
      else if (!greeted && !greetScheduled) { greetScheduled = true; setTimeout(doGreeting, 800); } // saúda ao entrar
    };
    sync();
    if (shell) new MutationObserver(sync).observe(shell, { attributes: true, attributeFilter: ['class'] });
    setInterval(sync, 2000);
  }

  function push(text, who) {
    const d = document.createElement('div');
    d.className = 'jv-msg ' + (who === 'user' ? 'jv-user' : 'jv-bot');
    d.textContent = text;
    el.log.appendChild(d);
    el.log.scrollTop = el.log.scrollHeight;
  }

  function ask(q) {
    q = (q || '').trim();
    if (!q) return;
    push(q, 'user');
    el.input.value = '';
    el.status.textContent = '';
    // Capturando o nome na primeira entrada
    if (awaitingName) {
      awaitingName = false;
      const nm = capName(q);
      if (nm) { userName = nm; try { localStorage.setItem('jarvis-username', nm); } catch (e) {} }
      const txt = userName ? `Prazer, ${userName}. Gostaria de uma atualização dos projetos?` : 'Certo. Gostaria de uma atualização dos projetos?';
      lastIntent = 'greet';
      push(txt, 'bot'); speak(txt);
      return;
    }
    const res = answer(q);
    push(res.text, 'bot');
    speak(res.text);
  }

  // ── Boot ──────────────────────────────────────────────────────────────
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', buildUI);
  else buildUI();
})();
