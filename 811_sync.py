#!/usr/bin/env python3
"""
OneDrill - 811 Automation Script
Indiana 811 + Sunshine 811

Melhorias v2:
  - Retry genérico em TODAS as operações Supabase
  - Lock file pra evitar execuções simultâneas
  - Resumo de sync estruturado (cleared, reverted, private_locator)
  - Dedup e truncamento de notas automáticas
  - COMPANY_PHONE movido pro .env
  - Validação no parser de contatos
  - Scheduler com stagger e melhor controle de concorrência
  - Context manager pra Playwright (evita Chrome órfão)
  - Logs mais claros e consistentes

═══════════════════════════════════════════════════════════════════════════════
ÍNDICE DE SEÇÕES — use Ctrl+F com "│ SECTION" pra navegar rapidamente
═══════════════════════════════════════════════════════════════════════════════

CONFIG E UTILITÁRIOS
  │ SECTION: DEBUG_MODE       ~L91    │  ONEDRILL_DEBUG env flags
  │ SECTION: CONFIG           ~L97    │  Constantes do app (.env, URLs, paths)
  │ SECTION: LOGGING          ~L136   │  Setup do logger
  │ SECTION: CONSTANTS        ~L152   │  Constantes gerais (BATCH_SIZE, etc)
  │ SECTION: LOCK             ~L161   │  ProcessLock (class) — lock file anti-concorrência
  │ SECTION: CANCEL_CACHE     ~L229   │  Cache de tickets cancelados em disco
  │ SECTION: WAIT_HELPERS     ~L278   │  Smart waits pra Playwright
  │ SECTION: PLAYWRIGHT_CTX   ~L334   │  Context manager pra Playwright
  │ SECTION: SUPABASE_IO      ~L366   │  sb_get, sb_insert, sb_patch, sb_upsert, sb_delete

DOMÍNIO DO 811
  │ SECTION: AUTO_NOTES       ~L440   │  append_auto_note (dedup + truncamento)
  │ SECTION: CLASSIFY         ~L470   │  classify() — interpreta status code → Clear/Pending/Damage
  │ SECTION: LOCATION_TEXT    ~L669   │  extract_location_text — parse do body
  │ SECTION: EXPIRE_PARSE     ~L714   │  extract_expire_date + normalize_expire + _is_polluted_expire
  │ SECTION: CANCEL_DETECT    ~L883   │  is_ticket_canceled()
  │ SECTION: COORDS_ADJUST    ~L898   │  adjust_coords_by_location (alinha pro texto 811)
  │ SECTION: GEOCODING        ~L934   │  geocode_address (Nominatim — sleep 1.1s, fix #14)

PORTAIS — PLAYWRIGHT
  │ SECTION: AUTO_LOGIN       ~L971   │  auto_login — renovação manual em janela visível
  │ SECTION: PORTAL_NAV       ~L1019  │  goto_dashboard, filter_ticket, back_to_dashboard, ensure_login
  │ SECTION: SCRAPE           ~L1177  │  scrape() — função central paralela
  │ SECTION: SYNC_SUMMARY     ~L1535  │  SyncSummary (class) — stats estruturadas
  │ SECTION: BATCH_PATCH      ~L1579  │  _get_latest_response_date + sb_batch_patch (fix #5)
  │ SECTION: UNRECOGNIZED     ~L1685  │  save_unrecognized_responses + send_unrecognized_alert
  │ SECTION: SAVE             ~L1760  │  save_to_supabase — grava resultados do scrape

COMANDOS PRINCIPAIS
  │ SECTION: IMPORT           ~L2098  │  import_new_tickets (batch upsert, fix #4)
  │ SECTION: RESCRAPE         ~L2393  │  rescrape_notes — atualiza notes + expire
  │ SECTION: CLEANUP          ~L2486  │  cleanup_canceled — remove tickets cancelados
  │ SECTION: FILTER_SYNC      ~L2573  │  filter_tickets_for_sync — prioriza o que scrapear
  │ SECTION: SYNC             ~L2661  │  sync_state + sync_all + sync_and_import*

ILLINOIS — JULIE PÚBLICO
  │ SECTION: JULIE            ~L2787  │  scrape_julie_ticket + scrape_il + sync_il + save_ticket_pdfs_il

CONTATOS (FL)
  │ SECTION: CONTACTS_FL      ~L3161  │  parse_contact_table + scrape_contacts
  │ SECTION: UTILITY_HELPERS  ~L3509  │  get_contacts_for_utility, get_all_contacts

EXPORTS E DEBUG
  │ SECTION: EXPORT_EXCEL     ~L3525  │  export_excel
  │ SECTION: DEBUG_SCREENSHOT ~L3596  │  debug_screenshot
  │ SECTION: BACKFILL         ~L3620  │  backfill_history + fix_clear_dates
  │ SECTION: PDF_SAVE         ~L3814  │  save_ticket_pdfs (Print Dialog via hwnd)
  │ SECTION: BACKUP           ~L4093  │  backup_database

MANUTENÇÃO E TESTES
  │ SECTION: SELF_TESTS       ~L4186  │  run_self_tests — validação inline
  │ SECTION: FIX_RENEWALS     ~L4412  │  fix_renewals — conserta expire_old
  │ SECTION: FIX_EXPIRES      ~L4549  │  fix_expires — conserta formato antigo
  │ SECTION: AUDIT            ~L4668  │  audit_health + clean_ghost_utilities
  │ SECTION: DEBUG_EXPIRE     ~L4994  │  debug_expire — testa patterns em um ticket
  │ SECTION: CLI              ~L5117  │  Argparse + main entry point

═══════════════════════════════════════════════════════════════════════════════
"""

import os, sys, time, logging, logging.handlers, argparse, asyncio, re, urllib.parse
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import requests
from playwright.async_api import async_playwright
import json as _json

# ── │ SECTION: DEBUG_MODE │ DEBUG MODE ────────────────────────────────────────
DEBUG_MODE = os.environ.get("ONEDRILL_DEBUG", "").lower() in ("1", "true", "yes")
DEBUG_TICKET = os.environ.get("ONEDRILL_DEBUG_TICKET", "").strip()  # Se setado, força debug desse ticket específico

load_dotenv()

# ── │ SECTION: CONFIG │ CONFIGURAÇÃO ──────────────────────────────────────────
SB_URL        = os.getenv("SB_URL",  "https://ofbqtaulvzeltfpqcjhh.supabase.co")
SB_KEY        = os.getenv("SB_KEY",  "")
IN_USER       = os.getenv("IN_USER", "")
IN_PASS       = os.getenv("IN_PASS", "")
FL_USER       = os.getenv("FL_USER", "")
FL_PASS       = os.getenv("FL_PASS", "")
COMPANY_PHONE = os.getenv("COMPANY_PHONE", "3219473131")

# Validação de configuração crítica
if not SB_KEY:
    print("⚠ AVISO: SB_KEY não definida no .env — operações Supabase vão falhar", file=sys.stderr)

PORTALS = {
    "IN": {
        "url":       "https://811.indiana811.org/login?returnUrl=%2Fhome",
        "home":      "https://811.indiana811.org/home",
        "dashboard": "https://811.indiana811.org/tickets/dashboard",
        "user":      lambda: IN_USER,
        "pass":      lambda: IN_PASS,
    },
    "FL": {
        "url":       "https://exactix.sunshine811.com/login",
        "home":      "https://exactix.sunshine811.com/home",
        "dashboard": "https://exactix.sunshine811.com/tickets/dashboard",
        "user":      lambda: FL_USER,
        "pass":      lambda: FL_PASS,
    },
}

JULIE_URL = "https://newtin.julie1call.com/responsedisplay/"
DIGGERS_URL = "https://geocall.diggershotline.com/geocall/portal"

SB_H = {
    "apikey":        SB_KEY,
    "Authorization": f"Bearer {SB_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=representation"
}

# ── │ SECTION: LOGGING │ LOGGING ──────────────────────────────────────────────
_log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "811_sync.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.handlers.RotatingFileHandler(
            _log_file, maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
        ),
        logging.StreamHandler(
            stream=open(sys.stdout.fileno(), mode='w', encoding='utf-8', closefd=False)
        )
    ]
)
log = logging.getLogger(__name__)

# ── │ SECTION: CONSTANTS │ CONSTANTES ─────────────────────────────────────────
BASE_DIR           = os.path.dirname(os.path.abspath(__file__))
VALID_STATES       = {"FL", "IN", "IL", "WI"}
W_SAFETY           = 300      # fallback ms quando não há seletor confiável
NUM_TABS           = 5        # 5 abas paralelas (reduzir nao ajuda, aumentar de 3→5 corta tempo ~40%)
CLEAR_CACHE_HOURS  = 24       # re-verifica Clear tickets 1x por dia
MAX_AUTO_NOTES     = 10       # máximo de notas automáticas por ticket
BATCH_SIZE         = 200      # tamanho do lote para bulk upsert
MAX_IMPORT_PAGES   = 5        # com 100/pagina, 5 paginas = 500 tickets
LOCK_FILE          = os.path.join(BASE_DIR, "811_sync.lock")
TIMEOUT_PAGE       = 60000    # Playwright default timeout (ms)
TIMEOUT_STABLE     = 2000     # wait_stable / wait_tab_content / wait_filter_results (3s era conservador demais)
TIMEOUT_NAV        = 5000     # wait_for / wait_nav
TIMEOUT_CLICK      = 8000     # click_and_wait


def _profile_path(state):
    return os.path.join(BASE_DIR, f"chrome_profile_{state}")

# ── │ SECTION: LOCK │ LOCK FILE (evita execuções simultâneas) ────────────────
class ProcessLock:
    """Lock file simples pra evitar múltiplas instâncias do script."""

    def __init__(self, path=LOCK_FILE):
        self.path = path
        self.acquired = False

    def acquire(self):
        """Tenta adquirir o lock. Retorna True se conseguiu."""
        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    data = _json.load(f)
                pid = data.get("pid", 0)
                started = data.get("started", "")
                # Se o processo ainda existe, lock ativo.
                # Fix bug #16 Python: diferenciar PermissionError (processo existe mas de outro
                # usuário) de ProcessLookupError (processo realmente morreu). No Windows especialmente,
                # os.kill(pid, 0) pode levantar PermissionError pra processos de outros usuários.
                # Antes: ambos caíam no except OSError, marcando lock como stale incorretamente —
                # podia resultar em 2 instâncias do scraper rodando simultaneamente.
                try:
                    os.kill(pid, 0)  # Só checa se existe (não mata)
                    log.warning(f"Lock ativo — PID {pid} iniciado em {started}")
                    return False
                except PermissionError:
                    # Processo existe mas é de outro usuário — tratar como ativo (NÃO stale)
                    log.warning(f"Lock ativo — PID {pid} (outro usuário) iniciado em {started}")
                    return False
                except (ProcessLookupError, OSError):
                    # Processo realmente morreu — stale lock
                    log.warning(f"Lock stale detectado (PID {pid}) — removendo")
                    self.release()
            except Exception:
                # Lock corrompido — remove
                self.release()

        try:
            with open(self.path, "w") as f:
                _json.dump({
                    "pid": os.getpid(),
                    "started": datetime.now().isoformat()
                }, f)
            self.acquired = True
            return True
        except Exception as e:
            log.error(f"Erro ao criar lock file: {e}")
            return False

    def release(self):
        """Libera o lock."""
        try:
            if os.path.exists(self.path):
                os.remove(self.path)
        except Exception as e:
            log.warning(f"Erro ao remover lock file: {e}")
        self.acquired = False

    def __enter__(self):
        if not self.acquire():
            raise RuntimeError("Outra instância já está rodando (lock file ativo)")
        return self

    def __exit__(self, *args):
        self.release()


# ── │ SECTION: CANCEL_CACHE │ CACHE DE TICKETS CANCELADOS ────────────────────
_CANCELED_CACHE_FILE = os.path.join(BASE_DIR, "canceled_cache.json")
_canceled_cache_mem = None


def load_canceled_cache():
    """Carrega set de tickets cancelados do disco (com cache em memória)."""
    global _canceled_cache_mem
    if _canceled_cache_mem is not None:
        return _canceled_cache_mem
    try:
        if os.path.exists(_CANCELED_CACHE_FILE):
            with open(_CANCELED_CACHE_FILE, "r") as f:
                data = _json.load(f)
            _canceled_cache_mem = {state: set(nums) for state, nums in data.items()}
            return _canceled_cache_mem
    except Exception as e:
        log.warning(f"Erro ao carregar cache de cancelados: {e}")
    _canceled_cache_mem = {}
    return _canceled_cache_mem


def save_canceled_cache(cache=None):
    """Salva cache de cancelados no disco."""
    global _canceled_cache_mem
    if cache is None:
        cache = _canceled_cache_mem or {}
    try:
        data = {state: sorted(nums) for state, nums in cache.items()}
        with open(_CANCELED_CACHE_FILE, "w") as f:
            _json.dump(data, f, indent=2)
    except Exception as e:
        log.warning(f"Erro ao salvar cache de cancelados: {e}")


def add_to_canceled_cache(state, ticket_num):
    """Adiciona ticket ao cache de cancelados (salva imediatamente)."""
    cache = load_canceled_cache()
    if state not in cache:
        cache[state] = set()
    cache[state].add(ticket_num)
    save_canceled_cache(cache)


def get_canceled_set(state):
    """Retorna set de tickets cancelados para um estado."""
    return load_canceled_cache().get(state, set())


# ── │ SECTION: WAIT_HELPERS │ SMART WAIT HELPERS ──────────────────────────────
async def wait_stable(page, timeout=TIMEOUT_STABLE):
    """Espera a página estabilizar (rede ociosa). Fallback silencioso."""
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        await page.wait_for_timeout(W_SAFETY)


async def wait_for(page, selector, timeout=TIMEOUT_NAV, state="visible"):
    """Espera seletor aparecer. Retorna True se encontrou."""
    try:
        await page.wait_for_selector(selector, timeout=timeout, state=state)
        return True
    except Exception:
        return False


async def wait_nav(page, timeout=TIMEOUT_NAV):
    """Espera após navegação: load completo + rede ociosa."""
    try:
        await page.wait_for_load_state("domcontentloaded", timeout=timeout)
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        await page.wait_for_timeout(W_SAFETY)


async def wait_tab_content(page, timeout=TIMEOUT_STABLE):
    """Espera conteúdo de aba carregar."""
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        await page.wait_for_timeout(W_SAFETY)


async def wait_filter_results(page, timeout=TIMEOUT_STABLE):
    """Espera resultados do filtro carregarem."""
    try:
        await page.wait_for_load_state("networkidle", timeout=timeout)
    except Exception:
        await page.wait_for_timeout(W_SAFETY)


async def click_and_wait(page, locator, wait_type="stable", timeout=TIMEOUT_CLICK):
    """Clica num elemento e espera o resultado."""
    await locator.click()
    if wait_type == "nav":
        await wait_nav(page, timeout)
    elif wait_type == "tab":
        await wait_tab_content(page, timeout)
    elif wait_type == "filter":
        await wait_filter_results(page, timeout)
    else:
        await wait_stable(page, timeout)


async def _dismiss_dialog(page, heading_text, label=""):
    """Fecha modal Angular Material (Exactix FL) tentando 4 estratégias."""
    dialog = page.locator(f'h1:has-text("{heading_text}")')
    if not await dialog.count():
        return True
    for sel in [
        'button[mat-dialog-close]', 'button.mat-dialog-close',
        'button:has-text("Close")', 'button:has-text("OK")',
        'button:has-text("Dismiss")', 'button:has-text("Cancel")',
        'mat-dialog-container button.mat-icon-button',
        'mat-dialog-container button[aria-label*="close" i]',
        'mat-dialog-container button[aria-label*="dismiss" i]',
    ]:
        try:
            btn = page.locator(sel)
            if await btn.count():
                await btn.first.click(timeout=2000)
                await page.wait_for_timeout(400)
                if not await dialog.count():
                    return True
        except Exception:
            continue
    try:
        await page.keyboard.press("Escape")
        await page.wait_for_timeout(400)
        if not await dialog.count():
            return True
    except Exception:
        pass
    try:
        backdrop = page.locator('.cdk-overlay-backdrop').first
        if await backdrop.count():
            await backdrop.click(timeout=2000, force=True)
            await page.wait_for_timeout(400)
            if not await dialog.count():
                return True
    except Exception:
        pass
    try:
        await page.evaluate(
            "document.querySelectorAll('.cdk-overlay-container mat-dialog-container,"
            " .cdk-overlay-pane, .cdk-overlay-backdrop').forEach(el => el.remove())"
        )
        await page.wait_for_timeout(200)
        if not await dialog.count():
            log.warning(f"  {label}: Dialog '{heading_text}' removido via JS (fallback)")
            return True
    except Exception as e:
        log.warning(f"  {label}: Falha ao remover dialog via JS: {e}")
    log.warning(f"  {label}: NAO conseguiu fechar dialog '{heading_text}'")
    return False


# ── │ SECTION: PLAYWRIGHT_CTX │ PLAYWRIGHT CONTEXT MANAGER (evita Chrome órfão)
@asynccontextmanager
async def playwright_context(state, headless=True):
    """Context manager que garante fechamento do browser mesmo com crash.

    Uso:
        async with playwright_context("FL") as (p, ctx, page):
            ...
    """
    perfil = _profile_path(state)
    p = ctx = page = None
    try:
        p = await async_playwright().start()
        ctx = await p.chromium.launch_persistent_context(
            perfil, headless=headless, args=["--no-sandbox"]
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)
        yield p, ctx, page
    finally:
        if ctx:
            try:
                await ctx.close()
            except Exception:
                pass
        if p:
            try:
                await p.stop()
            except Exception:
                pass


# ── │ SECTION: SUPABASE_IO │ SUPABASE (com retry genérico) ───────────────────
def _sb_request(method, url, retries=3, **kwargs):
    """Wrapper genérico com retry e backoff exponencial pra TODAS as operações Supabase."""
    kwargs.setdefault("timeout", 20)
    for attempt in range(retries):
        try:
            r = method(url, **kwargs)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code if e.response is not None else 0
            # 4xx (exceto 429) = erro do cliente, não adianta retry
            if 400 <= status_code < 500 and status_code != 429:
                log.error(f"[Supabase] {status_code} — {e}")
                raise
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                log.warning(f"[Supabase] Retry {attempt+1}/{retries} em {wait}s: {e}")
                time.sleep(wait)
            else:
                log.error(f"[Supabase] Falha após {retries} tentativas: {e}")
                raise
        except Exception as e:
            if attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                log.warning(f"[Supabase] Retry {attempt+1}/{retries} em {wait}s: {e}")
                time.sleep(wait)
            else:
                log.error(f"[Supabase] Falha após {retries} tentativas: {e}")
                raise


def _qv(val):
    """URL-encode um valor pra PostgREST query (safe='' escapa &, =, espaço, etc.)."""
    return urllib.parse.quote(str(val), safe="")


def sb_get(table, qs=""):
    r = _sb_request(requests.get, f"{SB_URL}/rest/v1/{table}?select=*{qs}", headers=SB_H)
    return r.json()


def sb_upsert(table, data, on_conflict="ticket_num,utility_name,state"):
    """Upsert com retry. Suporta dict único ou lista."""
    h = {**SB_H, "Prefer": "resolution=merge-duplicates,return=minimal"}
    _sb_request(requests.post, f"{SB_URL}/rest/v1/{table}?on_conflict={on_conflict}",
                headers=h, json=data, timeout=30)


def sb_insert(table, data):
    r = _sb_request(requests.post, f"{SB_URL}/rest/v1/{table}", headers=SB_H, json=data)
    return r.json()


def sb_patch(table, id_val, data):
    h = {**SB_H, "Prefer": "return=minimal"}
    _sb_request(requests.patch, f"{SB_URL}/rest/v1/{table}?id=eq.{id_val}", headers=h, json=data)


def sb_delete(table, qs):
    h = {**SB_H, "Prefer": "return=minimal"}
    _sb_request(requests.delete, f"{SB_URL}/rest/v1/{table}?{qs}", headers=h)


def log_start(state, by="manual"):
    res = sb_insert("sync_811_log", {"state": state, "status": "running", "triggered_by": by})
    return res[0]["id"] if res else None


def log_finish(lid, checked, updated, status="success", error=None):
    if not lid:
        return
    sb_patch("sync_811_log", lid, {
        "finished_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
        "tickets_checked": checked, "tickets_updated": updated,
        "status": status, "error_msg": error
    })


# ── │ SECTION: AUTO_NOTES │ NOTAS AUTOMÁTICAS (dedup + truncamento) ──────────
def append_auto_note(existing_notes, new_note, max_notes=MAX_AUTO_NOTES):
    """Adiciona nota automática ao campo notes com dedup e truncamento.

    - Não adiciona se a nota exata já existir
    - Remove notas auto mais antigas se ultrapassar max_notes
    - Retorna o campo notes atualizado
    """
    existing = (existing_notes or "").strip()
    lines = existing.split("\n") if existing else []

    # Dedup: se a nota exata já existe, não adiciona
    if new_note.strip() in [l.strip() for l in lines]:
        return existing

    lines.append(new_note.strip())

    # Separa notas automáticas das manuais
    auto_lines = [l for l in lines if l.strip().startswith("[AUTO 811]")]
    manual_lines = [l for l in lines if not l.strip().startswith("[AUTO 811]")]

    # Trunca auto-notas se exceder limite (mantém as mais recentes)
    if len(auto_lines) > max_notes:
        auto_lines = auto_lines[-max_notes:]

    # Reconstrói: manuais primeiro, depois auto
    result = "\n".join(manual_lines + auto_lines).strip()
    return result


# ── │ SECTION: CLASSIFY │ CLASSIFY STATUS ─────────────────────────────────────
def classify(status_text, response_text=""):
    """Classifica resposta de utility como Clear, Pending ou Damage.

    Retorna tupla (status, unrecognized):
      - status: "Clear", "Pending" ou "Damage"
      - unrecognized: True se nenhum padrão conhecido bateu (fallback genérico)
    """
    s = (status_text or "").strip().lower()
    r = (response_text or "").strip().lower()
    full = (s + " " + r).lower()

    # ── INDIANA 811 — OPEN CODES (PENDING: precisam de atualização) ──
    # 3C: Marking Delay — do not excavate
    # 3F: Line untonable — do not excavate
    # 3G: Ongoing — partially marked, do not excavate unmarked area
    # 6A: Joint Meet Conflict — meeting in conflict
    # 6B: Joint Meet Accepted — meeting accepted (aguardando)

    # ── INDIANA 811 — CLOSED CODES ──
    # 1:  Marked → Clear
    # 1A: Marked with Exceptions, Do Not Excavate, High-Profile → PENDING (do not excavate!)
    # 1B: Marked with Exceptions, High-Profile → Clear (MAY contact)
    # 1C: Work by Facility Owner → Clear
    # 2:  Clear — no underground facilities → Clear
    # 3A: Could Not Gain Access → PENDING (do not excavate until resolved)
    # 3B: Incorrect Address → PENDING (do not excavate until resolved)
    # 3D: Marking Instructions Unclear → PENDING (do not excavate until resolved)
    # 3E: Excavation Already Performed or Canceled → Clear
    # 4:  Private Line → Clear (private locator needed)
    # 5A: Design Notice Documents Provided → Clear
    # 5B: Design Notice Marked → Clear
    # 6C: Joint Meet Complete → Clear
    # 7:  Damage → Damage

    # ── FL SUNSHINE 811 CODES ──
    # 1: Marked → Clear
    # 2: Clear → Clear
    # 2E: Marked with Exceptions → Clear
    # 3F: Marking delay → Pending
    # 3H: Privately owned → Clear (needs private locator)
    # 3T: Extraordinary circumstances → Pending
    # 3U: Not service provider → Clear
    # 4: Clear No Facilities → Clear
    # 5: No Conflict → Clear
    # 6A: Active Facilities → Pending (DO NOT demolish)
    # 8: Ongoing Job → Pending

    # ── IL/JULIE: ENTRADAS DE AGENDAMENTO (não são respostas de status) ──
    # Idealmente filtradas antes de chegar aqui, mas safety net pra não disparar alerta
    SCHEDULE_SKIP = [
        "declined code 50", "accepted code 50",
        "locator and excavator agreed", "alternate marking schedule",
        "alternate date requested",
    ]
    for pat in SCHEDULE_SKIP:
        if pat in full:
            return "Pending", False  # Reconhecido: agendamento, não resposta real

    # ── DAMAGE ──
    if "7:" in full and "damage" in full:
        return "Damage", False

    # ── ALWAYS PENDING — "do not excavate/demolish" overrides everything ──
    if "1a" in full and ("do not excavate" in full or "high-profile" in full):
        return "Pending", False
    if "do not excavate" in full and "3u" not in full:
        return "Pending", False
    if "do not demolish" in full:
        return "Pending", False

    # ── SPECIFIC CLEAR CODES (check before generic blockers) ──
    # WI / Diggers Hotline: "Not Participating" = utility não atende a área (similar ao 3U)
    if "3u" in full or "not service provider" in full or "not participating" in full:
        return "Clear", False
    if "3h" in full or "privately owned" in full or "private facility owner" in full:
        return "Clear", False
    if "3e" in full and ("already performed" in full or "canceled" in full):
        return "Clear", False
    # IL/JULIE: Code 60 — Watch and Protect (W&P): utility has critical facility,
    # rep must be present during excavation. Clear, but needs W&P coordination.
    if "watch and protect" in full:
        return "Clear", False

    # ── SPECIFIC PENDING CODES ──
    if "6a" in full or "active facilities" in full:
        return "Pending", False
    if "6b" in full and "joint meet accepted" in full:
        return "Pending", False
    if "3a" in full and "could not gain access" in full:
        return "Pending", False
    if "3b" in full and "incorrect address" in full:
        return "Pending", False
    if "3d" in full and ("instructions" in full or "unclear" in full):
        return "Pending", False
    if "3g" in full and ("ongoing" in full or "partially" in full):
        return "Pending", False
    if "3t" in full and "extraordinary" in full:
        return "Pending", False

    # ── WI / Diggers Hotline: "Ongoing - Working with Excavator" ──
    # Utility começou a trabalhar mas ainda não terminou — bloqueia escavação.
    if "ongoing" in full and ("excavator" in full or "working with" in full):
        return "Pending", False

    # ── GENERIC BLOCKED PATTERNS ──
    BLOCKED = [
        "no response", "no access", "unmarked", "unmark", "marking delay",
        "incorrect address", "unclear instruction", "ongoing job",
        "scheduled marking", "late ticket"
    ]
    for b in BLOCKED:
        if b in full:
            return "Pending", False

    # ── GENERIC CLEAR PATTERNS ──
    CLEARED = [
        "1: marked", "1 - marked", "1 marked", "1b", "1c",
        "marked with exception", "2e",
        "2: clear", "2 - clear", "2 clear",
        "4: clear", "4 - clear", "4 clear", "4: private", "4 private line",
        "5: no conflict", "5 - no conflict", "no conflict", "no facilit",
        "5a", "5b", "6c", "joint meet complete",
    ]
    for c in CLEARED:
        if c in full:
            return "Clear", False

    if "marked" in full:
        return "Clear", False
    if "clear" in full and "unclear" not in full:
        return "Clear", False
    if s == "clear":
        return "Clear", False

    # ── AMBIGUOUS: Positive Response / Current sem código reconhecido ──
    if "positive response" in s or s == "current":
        if "3u" in r or "not service provider" in r:
            return "Clear", False
        if "3h" in r or "privately owned" in r:
            return "Clear", False
        for b in BLOCKED:
            if b in r:
                return "Pending", False
        if "marked" in r or "clear" in r or "no conflict" in r or "no facilit" in r:
            return "Clear", False
        return "Pending", True  # ⚠ UNRECOGNIZED: Positive Response/Current sem padrão

    # ── TAGS DE UTILITY (IL/JULIE) ──
    # Algumas utilities respondem só com a categoria delas, em colchetes ou cru.
    # Não é uma resposta real — significa que ainda não posicionaram, mas não é
    # um padrão "novo" que mereça alerta de unrecognized.
    rt_stripped = (response_text or "").strip()
    if re.match(r"^\[[A-Z]{3,5}\]$", rt_stripped):
        return "Pending", False  # tag tipo [COMM], [GAS], [ELEC], [FIBR], [WATR]
    if rt_stripped.upper() in {"ELECTRIC", "GAS", "WATER", "COMM", "TELECOM", "FIBER", "MCIU01"}:
        return "Pending", False  # label cru de tipo de utility

    # ── LATE RESPONSE ──
    # Resposta tardia mas válida; já se sabe que está pendente.
    if "late final" in full or "late response" in full:
        return "Pending", False

    return "Pending", True  # ⚠ UNRECOGNIZED: nenhum padrão reconhecido


def is_in_renewal_grace(ticket, ref_date=None):
    """Determina se um ticket renovado ainda está em período de carência.

    Args:
        ticket: dict com campos old_ticket2, expire_old
        ref_date: data de referência (default: hoje). Injetável para testes.

    Returns: (in_grace, old_ticket_num)
        in_grace: True se renovado E dentro do período de carência
        old_ticket_num: número do ticket antigo (str) ou "" se não aplicável
    """
    old_chain = (ticket.get("old_ticket2") or "").strip()
    if not old_chain:
        return False, ""
    old_num = old_chain.split(" → ")[0].strip()
    old_expire_str = (ticket.get("expire_old") or "").strip()
    if not old_expire_str or old_expire_str == "—":
        return False, old_num
    try:
        exp_str = old_expire_str.split("Time:")[0].strip()
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                exp_dt = datetime.strptime(exp_str, fmt)
                break
            except ValueError:
                continue
        else:
            return False, old_num
    except Exception:
        return False, old_num
    today = ref_date or datetime.now().date()
    if isinstance(today, datetime):
        today = today.date()
    return exp_dt.date() >= today, old_num


def _is_valid_utility_name(name):
    """Valida que uma string é nome real de utility (não lixo de UI do portal).

    Rejeita:
      - Vazio, muito curto (<3 chars)
      - Códigos de ID puros (ex: "NI0005", "ID8000") — letras+números sem espaço
      - Botões/filtros da UI do portal Indiana: "All (6)", "Current (3)",
        "Event", "Positive Response", "No Response", "Show all", etc.
      - Cabeçalhos de tabela: "Status", "Date", "Service Area", "Response"

    Exemplos válidos: "DUKE ENERGY", "COMCAST NORTH", "IN AMERICAN WATER"
    Exemplos inválidos: "All (6)", "Current", "NI0005", "ID2227", "Event"
    """
    if not name:
        return False
    n = str(name).strip()
    if len(n) < 3:
        return False
    # Prefixo ID ex: "ID2227"
    if n.startswith("ID") and re.match(r"^ID\d+$", n):
        return False
    # Código puro letras+números sem espaço: "NI0005", "ID8000"
    # Ou sigla curta só-letras (≤5 chars): "COMCN", "AEP", "TECO"
    if re.match(r"^[A-Z0-9]{2,10}$", n) and (re.search(r"[0-9]", n) or len(n) <= 5):
        return False
    # Lixo da UI do portal: "All (6)", "Current (3)", "Show all (9)"
    if re.match(r"^(All|Current|Show\s+all|Show|Hide|Filter|Previous|Next|Page)\s*\(?\s*\d*\s*\)?\s*$", n, re.IGNORECASE):
        return False
    # Labels de eventos/status puros
    ui_labels = {
        "event", "events", "status", "date", "service area", "response", "responses",
        "positive response", "no response", "entry method", "comments", "comment",
        "web service", "utility", "utilities", "utility name", "service",
        "current", "history", "ticket", "actions", "action",
    }
    if n.lower() in ui_labels:
        return False
    return True


def needs_private_locator(response_text):
    """Detecta se a resposta indica necessidade de private locator (3H)."""
    r = (response_text or "").strip().lower()
    return "3h" in r or "privately owned" in r or "private facility owner" in r


def needs_watch_and_protect(response_text):
    """Detecta se a resposta indica Watch and Protect (código 60 — IL/JULIE).

    W&P = utility tem instalação crítica e exige representante presente durante escavação.
    """
    r = (response_text or "").strip().lower()
    return "watch and protect" in r


# ── │ SECTION: LOCATION_TEXT │ EXTRAIR LOCATION TEXT DO BODY (IN + FL) ────────
def extract_location_text(body, state=""):
    """Extrai campo de localização do body do ticket."""
    if not body:
        return ""

    body_norm = re.sub(r'Locat\s*:', 'Locat:', body, flags=re.IGNORECASE)

    loc_idx = -1
    loc_offset = 0
    for loc_label in ["Locat:", "Location:"]:
        idx = body_norm.find(loc_label)
        if idx >= 0:
            loc_idx = idx
            loc_offset = len(loc_label)
            break

    if loc_idx < 0:
        return ""

    raw = body_norm[loc_idx + loc_offset:loc_idx + 2000]

    stop_pat = (
        r"(.*?)"
        r"(?:"
        r"\n\s*\n"
        r"|\nGrids|\nBoundary|\nService"
        r"|\*\*\*"
        r"|\nBoring|\nRemarks"
        r"|\nWork\s*[Dd]ate|\nDue\s*[Dd]ate|\nWork\s*[Tt]ype"
        r"|\nDone\s*[Ff]or|\nCompany|\nCategory"
        r"|\nHrs\s*notc|\nWhite-lined|\nUg/Oh|\nDepth"
        r"|\n\s*:\s*\n"
        r"|\nType\s*[Oo]f\s*[Ww]ork"
        r"|\nDuration|\nDig\s*[Ss]ite|\nStart\s*[Dd]ate"
        r"|\nExpir|\nTicket\s*#|\nFunction"
        r"|\n[A-Z][A-Za-z ]{2,25}:"
        r")"
    )
    m = re.search(stop_pat, raw, re.DOTALL)
    if m:
        return " ".join(m.group(1).split()).strip()
    return " ".join(raw[:800].split()).strip()


# ── │ SECTION: EXPIRE_PARSE │ EXTRAIR DATA DE VENCIMENTO DO BODY ──────────────
def extract_expire_date(body, ticket_num="", debug=False):
    """Extrai a data de vencimento REAL do ticket 811.

    IMPORTANTE: há 3 datas diferentes que podem aparecer no body:
      - Expires / Expiration Date / Ticket Expires / Exp Date → data real (correta)
      - Due Date / Due                                         → deadline pras utilities
      - Work Date / Start / Legal Date                         → data de início

    Formatos conhecidos:
      - Indiana 811:   "Ticket Expires: 05/13/2026 11:59 PM"
      - Sunshine FL:   "Due Date : 04/15/26 Time: 23:59ET  Exp Date : 05/13/26 Time: 23:59ET"
      - JULIE IL:      expire não está no body texto (retorna "")

    Sempre retorna normalizado como "MM/DD/YYYY" ou "" se não achar.
    """
    def _extract(pattern, text):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1).strip() if m else ""

    # PRIORIDADE: do mais específico/confiável pro mais genérico.
    # "Ticket Expires" antes de "Expires" pra evitar match parcial.
    # "Exp Date" é Sunshine FL — ANTES de genéricos pra não deixar passar.
    # O "?:\s{2,}|\t" no pattern Exp Date evita capturar o "Due Date" que vem na mesma linha.
    # NÃO incluir "Due Date", "Work Date", "Legal Date" — são datas DIFERENTES.
    patterns = [
        (r"Ticket\s+Expires?\s*(?:on)?\s*:\s*([^\n]+)",             "Ticket Expires"),
        (r"Expiration\s+Date\s*:\s*([^\n]+)",                        "Expiration Date"),
        (r"Expiration\s*:\s*([^\n]+)",                                "Expiration"),
        (r"(?<!\w)Expires?\s+on\s*:\s*([^\n]+)",                      "Expires on"),
        (r"(?<!\w)Expires\s*:\s*([^\n]+)",                            "Expires"),
        # Sunshine FL: "Exp Date : 05/13/26 Time: 23:59ET"
        # Usa lookahead pra parar em outro label ou fim de linha.
        (r"(?<!\w)Exp\s+Date\s*:\s*(.+?)(?=\s{2,}\w+\s*Date|\s{2,}Hrs|\s{2,}Work|\n|$)",  "Exp Date"),
        (r"(?<!\w)Expire\s*:\s*([^\n]+)",                             "Expire"),
    ]

    expire_raw = ""
    matched_label = ""
    for pat, label in patterns:
        expire_raw = _extract(pat, body)
        if expire_raw:
            matched_label = label
            break

    if not expire_raw:
        if debug:
            log.warning(f"[ExpireParse] {ticket_num}: nenhum pattern de Expires bateu no body")
        return ""

    if debug:
        log.info(f"[ExpireParse] {ticket_num}: '{matched_label}' → raw='{expire_raw[:80]}'")

    # LIMPEZAS:
    # 1. Remove " at " que alguns portais injetam: "05/13/2026 at 11:59 PM" → "05/13/2026 11:59 PM"
    cleaned = re.sub(r"\s+at\s+", " ", expire_raw, flags=re.IGNORECASE)
    # 2. Remove "Time:" literal do formato Sunshine FL: "05/13/26 Time: 23:59ET" → "05/13/26 23:59ET"
    cleaned = re.sub(r"\s*Time\s*:\s*", " ", cleaned, flags=re.IGNORECASE)
    # 3. Remove sufixos de timezone colados ou separados: "23:59ET" → "23:59", "23:59 EST" → "23:59"
    cleaned = re.sub(r"\s*(ET|EST|EDT|CT|CST|CDT|PT|PST|PDT|MT|MST|MDT|UTC|GMT)\b", "", cleaned, flags=re.IGNORECASE)
    # 4. Normaliza espaços múltiplos
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    # Pega as primeiras ~5 palavras (suficiente pra data + hora + AM/PM)
    expire_clean = " ".join(cleaned.split()[:5])

    # Tenta formatos do MAIS ESPECÍFICO pro MAIS GENÉRICO
    formats = [
        "%m/%d/%Y %I:%M:%S %p",   # 05/13/2026 11:59:00 PM
        "%m/%d/%Y %I:%M %p",       # 05/13/2026 11:59 PM
        "%m/%d/%Y %H:%M:%S",       # 05/13/2026 23:59:00
        "%m/%d/%Y %H:%M",          # 05/13/2026 23:59
        "%m/%d/%y %H:%M",          # 05/13/26 23:59       (Sunshine FL)
        "%m/%d/%y %I:%M %p",       # 05/13/26 11:59 PM
        "%m/%d/%y %I:%M:%S %p",    # 05/13/26 11:59:00 PM
        "%B %d, %Y %I:%M %p",      # May 13, 2026 11:59 PM
        "%b %d, %Y %I:%M %p",      # May 13, 2026 11:59 PM (abbrev)
        "%B %d, %Y",                # May 13, 2026
        "%b %d, %Y",                # May 13, 2026
        "%m/%d/%Y",                 # 05/13/2026
        "%m-%d-%Y",                 # 05-13-2026
        "%Y-%m-%d",                 # 2026-05-13
        "%m/%d/%y",                 # 05/13/26
    ]
    # Tenta parsear com cada formato pegando prefixo do tamanho apropriado.
    for fmt in formats:
        for attempt_len in [len(fmt) + 5, len(fmt) + 2, len(expire_clean)]:
            try:
                parsed = datetime.strptime(expire_clean[:attempt_len].strip(), fmt)
                return parsed.strftime("%m/%d/%Y")
            except ValueError:
                continue

    # Último recurso: extrai qualquer padrão MM/DD/YYYY ou MM/DD/YY
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", expire_clean)
    if m:
        for fmt in ["%m/%d/%Y", "%m/%d/%y"]:
            try:
                return datetime.strptime(m.group(1), fmt).strftime("%m/%d/%Y")
            except ValueError:
                continue

    if debug:
        log.warning(f"[ExpireParse] {ticket_num}: não parseou '{expire_clean}' com nenhum formato")
    return ""  # Nunca retornar string parcial/crua — melhor vazio


def normalize_expire(s):
    """Normaliza qualquer valor de expire pra 'MM/DD/YYYY' ou ''.

    Aceita:
      - 'MM/DD/YYYY'               → retorna como está
      - 'MM/DD/YY Time: HH:MM'     → legado poluído, limpa
      - 'MM/DD/YY HH:MMET'         → formato Sunshine bruto
      - '05/13/26'                 → converte pra '05/13/2026'
      - '' / None / '—'            → retorna ''

    Usa as mesmas regras do extract_expire_date nas partes de limpeza,
    mas opera sobre um valor já extraído (não sobre body cru).
    Garante que banco/app sempre tenham MM/DD/YYYY consistente.
    """
    if not s:
        return ""
    s = str(s).strip()
    if s in ("—", "-", "N/A", "null", "None"):
        return ""
    # Aplica mesmas limpezas do extract_expire_date
    cleaned = re.sub(r"\s+at\s+", " ", s, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*Time\s*:\s*", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*(ET|EST|EDT|CT|CST|CDT|PT|PST|PDT|MT|MST|MDT|UTC|GMT)\b", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    prefix = " ".join(cleaned.split()[:5])
    formats = [
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
        "%m/%d/%y %H:%M", "%m/%d/%y %I:%M %p", "%m/%d/%y %I:%M:%S %p",
        "%B %d, %Y %I:%M %p", "%b %d, %Y %I:%M %p",
        "%B %d, %Y", "%b %d, %Y",
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%m/%d/%y",
    ]
    for fmt in formats:
        for attempt_len in [len(fmt) + 5, len(fmt) + 2, len(prefix)]:
            try:
                return datetime.strptime(prefix[:attempt_len].strip(), fmt).strftime("%m/%d/%Y")
            except ValueError:
                continue
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", prefix)
    if m:
        for fmt in ["%m/%d/%Y", "%m/%d/%y"]:
            try:
                return datetime.strptime(m.group(1), fmt).strftime("%m/%d/%Y")
            except ValueError:
                continue
    return ""


def _is_polluted_expire(s):
    """Detecta formato antigo/poluído que deveria ser re-scrapado."""
    if not s:
        return False
    s = str(s)
    if "Time:" in s or re.search(r"\b(ET|EST|EDT)\b", s):
        return True
    # Se não é MM/DD/YYYY limpo, considera poluído
    if not re.fullmatch(r"\d{2}/\d{2}/\d{4}", s.strip()):
        return True
    return False


# ── │ SECTION: CANCEL_DETECT │ DETECTAR CANCELAMENTO ──────────────────────────
def is_ticket_canceled(body):
    if not body:
        return False
    first_line = body.strip().split("\n")[0].strip().upper()
    body_upper = body.upper()
    return (
        first_line.startswith("CANCEL")
        or "FUNCTION: CANCEL" in body_upper
        or "FUNCTION:\nCANCEL" in body_upper
        or ("CANCELED" in body_upper and "REPLACED BY TICKET NUMBER" in body_upper)
        or "DUE TO TICKET BEING CANCELED" in body_upper
    )


# ── │ SECTION: COORDS_ADJUST │ AJUSTE DE POSIÇÃO PELO TEXTO 811 ──────────────
def adjust_coords_by_location(lat, lon, location_text, tipo_out=None):
    if not lat or not lon or not location_text:
        return lat, lon, tipo_out or "Main line"

    text = location_text.upper()
    OFFSET = 0.00012

    is_service = any(w in text for w in ["LATERAL", " REAR ", "REAR OF", "FRONT OF", " FRONT "])
    if is_service:
        tipo = "Service"
        if "NORTH" in text or " N " in text:
            return round(lat + OFFSET, 6), lon, tipo
        if "SOUTH" in text or " S " in text:
            return round(lat - OFFSET, 6), lon, tipo
        if "EAST" in text or " E " in text:
            return lat, round(lon + OFFSET, 6), tipo
        if "WEST" in text or " W " in text:
            return lat, round(lon - OFFSET, 6), tipo
        return round(lat + OFFSET * 0.5, 6), lon, tipo

    if "R/O/W" in text or "ROW TO ROW" in text or "R.O.W" in text:
        return lat, lon, "Main line"

    tipo = "Main line"
    if any(p in text for p in ["NORTH SIDE", "N SIDE", "NORTHSIDE", "ON NORTH", "ALONG NORTH"]):
        return round(lat + OFFSET, 6), lon, tipo
    if any(p in text for p in ["SOUTH SIDE", "S SIDE", "SOUTHSIDE", "ON SOUTH", "ALONG SOUTH"]):
        return round(lat - OFFSET, 6), lon, tipo
    if any(p in text for p in ["EAST SIDE", "E SIDE", "EASTSIDE", "ON EAST", "ALONG EAST"]):
        return lat, round(lon + OFFSET, 6), tipo
    if any(p in text for p in ["WEST SIDE", "W SIDE", "WESTSIDE", "ON WEST", "ALONG WEST"]):
        return lat, round(lon - OFFSET, 6), tipo
    return lat, lon, tipo


# ── │ SECTION: GEOCODING │ GEOCODING ──────────────────────────────────────────
async def geocode_address(street, city, state_code):
    import aiohttp
    STATE_BOUNDS = {
        "IN": {"lat": (37.77, 41.76), "lon": (-88.10, -84.79)},
        "FL": {"lat": (24.39, 31.00), "lon": (-87.63, -79.97)},
    }
    query = f"{street}, {city}, {state_code}, USA"
    try:
        # Fix bug #14: Nominatim usage policy exige MAX 1 req/segundo por IP.
        # 0.3s violava o TOS e podia levar a ban do IP em produção. 1.1s dá margem.
        # Ver: https://operations.osmfoundation.org/policies/nominatim/
        await asyncio.sleep(1.1)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": query, "format": "json", "limit": 5, "countrycodes": "us"},
                headers={"User-Agent": "OneDrill-811-Sync/2.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                results = await r.json()

        bounds = STATE_BOUNDS.get(state_code, {})
        lat_range = bounds.get("lat", (-90, 90))
        lon_range = bounds.get("lon", (-180, 180))

        for item in results:
            lat, lon = float(item["lat"]), float(item["lon"])
            if lat_range[0] <= lat <= lat_range[1] and lon_range[0] <= lon <= lon_range[1]:
                return lat, lon
        if city:
            return await geocode_address(street, "", state_code)
    except Exception as e:
        log.debug(f"Geocoding error for '{query}': {e}")
    return None, None


# ── │ SECTION: COUNTY_RESOLVER │ COUNTY RESOLVER (Fase 1 filtro por county) ──
# Resolve o county de um ticket a partir de (location, state, lat, lon).
# Estratégias em cascata: 1) lookup direto 2) fuzzy 3) Nominatim reverse (fallback).

_COUNTIES_DB = None  # carregado lazy

def _load_counties_db():
    """Carrega counties_data.json uma vez. Se arquivo não existe, retorna {} e loga warning."""
    global _COUNTIES_DB
    if _COUNTIES_DB is not None:
        return _COUNTIES_DB
    path = os.path.join(BASE_DIR, "counties_data.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            _COUNTIES_DB = _json.load(f)
        total = sum(len(v) for v in _COUNTIES_DB.values())
        log.info(f"[county] Base carregada: {total} cidades ({', '.join(f'{k}={len(v)}' for k,v in _COUNTIES_DB.items())})")
    except FileNotFoundError:
        log.warning(f"[county] Arquivo counties_data.json não encontrado em {path} — só fallback Nominatim será usado")
        _COUNTIES_DB = {}
    except Exception as e:
        log.error(f"[county] Erro carregando counties_data.json: {e}")
        _COUNTIES_DB = {}
    return _COUNTIES_DB


def _normalize_city_name(name):
    """Normaliza nome de cidade pra lookup: lowercase, remove pontuação, trim, expande abreviações comuns."""
    if not name:
        return ""
    n = name.lower().strip()
    # Insere espaço antes de letras quando grudado em pontuação (ex: "st.petersburg" → "st. petersburg")
    n = re.sub(r"\.(\w)", r". \1", n)
    # Remove pontuação comum
    n = re.sub(r"[.,;'\"]", "", n)
    n = re.sub(r"\s+", " ", n).strip()
    return n


def _city_variants(name):
    """Gera variantes de busca pra uma cidade: com/sem 'saint', 'fort', com/sem pontuação."""
    base = _normalize_city_name(name)
    variants = {base}
    # st ↔ saint
    if base.startswith("st "):
        variants.add("saint " + base[3:])
    if base.startswith("saint "):
        variants.add("st " + base[6:])
    # ft ↔ fort
    if base.startswith("ft "):
        variants.add("fort " + base[3:])
    if base.startswith("fort "):
        variants.add("ft " + base[5:])
    return variants


async def _reverse_geocode_county(lat, lon):
    """Fallback: Nominatim reverse geocoding pra pegar county.
    Respeita o rate limit 1.1s (fix #14) — compartilha com geocode_address.
    """
    import aiohttp
    try:
        await asyncio.sleep(1.1)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "https://nominatim.openstreetmap.org/reverse",
                params={"lat": lat, "lon": lon, "format": "json", "zoom": 10, "addressdetails": 1},
                headers={"User-Agent": "OneDrill-811-Sync/2.0"},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as r:
                data = await r.json()
        addr = data.get("address", {}) or {}
        # Nominatim retorna "county" na maioria dos casos; às vezes "Pinellas County" vem com "County" no fim
        county = addr.get("county") or ""
        if county:
            # Remove sufixo " County" se existir — salvamos só o nome
            county = re.sub(r"\s+County\s*$", "", county, flags=re.IGNORECASE).strip()
        return county or None
    except Exception as e:
        log.debug(f"[county] Reverse geocode error lat={lat},lon={lon}: {e}")
        return None


def _extract_city_candidates(location):
    """Extrai candidatos a 'cidade' do campo location, cobrindo os formatos reais vistos:

    - "St. Petersburg, FL"          → ["St. Petersburg"]
    - "Orlando - Tangelo Park"      → ["Orlando", "Tangelo Park"]          (City - Neighborhood)
    - "Vigo - Terre Haute"          → ["Vigo", "Terre Haute"]              (County - City)
    - "ILLINOIS - ELK GROVE"        → ["ILLINOIS", "ELK GROVE"]            (STATE - City)
    - "Pinelas - St. Petersburg"    → ["Pinelas", "St. Petersburg"]        (County typo - City)
    - "St. Petesburg"               → ["St. Petesburg"]                    (City com typo)

    Retorna lista em ordem de tentativa, sem duplicatas.
    """
    if not location:
        return []
    candidates = []
    loc = location.strip()
    # Primeiro segmento antes de vírgula (tira "FL" / "IN" etc do final)
    first_comma = loc.split(",")[0].strip()
    # Se tem hífen, adiciona AMBOS os lados como candidatos
    if " - " in first_comma:
        parts = [p.strip() for p in first_comma.split(" - ") if p.strip()]
        for p in parts:
            if p and p not in candidates:
                candidates.append(p)
    else:
        # Sem hífen, o primeiro segmento inteiro é o candidato
        if first_comma and first_comma not in candidates:
            candidates.append(first_comma)
    return candidates


def _fuzzy_city_match(city_norm, state_db, max_distance=2):
    """Busca cidade na base tolerando pequenas variações (typos).
    Usa distância de Levenshtein simples. max_distance=2 pega casos como:
      'pinelas' → 'pinellas' (dist 1)
      'petesburg' → 'petersburg' (dist 1)
      'jaksonvile' → 'jacksonville' (dist 2)

    Retorna o county se achou match bom, "" caso contrário.
    """
    if not city_norm or not state_db:
        return ""

    def _lev(a, b):
        # Levenshtein iterativo (O(m*n) tempo, O(min(m,n)) memória)
        if len(a) < len(b):
            a, b = b, a
        if not b:
            return len(a)
        # Early exit: se diferença de tamanho já passou o threshold, skip
        if len(a) - len(b) > max_distance:
            return max_distance + 1
        prev_row = list(range(len(b) + 1))
        for i, ca in enumerate(a, 1):
            curr_row = [i]
            for j, cb in enumerate(b, 1):
                ins = curr_row[j - 1] + 1
                dele = prev_row[j] + 1
                sub = prev_row[j - 1] + (ca != cb)
                curr_row.append(min(ins, dele, sub))
            prev_row = curr_row
        return prev_row[-1]

    # Só tenta fuzzy em nomes razoáveis (evita ruído com strings curtas)
    if len(city_norm) < 5:
        return ""

    best_match = None
    best_dist = max_distance + 1
    for city_in_db, county in state_db.items():
        # Só compara com nomes de tamanho similar (otimização)
        if abs(len(city_in_db) - len(city_norm)) > max_distance:
            continue
        d = _lev(city_norm, city_in_db)
        if d < best_dist:
            best_dist = d
            best_match = (city_in_db, county)
            if d == 0:
                break

    return best_match[1] if best_match else ""


async def resolve_county(location, state, lat=None, lon=None):
    """Resolve county de um ticket. Estratégias em cascata:
       1) Lookup exato (cobre 'St. Petersburg, FL', 'Tampa, Hillsborough')
       2) Múltiplos candidatos do location (cobre 'Orlando - Tangelo Park', 'Vigo - Terre Haute')
       3) Variantes (st↔saint, ft↔fort)
       4) Fuzzy match (cobre typos: 'Pinelas' → Pinellas, 'Petesburg' → Petersburg)
       5) Nominatim reverse geocoding se tem lat/lon

    Retorna string (ex: "Pinellas") ou "" se não conseguiu resolver.
    """
    if not state:
        return ""
    state = state.upper()
    db = _load_counties_db()
    state_db = db.get(state, {})

    # Extrai todos os candidatos a cidade do location
    candidates = _extract_city_candidates(location or "")

    # Estratégia 1 + 2 + 3: testa cada candidato com todas as variantes
    for candidate in candidates:
        for variant in _city_variants(candidate):
            if variant in state_db:
                return state_db[variant]

    # Estratégia 4: fuzzy match — tenta typos nos candidatos
    for candidate in candidates:
        candidate_norm = _normalize_city_name(candidate)
        fuzzy_county = _fuzzy_city_match(candidate_norm, state_db)
        if fuzzy_county:
            log.info(f"[county] fuzzy match: {candidate!r} → {fuzzy_county} (FL)")
            return fuzzy_county

    # Estratégia 5: fallback via reverse geocoding
    if lat is not None and lon is not None:
        try:
            lat_f = float(lat)
            lon_f = float(lon)
            county = await _reverse_geocode_county(lat_f, lon_f)
            if county:
                log.info(f"[county] {location!r}, {state}: resolvido via Nominatim → {county}")
                return county
        except (TypeError, ValueError):
            pass

    log.debug(f"[county] Não resolvido: location={location!r}, state={state}, lat={lat}, lon={lon}")
    return ""


# ── │ SECTION: AUTO_LOGIN │ AUTO-LOGIN ────────────────────────────────────────


# Helpers de humanizacao (anti-detect)
async def _human_wait(min_s=0.5, max_s=2.0):
    import random as _rnd
    await asyncio.sleep(_rnd.uniform(min_s, max_s))


async def _human_type_into(page, selector, text):
    import random as _rnd
    try:
        el = page.locator(selector).first
        await el.click()
        await _human_wait(0.2, 0.5)
        for ch in text:
            await page.keyboard.type(ch, delay=_rnd.randint(60, 160))
        return True
    except Exception as e:
        log.debug(f"[type] erro em {selector}: {e}")
        return False


async def auto_login_silent(state):
    log.info(f"[{state}] Tentando auto-login silent (anti-detect)...")

    if state not in PORTALS:
        log.error(f"[{state}] estado nao suportado")
        return False

    user = PORTALS[state]["user"]()
    passwd = PORTALS[state]["pass"]()
    if not user or not passwd:
        log.error(f"[{state}] Credenciais ausentes no .env")
        return False

    perfil = _profile_path(state)

    stealth_fn = None
    try:
        from playwright_stealth import stealth_async as _stealth
        stealth_fn = _stealth
    except ImportError:
        log.warning("[stealth] playwright-stealth nao instalado - mais detectavel")

    try:
        async with async_playwright() as p:
            ctx = await p.chromium.launch_persistent_context(
                perfil,
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                ],
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/131.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="America/New_York",
            )
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()

            if stealth_fn:
                try:
                    await stealth_fn(page)
                except Exception:
                    pass

            page.set_default_timeout(TIMEOUT_PAGE)
            await page.goto(PORTALS[state]["url"], wait_until="domcontentloaded")
            await wait_stable(page)
            await _human_wait(1.5, 3.5)

            url_now = page.url.lower()
            if "login" not in url_now and "signin" not in url_now and "auth" not in url_now:
                log.info(f"[{state}] Ja logado via cookie - URL: {page.url}")
                await ctx.close()
                return True

            email_sels = [
                'input[type="email"]',
                'input[name="email"]',
                'input[name="username"]',
                'input[id*="email"]',
                'input[id*="user"]',
                "#email",
                "#username",
            ]
            pass_sels = [
                'input[type="password"]',
                'input[name="password"]',
                'input[id*="password"]',
                "#password",
            ]

            email_input = None
            for sel in email_sels:
                if await page.locator(sel).count():
                    email_input = sel
                    break
            if not email_input:
                log.error(f"[{state}] Campo email nao encontrado")
                await ctx.close()
                return False

            pass_input = None
            for sel in pass_sels:
                if await page.locator(sel).count():
                    pass_input = sel
                    break
            if not pass_input:
                log.error(f"[{state}] Campo senha nao encontrado")
                await ctx.close()
                return False

            log.debug(f"[{state}] Campos: email={email_input}, pass={pass_input}")

            if not await _human_type_into(page, email_input, user):
                await ctx.close()
                return False
            await _human_wait(0.5, 1.2)

            if not await _human_type_into(page, pass_input, passwd):
                await ctx.close()
                return False
            await _human_wait(0.8, 1.8)

            submit_clicked = False
            for sel in [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("Login")',
                'button:has-text("Sign In")',
                'button:has-text("Submit")',
                'button:has-text("Log In")',
            ]:
                if await page.locator(sel).count():
                    try:
                        await page.locator(sel).first.click()
                        submit_clicked = True
                        break
                    except Exception:
                        continue
            if not submit_clicked:
                try:
                    await page.locator(pass_input).first.press("Enter")
                    submit_clicked = True
                except Exception:
                    pass

            if not submit_clicked:
                log.error(f"[{state}] Nao consegui submeter")
                await ctx.close()
                return False

            await _human_wait(2, 4)
            try:
                await page.wait_for_load_state("networkidle", timeout=30000)
            except Exception:
                pass
            await wait_stable(page)

            if "login" in page.url.lower() or "signin" in page.url.lower():
                log.error(f"[{state}] Login falhou - URL: {page.url}")
                await ctx.close()
                return False

            try:
                await page.goto(PORTALS[state]["dashboard"], wait_until="domcontentloaded")
                await wait_stable(page)
            except Exception:
                pass

            if "login" in page.url.lower():
                log.error(f"[{state}] Dashboard redireciona pra login")
                await ctx.close()
                return False

            log.info(f"[{state}] AUTO-LOGIN SILENT OK - URL: {page.url}")
            await ctx.close()
            return True

    except Exception as e:
        log.error(f"[{state}] Erro fatal em auto_login_silent: {e}", exc_info=True)
        return False



async def auto_login(state):
    log.warning(f"[{state}] Sessão expirada — abrindo janela para renovação rápida...")
    perfil = _profile_path(state)
    try:
        async with async_playwright() as p:
            ctx2 = await p.chromium.launch_persistent_context(
                perfil, headless=False, args=["--no-sandbox", "--start-maximized"], viewport=None
            )
            page2 = ctx2.pages[0] if ctx2.pages else await ctx2.new_page()
            page2.set_default_timeout(60000)
            await page2.goto(PORTALS[state]["url"], wait_until="domcontentloaded")
            await wait_stable(page2)
            log.info(f"[{state}] Aguardando login manual (max 3 minutos)...")

            for i in range(60):
                await page2.wait_for_timeout(3000)
                url_now = page2.url.lower()
                login_keywords = ["login", "google", "cognito", "accounts", "signin"]
                if not any(kw in url_now for kw in login_keywords) and "811" in url_now:
                    log.info(f"[{state}] Login detectado! URL: {page2.url}")
                    try:
                        await page2.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
                        await wait_stable(page2)
                        await page2.goto(PORTALS[state]["dashboard"], wait_until="domcontentloaded")
                        await wait_stable(page2)
                        log.info(f"[{state}] Cookies persistidos — URL final: {page2.url}")
                    except Exception as e:
                        log.debug(f"[{state}] Navegação pós-login: {e}")
                    await page2.wait_for_timeout(4000)
                    await asyncio.sleep(3)
                    try:
                        await ctx2.close()
                    except Exception:
                        pass
                    await asyncio.sleep(2)
                    return True
                if i % 10 == 0:
                    log.info(f"[{state}] Aguardando login... ({i*3}s)")

            await ctx2.close()
            log.error(f"[{state}] Timeout — login não completado")
            return False
    except Exception as e:
        log.error(f"[{state}] Erro na renovação: {e}")
        return False


# ── │ SECTION: PORTAL_NAV │ HELPERS: NAVEGAÇÃO DO PORTAL ──────────────────────
async def goto_dashboard(page, state):
    """Navega ao dashboard e seleciona filtros corretos."""
    portal_home = PORTALS[state]["home"]
    portal_dashboard = PORTALS[state]["dashboard"]

    await page.goto(portal_home, wait_until="domcontentloaded")
    await wait_stable(page)

    for sel in ['text="Go to Ticket Dashboard"', 'a:has-text("Ticket Dashboard")']:
        if await page.locator(sel).count():
            await click_and_wait(page, page.locator(sel).first, "nav")
            break

    if "dashboard" not in page.url.lower() and "tickets" not in page.url.lower():
        await page.goto(portal_dashboard, wait_until="domcontentloaded")
        await wait_nav(page)

    if "login" in page.url.lower():
        log.warning(f"[{state}] Dashboard redirecionou para login — tentando novamente...")
        await page.goto(portal_home, wait_until="domcontentloaded")
        await wait_nav(page)
        if "login" in page.url.lower():
            return False
        await page.goto(portal_dashboard, wait_until="domcontentloaded")
        await wait_nav(page)
        if "login" in page.url.lower():
            return False

    if state == "IN":
        for sel in ['label:has-text("My Office Tickets")', 'span:has-text("My Office Tickets")', 'text="My Office Tickets"']:
            el = page.locator(sel).first
            if await el.count():
                await click_and_wait(page, el, "stable")
                break
        for sel in ['label:has-text("Show Completed Tickets")', 'text="Show Completed Tickets"', 'input[type="checkbox"]:near(:text("Completed"))']:
            el = page.locator(sel).first
            if await el.count():
                try:
                    cb = page.locator('input[type="checkbox"]').first
                    if not await cb.is_checked():
                        await click_and_wait(page, el, "stable")
                except Exception:
                    await click_and_wait(page, el, "stable")
                break
    elif state == "FL":
        selected = False
        for sel in ['label:has-text("My Office Tickets")', 'span:has-text("My Office Tickets")', 'text="My Office Tickets"']:
            el = page.locator(sel).first
            if await el.count():
                await click_and_wait(page, el, "stable")
                selected = True
                break
        if not selected:
            for sel in ['label:has-text("My Company Tickets")', 'span:has-text("My Company Tickets")', 'text="My Company Tickets"']:
                el = page.locator(sel).first
                if await el.count():
                    await click_and_wait(page, el, "stable")
                    break

    return True


async def set_items_per_page(page, count=100):
    """Muda Items/Page pra 100 no dashboard."""
    try:
        sel = page.locator('.num-per-page-selector mat-select, mat-select:near(:text("Items / Page"))')
        if await sel.count():
            await sel.first.click()
            await page.wait_for_timeout(300)
            opt = page.locator(f'mat-option:has-text("{count}")')
            if await opt.count():
                await opt.first.click()
                await wait_stable(page)
                log.info(f"Items/Page alterado para {count}")
                return True
    except Exception as e:
        log.debug(f"Não conseguiu mudar Items/Page: {e}")
    return False


async def select_office(page, state):
    """Re-seleciona My Office/Company Tickets após voltar ao dashboard."""
    if state == "IN":
        for sel in ['label:has-text("My Office Tickets")', 'text="My Office Tickets"']:
            if await page.locator(sel).count():
                await click_and_wait(page, page.locator(sel).first, "stable")
                break
    elif state == "FL":
        selected = False
        for sel in ['label:has-text("My Office Tickets")', 'text="My Office Tickets"']:
            if await page.locator(sel).count():
                await click_and_wait(page, page.locator(sel).first, "stable")
                selected = True
                break
        if not selected:
            for sel in ['label:has-text("My Company Tickets")', 'text="My Company Tickets"']:
                if await page.locator(sel).count():
                    await click_and_wait(page, page.locator(sel).first, "stable")
                    break


async def filter_ticket(page, tnum):
    """Digita número do ticket no filtro e espera resultados."""
    lbl = page.locator('mat-label:has-text("Filter by Ticket Number")')
    if await lbl.count():
        await lbl.first.click()
    else:
        await page.locator('#mat-input-0').click(force=True)
    await page.wait_for_timeout(80)
    await page.keyboard.press("Control+a")
    await page.keyboard.type(tnum, delay=8)
    # Espera o ticket aparecer na lista em vez de networkidle genérico (mais rápido)
    try:
        await page.get_by_text(tnum, exact=True).first.wait_for(timeout=4000)
    except Exception:
        await page.wait_for_timeout(800)


async def back_to_dashboard(page, state):
    """Volta ao dashboard rapidamente."""
    await page.goto(PORTALS[state]["dashboard"], wait_until="domcontentloaded")
    await wait_stable(page)
    await select_office(page, state)


async def ensure_login(page, ctx, p, state):
    """Verifica login e renova se necessário. Retorna (page, ctx) atualizados."""
    perfil = _profile_path(state)
    if "login" in page.url.lower():
        await ctx.close()
        await asyncio.sleep(1)
        # Fix 2026-05-15: tenta auto_login_silent primeiro (full auto)
        ok = await auto_login_silent(state)
        if not ok:
            log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")
            ok = await auto_login(state)
        if not ok:
            return None, None
        await asyncio.sleep(1)
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)
        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)
        if "login" in page.url.lower():
            log.error(f"[{state}] Login falhou após renovação — cookies não persistiram")
            await ctx.close()
            return None, None
        log.info(f"[{state}] Sessão renovada com sucesso: {page.url}")
    return page, ctx


async def fast_back(page, state):
    """Volta ao dashboard usando go_back (mais rápido que goto completo)."""
    try:
        await page.go_back(wait_until="domcontentloaded")
        await page.wait_for_timeout(300)
        url = page.url.lower()
        if "dashboard" in url or ("tickets" in url and "ticket/" not in url):
            return
    except Exception:
        pass
    await back_to_dashboard(page, state)


# ── │ SECTION: SCRAPE │ SCRAPE (PARALELO) ────────────────────────────────────
async def scrape(state, ticket_numbers, tickets_data=None):
    results = {}
    perfil = _profile_path(state)

    # Tickets que precisam de notes (Text tab)
    needs_notes = set()
    if tickets_data:
        for t in tickets_data:
            notes = (t.get("notes") or "").strip()
            if not notes or notes == "[811 Location]":
                needs_notes.add(t["ticket"])

    # Divide em chunks pras abas paralelas
    chunks = [[] for _ in range(NUM_TABS)]
    for i, tnum in enumerate(ticket_numbers):
        chunks[i % NUM_TABS].append(tnum)
    chunks = [c for c in chunks if c]

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)

        # Verifica login
        if "login" in page.url.lower():
            await ctx.close()
            await asyncio.sleep(1)
            ok = await auto_login_silent(state)

            if not ok:

                log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")

                ok = await auto_login(state)
            if not ok:
                return results
            await asyncio.sleep(1)
            ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)
            await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
            await wait_stable(page)
            if "login" in page.url.lower():
                log.error(f"[{state}] Scrape: login falhou após renovação")
                await ctx.close()
                return results

        log.info(f"[{state}] Logado! Abrindo {len(chunks)} abas paralelas...")

        async def process_chunk(chunk, tab_id):
            """Processa um grupo de tickets em uma aba independente."""
            tab_results = {}
            if not chunk:
                return tab_results

            pg = await ctx.new_page()
            pg.set_default_timeout(60000)

            ok = await goto_dashboard(pg, state)
            if not ok:
                log.error(f"[{state}][T{tab_id}] Dashboard inacessível — abortando aba")
                try:
                    await pg.close()
                except Exception:
                    pass
                return tab_results

            log.info(f"[{state}][T{tab_id}] Dashboard OK — {len(chunk)} tickets")

            for idx, tnum in enumerate(chunk):
                log.info(f"[{state}][T{tab_id}] ({idx+1}/{len(chunk)}) Ticket {tnum}")
                result = {"location_text": "", "responses": [], "expire_date": ""}
                MAX_RETRIES = 2

                for _attempt in range(MAX_RETRIES):
                    try:
                        await filter_ticket(pg, tnum)

                        if not await pg.get_by_text(tnum, exact=True).count():
                            log.warning(f"[{state}] {tnum}: não encontrado")
                            tab_results[tnum] = result
                            break

                        await pg.get_by_text(tnum, exact=True).first.click()
                        await wait_stable(pg)

                        # Text tab — SEMPRE lê body pra extrair expire_date;
                        # location_text só se precisa de notes
                        tt = pg.locator('[role="tab"]:has-text("Text")').first
                        if await tt.count():
                            await click_and_wait(pg, tt, "tab")
                        body = await pg.locator("body").inner_text()
                        result["expire_date"] = extract_expire_date(body)
                        if tnum in needs_notes or not tickets_data:
                            result["location_text"] = extract_location_text(body, state=state)

                        # Responses tab
                        rt = pg.locator('[role="tab"]:has-text("Responses")').first
                        if await rt.count():
                            await rt.click()
                            try:
                                await pg.wait_for_selector(
                                    'text="Loading ticket responses..."',
                                    state="hidden", timeout=6000
                                )
                            except Exception:
                                pass
                            await wait_tab_content(pg)

                            await _dismiss_dialog(pg, "Marking delay", label=tnum)

                            # Clica "All" pra capturar TODAS as utilities
                            filter_clicked = False
                            all_links = pg.locator('text=/^All \\(/')
                            if await all_links.count():
                                await click_and_wait(pg, all_links.first, "filter")
                                filter_clicked = True

                            if not filter_clicked:
                                cur_links = pg.locator('text=/^Current Only/')
                                if await cur_links.count():
                                    await click_and_wait(pg, cur_links.first, "filter")
                                    filter_clicked = True

                            if not filter_clicked:
                                for sel in ['button:has-text("All")', 'a:has-text("All")']:
                                    if await pg.locator(sel).count():
                                        await click_and_wait(pg, pg.locator(sel).first, "filter")
                                        break

                            await wait_stable(pg)
                            body_text = await pg.locator("body").inner_text()

                            # Salva texto bruto pra debug.
                            # - DEBUG_MODE=1: salva os 2 primeiros de cada aba (amostragem)
                            # - ONEDRILL_DEBUG_TICKET=XXX: força salvar só esse ticket específico
                            should_debug = False
                            if DEBUG_TICKET and str(tnum).strip() == DEBUG_TICKET:
                                should_debug = True
                            elif DEBUG_MODE and idx < 2 and tab_id == 0:
                                should_debug = True
                            if should_debug:
                                with open(f"debug_responses_{state}_{tnum}.txt", "w", encoding="utf-8") as df:
                                    df.write(body_text)
                                log.info(f"[{state}] {tnum}: DEBUG salvo em debug_responses_{state}_{tnum}.txt")

                            # Parse responses (resilient to whitespace/order changes)
                            resp_idx = -1
                            resp_patterns = [
                                r"Status\s+Date\s+Service\s+Area\s+Response",
                                r"Service\s+Area",
                                r"Utility\s+Name",
                            ]
                            for rp in resp_patterns:
                                rm = re.search(rp, body_text, re.IGNORECASE)
                                if rm:
                                    resp_idx = rm.start()
                                    break
                            if resp_idx >= 0:
                                resp_section = body_text[resp_idx:resp_idx + 50000]
                                linhas = resp_section.split("\n")
                                i = 0
                                while i < len(linhas):
                                    linha = linhas[i].strip()
                                    if linha in ["Status", "Date", "Service Area", "Response", "Entry Method", "Comments", ""]:
                                        i += 1
                                        continue

                                    if linha == "Event":
                                        i += 1
                                        while (i < len(linhas)
                                               and linhas[i].strip() not in ["Current", "No Response", "Positive Response", "Event"]
                                               and not linhas[i].strip().startswith("No Response")):
                                            i += 1
                                        continue

                                    if linha.startswith("No Response"):
                                        status_raw = "No Response"
                                        i += 1
                                        utility = linhas[i].strip() if i < len(linhas) else ""
                                        i += 1
                                        if i < len(linhas) and re.match(r"^[A-Z0-9]{2,10}$", linhas[i].strip()):
                                            i += 1
                                        if i < len(linhas) and linhas[i].strip().startswith("ID"):
                                            i += 1
                                        if _is_valid_utility_name(utility):
                                            _cls_status, _cls_unrec = classify(status_raw, "")
                                            result["responses"].append({
                                                "utility": utility, "status_raw": status_raw,
                                                "status": _cls_status,
                                                "response": "", "comment": "",
                                                "_unrecognized": _cls_unrec
                                            })
                                        continue

                                    if linha in ["Current", "Positive Response"]:
                                        status_raw = linha
                                        i += 1
                                        data_line = linhas[i].strip() if i < len(linhas) else ""
                                        i += 1
                                        utility = linhas[i].strip() if i < len(linhas) else ""
                                        i += 1
                                        if i < len(linhas) and re.match(r"^[A-Z0-9]{2,10}$", linhas[i].strip()):
                                            i += 1
                                        if i < len(linhas) and linhas[i].strip().startswith("ID"):
                                            i += 1
                                        response = linhas[i].strip() if i < len(linhas) else ""
                                        i += 1
                                        comment = ""
                                        while i < len(linhas):
                                            l = linhas[i].strip()
                                            if l in ["Web Service", ""] or l.startswith("Entered via"):
                                                i += 1
                                                continue
                                            if l in ["Current", "Positive Response", "Event", "No Response"] or l.startswith("No Response"):
                                                break
                                            comment = l
                                            i += 1
                                            break
                                        responded_date = None
                                        if data_line:
                                            dl = data_line.strip()[:24]
                                            for fmt in [
                                                "%m/%d/%Y %I:%M %p",   # 03/24/2026 08:50 AM
                                                "%m/%d/%Y %I:%M:%S %p",# 03/24/2026 08:50:00 AM
                                                "%m/%d/%Y %H:%M",      # 03/24/2026 08:50
                                                "%m/%d/%Y %H:%M:%S",   # 03/24/2026 08:50:00
                                                "%m/%d/%Y",            # 03/24/2026
                                                "%m/%d/%y %I:%M %p",   # 03/24/26 08:50 AM
                                                "%m/%d/%y %H:%M",      # 03/24/26 08:50
                                                "%m/%d/%y",            # 03/24/26
                                            ]:
                                                try:
                                                    responded_date = datetime.strptime(dl[:len(fmt)+2].strip(), fmt).replace(tzinfo=None).isoformat()
                                                    break
                                                except ValueError:
                                                    continue
                                        if _is_valid_utility_name(utility):
                                            _cls_status, _cls_unrec = classify(status_raw, response + " " + comment)
                                            result["responses"].append({
                                                "utility": utility, "status_raw": status_raw,
                                                "status": _cls_status,
                                                "response": response, "comment": comment,
                                                "responded_date": responded_date,
                                                "_unrecognized": _cls_unrec
                                            })
                                        continue
                                    i += 1

                            log.info(f"[{state}] {tnum}: {len(result['responses'])} utilities")

                        await fast_back(pg, state)
                        break  # Sucesso — sai do retry loop

                    except Exception as e:
                        if _attempt < MAX_RETRIES - 1 and "Timeout" in str(e):
                            log.warning(f"[{state}] {tnum}: Timeout (tentativa {_attempt+1}/{MAX_RETRIES}) — retry...")
                            try:
                                await back_to_dashboard(pg, state)
                            except Exception:
                                pass
                            continue
                        log.error(f"[{state}] {tnum}: ERRO -> {e}")
                        try:
                            await back_to_dashboard(pg, state)
                        except Exception:
                            pass

                tab_results[tnum] = result

            try:
                await pg.close()
            except Exception:
                pass
            log.info(f"[{state}][T{tab_id}] Concluído — {len(chunk)} tickets processados")
            return tab_results

        # Roda todas as abas em paralelo
        try:
            chunk_results = await asyncio.gather(*[process_chunk(c, i) for i, c in enumerate(chunks)])
            for cr in chunk_results:
                results.update(cr)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass
    return results


# ── │ SECTION: SYNC_SUMMARY │ SYNC SUMMARY (estruturado) ─────────────────────
class SyncSummary:
    """Resumo estruturado de uma execução de sync."""

    def __init__(self):
        self.cleared = 0
        self.reverted = 0
        self.private_locator = 0
        self.watch_protect = 0
        self.pending = 0
        self.confirmed_clear = 0
        self.backfilled = 0
        self.locked_skipped = 0
        self.responses_saved = 0
        self.unrecognized = 0
        self.unrecognized_list = []  # [{ticket, utility, raw_text, state}]
        self.expire_updated = 0

    def __str__(self):
        parts = []
        if self.cleared:
            parts.append(f"{self.cleared} auto-clear")
        if self.reverted:
            parts.append(f"{self.reverted} revertidos")
        if self.private_locator:
            parts.append(f"{self.private_locator} private-locator")
        if self.watch_protect:
            parts.append(f"👁 {self.watch_protect} watch-protect")
        if self.pending:
            parts.append(f"{self.pending} pendentes")
        if self.confirmed_clear:
            parts.append(f"{self.confirmed_clear} clear confirmados")
        if self.backfilled:
            parts.append(f"{self.backfilled} backfill")
        if self.locked_skipped:
            parts.append(f"{self.locked_skipped} locked")
        if self.unrecognized:
            parts.append(f"⚠ {self.unrecognized} não-reconhecidas")
        if self.expire_updated:
            parts.append(f"📅 {self.expire_updated} datas atualizadas")
        parts.append(f"{self.responses_saved} respostas salvas")
        return " | ".join(parts)


# ── │ SECTION: BATCH_PATCH │ BATCH PATCH HELPER ───────────────────────────────

def _get_latest_response_date(responses, ticket_num=""):
    """Extrai a data da ÚLTIMA utility que respondeu.

    Itera as respostas e encontra o MAX de responded_date.
    Retorna (datetime, is_fallback):
      - is_fallback=False  → data real extraída das respostas
      - is_fallback=True   → nenhuma data válida, usado datetime.now()
    """
    DATE_FMTS = [
        "%Y-%m-%dT%H:%M:%S",      # ISO
        "%Y-%m-%dT%H:%M",          # ISO short
        "%m/%d/%Y %I:%M:%S %p",    # 03/24/2026 08:50:00 AM
        "%m/%d/%Y %I:%M %p",       # 03/24/2026 08:50 AM
        "%m/%d/%Y %H:%M:%S",       # 03/24/2026 08:50:00
        "%m/%d/%Y %H:%M",          # 03/24/2026 08:50
        "%m/%d/%Y",                # 03/24/2026
        "%m/%d/%y",                # 03/24/26
    ]
    latest = None
    unparsed = []
    for resp in responses:
        rd = resp.get("responded_date")
        if not rd:
            continue
        rd_str = str(rd).strip().replace("Z", "")
        # Fix bug #20 Python: pré-normaliza datas com mês/dia sem leading zero (ex: "3/5/26" → "03/05/26").
        # Python strptime exige 2 dígitos no %m e %d — sem isso, falha silenciosamente e cai no
        # fromisoformat, que tbm falha. Tickets com data não parseada usavam datetime.now() como
        # fallback, resultando em responded_at impreciso e "gap" incorreto em análises temporais.
        rd_norm = re.sub(r'\b(\d)/(\d)/', r'0\1/0\2/', rd_str)       # 3/5/26 → 03/05/26
        rd_norm = re.sub(r'\b(\d)/(\d\d)/', r'0\1/\2/', rd_norm)     # 3/05/26 → 03/05/26
        rd_norm = re.sub(r'\b(\d\d)/(\d)/', r'\1/0\2/', rd_norm)     # 03/5/26 → 03/05/26
        parsed = None
        for fmt in DATE_FMTS:
            try:
                parsed = datetime.strptime(rd_norm[:len(fmt)+4].strip(), fmt)
                break
            except ValueError:
                continue
        if not parsed:
            try:
                parsed = datetime.fromisoformat(rd_str)
            except Exception:
                unparsed.append(rd_str)
                continue
        if parsed and (latest is None or parsed > latest):
            latest = parsed
    if latest:
        return latest, False
    if unparsed:
        log.warning(f"[DateParse] {ticket_num}: datas não parseadas: {unparsed[:5]} — usando datetime.now() como fallback")
    elif responses:
        log.warning(f"[DateParse] {ticket_num}: nenhuma resposta tem responded_date — usando datetime.now() como fallback")
    return datetime.now(), True


def sb_batch_patch(table, patches, id_field="id"):
    """Aplica múltiplos patches em batch (fix bug #5).

    Antes: loop chamava sb_patch() individualmente (N HTTP requests).
    Agora: 1 request POST com Prefer: resolution=merge-duplicates + on_conflict=<id_field>.
    Upsert pelo PK é seguro porque o PK está sempre preenchido nos patches → Supabase
    encontra a row existente e faz MERGE, não tenta INSERT vazio.

    Em caso de falha do batch (ex: RLS, NOT NULL surpresa, rede), faz fallback
    pro loop item-por-item antigo pra garantir que ao menos alguns patches passem.
    """
    if not patches:
        return

    # Valida que todos têm o id_field — sem isso, nem pode tentar batch
    valid = [p for p in patches if p.get(id_field) is not None]
    invalid_count = len(patches) - len(valid)
    if invalid_count:
        log.warning(f"[BatchPatch] {invalid_count}/{len(patches)} itens sem {id_field}, pulando")
    if not valid:
        return

    # ── Tentativa 1: BATCH REAL (1 request pra tudo) ──
    try:
        # Divide em lotes grandes pra evitar payload gigante (limite Supabase ~10MB/req)
        for i in range(0, len(valid), BATCH_SIZE):
            chunk = valid[i:i + BATCH_SIZE]
            sb_upsert(table, chunk, on_conflict=id_field)
            if i + BATCH_SIZE < len(valid):
                time.sleep(0.3)
        log.info(f"[BatchPatch] ✅ {len(valid)} patches aplicados em batch em {table}")
        return
    except Exception as e:
        log.warning(f"[BatchPatch] Batch falhou ({e}), tentando fallback 1-por-1...")

    # ── Fallback: loop antigo (seguro mas lento) ──
    success = 0
    for item in valid:
        item_copy = dict(item)
        item_id = item_copy.pop(id_field, None)
        try:
            sb_patch(table, item_id, item_copy)
            success += 1
        except Exception as e:
            log.error(f"[BatchPatch] Erro fallback em {id_field}={item_id}: {e}")
    log.info(f"[BatchPatch] {success}/{len(valid)} patches aplicados em {table} (fallback)")


# ── │ SECTION: UNRECOGNIZED │ RESPOSTAS NÃO RECONHECIDAS (salva no Supabase + alerta por email) 
def save_unrecognized_responses(items):
    """Salva respostas não reconhecidas na tabela unrecognized_responses.

    Tabela deve ser criada no Supabase (ver create_unrecognized_table.sql).
    Se a tabela não existir, apenas loga o aviso.
    """
    if not items:
        return
    now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    records = []
    for item in items:
        records.append({
            "ticket_num": item["ticket_num"],
            "state": item.get("state", ""),
            "utility_name": item.get("utility_name", ""),
            "status_raw": item.get("status_raw", ""),
            "raw_text": (item.get("raw_text") or "")[:500],
            "detected_at": now_iso,
            "resolved": False,
        })
    try:
        h = {**SB_H, "Prefer": "return=minimal"}
        _sb_request(requests.post, f"{SB_URL}/rest/v1/unrecognized_responses",
                    headers=h, json=records, timeout=15)
        log.info(f"[Unrecognized] {len(records)} respostas não reconhecidas salvas no Supabase")
    except Exception as e:
        # Se a tabela não existir, loga mas não quebra o sync
        log.warning(f"[Unrecognized] Erro ao salvar (tabela existe?): {e}")
        log.warning(f"[Unrecognized] Rode o script SQL para criar a tabela: create_unrecognized_table.sql")


def send_unrecognized_alert(state, items):
    """Envia email com resumo de respostas não reconhecidas."""
    import smtplib
    from email.mime.text import MIMEText

    gmail_user = os.getenv("GMAIL_USER")
    gmail_pass = os.getenv("GMAIL_PASS")
    alert_to = os.getenv("ALERT_EMAIL")

    if not all([gmail_user, gmail_pass, alert_to]):
        log.info("[Unrecognized] Email não configurado — alerta pulado")
        return

    subject = f"[OneDrill] ⚠ {len(items)} resposta(s) 811 não reconhecida(s) — {state}"
    body = f"OneDrill 811 Sync detectou {len(items)} resposta(s) que não batem com nenhum padrão conhecido.\n"
    body += f"Estado: {state}\n"
    body += f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
    body += "\n" + "=" * 60 + "\n\n"

    for i, item in enumerate(items, 1):
        body += f"{i}. Ticket: {item['ticket_num']}\n"
        body += f"   Utility: {item.get('utility_name', '—')}\n"
        body += f"   Status raw: {item.get('status_raw', '—')}\n"
        body += f"   Texto: {item.get('raw_text', '—')}\n\n"

    body += "=" * 60 + "\n"
    body += "\nAção necessária: verifique esses tickets manualmente no portal 811.\n"
    body += "Se o padrão se repetir, adicione-o na função classify() do 811_sync.py.\n"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = alert_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(gmail_user, gmail_pass)
            s.send_message(msg)
        log.info(f"[Unrecognized] Alerta enviado para {alert_to}")
    except Exception as e:
        log.warning(f"[Unrecognized] Erro ao enviar email: {e}")


# ── │ SECTION: SAVE │ SAVE TO SUPABASE ────────────────────────────────────────
def save_to_supabase(state, results, tickets, grace_old_map=None):
    grace_old_map = grace_old_map or {}
    ticket_map = {t["ticket"]: t for t in tickets}
    summary = SyncSummary()
    all_records = []
    ticket_patches = []  # Acumula patches pra batch no final
    unrecognized_list = []  # Respostas que caíram no fallback do classify()

    for tnum, data in results.items():
        t = ticket_map.get(tnum)

        # Ticket antigo em carência: salva respostas mas pula status update
        if not t and tnum in grace_old_map:
            tid = grace_old_map[tnum]
            latest_by_utility = {}
            for resp in data["responses"]:
                key = resp["utility"]
                if key in latest_by_utility:
                    existing = latest_by_utility[key]
                    ex_is_nr = (existing.get("status_raw") or "").lower().startswith("no response")
                    new_is_nr = (resp.get("status_raw") or "").lower().startswith("no response")
                    if ex_is_nr and not new_is_nr:
                        pass
                    elif not ex_is_nr and new_is_nr:
                        continue
                    else:
                        existing_date = existing.get("responded_date") or ""
                        new_date = resp.get("responded_date") or ""
                        if existing_date and new_date and new_date < existing_date:
                            continue
                latest_by_utility[key] = resp
            deduped = list(latest_by_utility.values())
            now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
            for resp in deduped:
                parsed_date = resp.get("responded_date")
                all_records.append({
                    "ticket_id": tid, "ticket_num": tnum, "state": state,
                    "utility_name": resp["utility"], "status": resp["status"],
                    "response_text": resp.get("response") or resp.get("comment") or resp.get("status_raw", ""),
                    "synced_at": now_iso,
                    "responded_at": parsed_date if parsed_date else now_iso,
                })
            statuses = [r["status"] for r in deduped]
            pending_names = [r["utility"] for r in deduped if r["status"] == "Pending"]
            log.info(f"[{state}] {tnum} (antigo/carência): {len(deduped)} respostas"
                     + (f" — pendentes: {pending_names}" if pending_names else " — TUDO CLEAR"))
            continue

        if not t:
            continue
        tid = t["id"]
        patch = {"id": tid}  # Acumula campos pra este ticket
        needs_patch = False

        if data["location_text"] and not (t.get("notes") or "").strip():
            patch["notes"] = f"[811 Location] {data['location_text']}"
            needs_patch = True

        # ── ATUALIZAR DATA DE VENCIMENTO (renovações) ──
        scraped_expire = normalize_expire(data.get("expire_date") or "")
        current_expire = (t.get("expire") or "").strip()
        current_normalized = normalize_expire(current_expire)
        current_is_polluted = _is_polluted_expire(current_expire)

        if scraped_expire and scraped_expire != current_normalized:
            # Novo valor válido e diferente → atualiza
            patch["expire"] = scraped_expire
            needs_patch = True
            log.info(f"[{state}] {tnum}: 📅 EXPIRE ATUALIZADO: {current_expire} → {scraped_expire}")
            summary.expire_updated += 1
        elif current_is_polluted and current_normalized and not scraped_expire:
            # Scraper não extraiu, mas podemos limpar o formato poluído do banco
            patch["expire"] = current_normalized
            needs_patch = True
            log.warning(f"[{state}] {tnum}: 🧹 EXPIRE NORMALIZADO: {current_expire} → {current_normalized}")
            summary.expire_updated += 1
        elif current_is_polluted and not current_normalized:
            log.warning(f"[{state}] {tnum}: ⚠ expire poluído '{current_expire}' não normalizável — scraper tb falhou")

        # Dedup responses por utility.
        # Regras:
        #   1. "No Response" (utility não respondeu) SEMPRE perde pra resposta real
        #   2. Resposta real NUNCA é sobrescrita por "No Response"
        #   3. Entre duas respostas reais: usa responded_date (mais recente ganha)
        #   4. Se datas não comparáveis: mantém a última (ordem do portal = cronológica)
        latest_by_utility = {}
        for resp in data["responses"]:
            key = resp["utility"]
            if key in latest_by_utility:
                existing = latest_by_utility[key]
                ex_is_nr = (existing.get("status_raw") or "").lower().startswith("no response")
                new_is_nr = (resp.get("status_raw") or "").lower().startswith("no response")
                # "No Response" SEMPRE perde pra resposta real
                if ex_is_nr and not new_is_nr:
                    log.debug(f"  [Dedup] {tnum}/{key}: No Response → {resp['status']} ({resp.get('status_raw','')})")
                elif not ex_is_nr and new_is_nr:
                    log.debug(f"  [Dedup] {tnum}/{key}: mantém {existing['status']} ({existing.get('status_raw','')}), ignora No Response")
                    continue  # Mantém resposta real, ignora No Response
                else:
                    # Ambas reais (ou ambas No Response): usa data se disponível
                    existing_date = existing.get("responded_date") or ""
                    new_date = resp.get("responded_date") or ""
                    if existing_date and new_date and new_date < existing_date:
                        log.debug(f"  [Dedup] {tnum}/{key}: mantém {existing['status']} (data {existing_date} > {new_date})")
                        continue  # Existente é mais recente
                    log.debug(f"  [Dedup] {tnum}/{key}: {existing['status']}→{resp['status']} (data: {existing_date or 'N/A'}→{new_date or 'N/A'})")
                    # Se datas não comparáveis: mantém a última (portal lista cronologicamente)
            latest_by_utility[key] = resp
        deduped_responses = list(latest_by_utility.values())

        # Aplica overrides locais (Frontier Terre Haute, etc)
        try:
            _apply_local_overrides(t, deduped_responses)
        except Exception as _ovr_e:
            log.warning(f"[Override] erro: {_ovr_e}")

        # Coleta records para bulk upsert
        now_iso = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        for resp in deduped_responses:
            parsed_date = resp.get("responded_date")
            record = {
                "ticket_id": tid, "ticket_num": tnum, "state": state,
                "utility_name": resp["utility"], "status": resp["status"],
                "response_text": resp.get("response") or resp.get("comment") or resp.get("status_raw", ""),
                "synced_at": now_iso,
                # Sempre inclui responded_at (evita NOT NULL constraint).
                # Se tem data real parseada, usa ela. Senão, fallback pra synced_at.
                # fix_clear_dates diferencia comparando responded_at vs synced_at.
                # Fix bug #15: simplificado — era `parsed_date if parsed_date else (now_iso if X else now_iso)`,
                # com os dois lados do ternário interno iguais (código morto, provável sobra de refactor).
                "responded_at": parsed_date if parsed_date else now_iso,
            }
            all_records.append(record)

        if deduped_responses:
            statuses = [r["status"] for r in deduped_responses]
            none_pending = not any(s == "Pending" for s in statuses)
            all_responded = all(s in ("Clear", "Pending") for s in statuses)
            ticket_locked = t.get("status_locked", False)

            for resp in deduped_responses:
                log.info(f"  [{state}] {tnum} | {resp['utility']}: {resp['status']} ({resp.get('response', '')[:60]})")

            # ── SEGURANÇA: cruzar com banco para detectar Pending que o scrape perdeu ──
            # Se o scrape retornou tudo Clear, verifica se o banco tem Pending
            # que NÃO apareceu neste scrape (ex: portal truncou a lista de utilities).
            # Se encontrar, bloqueia o auto-clear — dados incompletos.
            if none_pending and not ticket_locked:
                scraped_utils = {r["utility"] for r in deduped_responses}
                try:
                    db_pending = sb_get("ticket_811_responses",
                                       f"&ticket_num=eq.{_qv(tnum)}&status=eq.Pending&select=utility_name")
                    missed_pending = {r["utility_name"] for r in db_pending} - scraped_utils
                    if missed_pending:
                        log.warning(f"[{state}] {tnum}: ⚠ SEGURANÇA — {len(missed_pending)} utility(s) Pending no banco ausente(s) no scrape: {list(missed_pending)}")
                        log.warning(f"[{state}] {tnum}: Bloqueando auto-clear (scrape incompleto: {len(deduped_responses)} capturadas, banco tem Pending que faltou)")
                        none_pending = False
                except Exception as e:
                    log.warning(f"[{state}] {tnum}: Erro ao verificar Pending no banco (conservador: bloqueando auto-clear): {e}")
                    none_pending = False

            # ── DETECTAR RESPOSTAS NÃO RECONHECIDAS ──
            for resp in deduped_responses:
                if resp.get("_unrecognized"):
                    raw = (resp.get("response") or resp.get("comment") or resp.get("status_raw", ""))[:200]
                    log.warning(f"[{state}] {tnum}: ⚠ RESPOSTA NÃO RECONHECIDA — {resp['utility']}: '{raw}'")
                    summary.unrecognized += 1
                    summary.unrecognized_list.append({
                        "ticket_num": tnum,
                        "state": state,
                        "utility_name": resp["utility"],
                        "status_raw": resp.get("status_raw", ""),
                        "raw_text": raw,
                    })

            # ── DETECTAR 3H: PRIVATE LOCATOR NECESSÁRIO ──
            private_utils = []
            for resp in deduped_responses:
                resp_text = (resp.get("response", "") + " " + resp.get("comment", "") + " " + resp.get("status_raw", "")).strip()
                if needs_private_locator(resp_text):
                    private_utils.append(resp["utility"])
            if private_utils:
                current_pending = (t.get("pending") or "").strip()
                pvt_tag = "🔒 PRIVATE LOCATOR: " + ", ".join(private_utils)
                if "PRIVATE LOCATOR" not in current_pending:
                    new_pending = (current_pending + "\n" + pvt_tag).strip() if current_pending else pvt_tag
                    patch["pending"] = new_pending
                    needs_patch = True
                    log.warning(f"[{state}] {tnum}: ⚠ PRIVATE LOCATOR necessário — {', '.join(private_utils)}")
                    summary.private_locator += 1

            # ── DETECTAR WATCH AND PROTECT (W&P — código 60, IL) ──
            wp_utils = []
            for resp in deduped_responses:
                resp_text = (resp.get("response", "") + " " + resp.get("comment", "") + " " + resp.get("status_raw", "")).strip()
                if needs_watch_and_protect(resp_text):
                    wp_utils.append(resp["utility"])
            if wp_utils:
                current_pending = (patch.get("pending") or t.get("pending") or "").strip()
                wp_tag = "⚠️ WATCH & PROTECT: " + ", ".join(wp_utils)
                if "WATCH & PROTECT" not in current_pending:
                    new_pending = (current_pending + "\n" + wp_tag).strip() if current_pending else wp_tag
                    patch["pending"] = new_pending
                    needs_patch = True
                    log.warning(f"[{state}] {tnum}: ⚠ WATCH & PROTECT — representante obrigatório: {', '.join(wp_utils)}")
                    summary.watch_protect += 1

            # ── STATUS LOCKED: NUNCA alterar ──
            if ticket_locked:
                log.info(f"[{state}] {tnum}: 🔒 STATUS TRAVADO (manual) — nenhuma alteração automática")
                summary.locked_skipped += 1
                if needs_patch:
                    ticket_patches.append(patch)
                continue

            # ── RENOVAÇÃO: Período de graça ──
            old_status = (t.get("status_old") or "").strip()
            in_grace, old_ticket_num = is_in_renewal_grace(t)

            if in_grace:
                # Primeiro: checa se o ticket NOVO tem Pending. Se sim, NÃO protege —
                # processa normalmente pra disparar a reversão Clear→Open.
                # Regra rigorosa: graça do antigo não pode mascarar pendências reais do novo.
                new_has_pending = any(r["status"] == "Pending" for r in deduped_responses)
                if new_has_pending:
                    log.info(f"[{state}] {tnum}: 🔄 RENOVAÇÃO em graça, MAS ticket novo tem pendências — processando normalmente (graça não protege falso-Clear)")
                    # Cai fora do branch de graça, segue pro auto-clear/revert abaixo
                else:
                    real_old_clear = False
                    if old_ticket_num:
                        try:
                            old_resps = sb_get("ticket_811_responses", f"&ticket_num=eq.{_qv(old_ticket_num)}&select=status")
                            if old_resps and len(old_resps) > 0:
                                has_pending = any(r.get("status") == "Pending" for r in old_resps)
                                if not has_pending:
                                    real_old_clear = True
                                    if old_status != "Clear":
                                        log.info(f"[{state}] {tnum}: 🔄 RENOVAÇÃO — status_old={old_status or 'Open'} mas utilities REAIS do antigo ({old_ticket_num}) estão todas Clear")
                        except Exception as e:
                            log.debug(f"[{state}] {tnum}: Erro ao checar utilities do antigo: {e}")

                    if old_status == "Clear" or real_old_clear:
                        log.info(f"[{state}] {tnum}: 🔄 RENOVAÇÃO (graça até {t.get('expire_old', '')}) — antigo {'Clear (verificado)' if real_old_clear else 'Clear'}, mantém")
                        if needs_patch:
                            ticket_patches.append(patch)
                        continue
                    elif old_status:
                        log.info(f"[{state}] {tnum}: 🔄 RENOVAÇÃO (graça até {t.get('expire_old', '')}) — antigo era {old_status}, processando normalmente")
                    else:
                        log.warning(f"[{state}] {tnum}: 🔄 RENOVAÇÃO (graça até {t.get('expire_old', '')}) — status do antigo DESCONHECIDO, protegendo por precaução")
                        if needs_patch:
                            ticket_patches.append(patch)
                        continue

            if none_pending and all_responded and t.get("status") == "Open":
                # AUTO-CLEAR (Fix 2026-05-14): clear_ts = now (quando ticket mudou pra Clear)
                # clear_label = data real da ultima resposta (info no historico)
                clear_dt, is_fallback = _get_latest_response_date(deduped_responses, ticket_num=tnum)
                clear_ts = int(datetime.now().timestamp() * 1000)
                clear_label = clear_dt.strftime('%m/%d/%Y')
                hist = t.get("history") or []
                clear_note = f"[AUTO 811] Clear em {clear_label}"
                hist.append({"ts": clear_ts, "action": clear_note, "color": "#16a34a"})
                new_notes = append_auto_note(patch.get("notes") or t.get("notes"), clear_note)
                patch.update({"status": "Clear", "notes": new_notes, "history": hist})
                needs_patch = True
                log.info(f"[{state}] {tnum}: AUTO-CLEAR (data{'⚠ FALLBACK sync-time' if is_fallback else ' real'}: {clear_label})")
                summary.cleared += 1

            elif none_pending and all_responded and t.get("status") == "Clear":
                # Backfill: se não tem evento de clear no histórico, adiciona
                hist = t.get("history") or []
                has_clear_evt = any(
                    "auto 811" in (h.get("action", "")).lower() and "revertido" not in (h.get("action", "")).lower()
                    for h in hist
                )
                has_clear_evt = has_clear_evt or any("→ clear" in (h.get("action", "")).lower() for h in hist)

                if not has_clear_evt:
                    # Tenta usar data real das respostas; fallback pra notas; fallback pra now
                    backfill_dt, is_fallback = _get_latest_response_date(deduped_responses, ticket_num=tnum)
                    # Se fallback (sem datas reais), tenta extrair das notas
                    if is_fallback:
                        notes_text = t.get("notes") or ""
                        date_match = re.search(r'\[AUTO 811\] Clear em (\d{1,2}/\d{1,2}/\d{4})', notes_text)
                        if date_match:
                            try:
                                backfill_dt = datetime.strptime(date_match.group(1), "%m/%d/%Y")
                            except Exception:
                                pass
                    backfill_ts = int(backfill_dt.timestamp() * 1000)
                    backfill_label = backfill_dt.strftime('%m/%d/%Y')

                    hist.append({"ts": backfill_ts, "action": f"[AUTO 811] Clear em {backfill_label}", "color": "#16a34a"})
                    patch["history"] = hist
                    needs_patch = True
                    log.info(f"[{state}] {tnum}: Clear confirmado + BACKFILL histórico ({backfill_label})")
                    summary.backfilled += 1
                else:
                    log.info(f"[{state}] {tnum}: Clear confirmado")
                    summary.confirmed_clear += 1

            elif not none_pending and t.get("status") == "Open":
                pending_utils = [r["utility"] for r in deduped_responses if r["status"] == "Pending"]
                log.info(f"[{state}] {tnum}: Pendente — aguardando: {pending_utils}")
                summary.pending += 1

            elif not none_pending and t.get("status") == "Clear":
                # REVERTENDO Clear→Open
                pending_utils = [r["utility"] for r in deduped_responses if r["status"] == "Pending"]
                now_ts = int(datetime.now().timestamp() * 1000)
                hist = t.get("history") or []
                revert_note = f"[AUTO 811] Revertido Clear→Open em {datetime.now().strftime('%m/%d/%Y')} — pendente: {', '.join(pending_utils)}"
                hist.append({"ts": now_ts, "action": f"[AUTO 811] Revertido Clear→Open — {', '.join(pending_utils)}", "color": "#dc2626"})
                new_notes = append_auto_note(patch.get("notes") or t.get("notes"), revert_note)
                log.warning(f"[{state}] {tnum}: REVERTENDO Clear→Open — pendente: {pending_utils}")
                patch.update({"status": "Open", "notes": new_notes, "history": hist})
                needs_patch = True
                summary.reverted += 1

        if needs_patch:
            ticket_patches.append(patch)

    # ── Batch ticket patches (substitui N+1 sb_patch individuais) ──
    if ticket_patches:
        log.info(f"[{state}] Aplicando {len(ticket_patches)} ticket patches em batch...")
        sb_batch_patch("tickets", ticket_patches, id_field="id")

    # Bulk upsert responses
    if all_records:
        try:
            for i in range(0, len(all_records), BATCH_SIZE):
                batch = all_records[i:i + BATCH_SIZE]
                sb_upsert("ticket_811_responses", batch)
                if i + BATCH_SIZE < len(all_records):
                    time.sleep(0.5)
            summary.responses_saved = len(all_records)
            log.info(f"[{state}] Bulk upsert: {summary.responses_saved} registros salvos")
        except Exception as e:
            log.error(f"[{state}] Erro no bulk upsert: {e} — tentando individual...")
            for rec in all_records:
                try:
                    sb_upsert("ticket_811_responses", rec)
                    summary.responses_saved += 1
                except Exception as e2:
                    log.error(f"Erro individual {rec.get('ticket_num')}/{rec.get('utility_name')}: {e2}")

    # ── Salvar respostas não reconhecidas no Supabase ──
    if summary.unrecognized_list:
        save_unrecognized_responses(summary.unrecognized_list)
        send_unrecognized_alert(state, summary.unrecognized_list)

    return summary


# ── │ SECTION: IMPORT │ IMPORTAR TICKETS NOVOS ────────────────────────────────

async def _scrape_bodies_parallel(state, ticket_numbers):
    """Scrape body (Text tab) de múltiplos tickets em paralelo.

    Abre NUM_TABS abas, cada uma processa um chunk. Retorna dict {tnum: body_text}.
    Usado pelo import_new_tickets pra coletar bodies sem bloquear 1 por 1.
    """
    results = {}
    if not ticket_numbers:
        return results

    perfil = _profile_path(state)
    n_tabs = min(NUM_TABS, len(ticket_numbers))
    chunks = [[] for _ in range(n_tabs)]
    for i, tnum in enumerate(ticket_numbers):
        chunks[i % n_tabs].append(tnum)
    chunks = [c for c in chunks if c]

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)

        if "login" in page.url.lower():
            await ctx.close()
            await asyncio.sleep(1)
            ok = await auto_login_silent(state)
            if not ok:
                log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")
                ok = await auto_login(state)
            if not ok:
                return results
            await asyncio.sleep(1)
            ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)
            await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
            await wait_stable(page)
            if "login" in page.url.lower():
                log.error(f"[{state}] Import bodies: login falhou após renovação")
                await ctx.close()
                return results

        log.info(f"[{state}] Import bodies: {len(ticket_numbers)} tickets em {len(chunks)} abas paralelas")

        async def _body_chunk(chunk, tab_id):
            tab_results = {}
            if not chunk:
                return tab_results
            pg = await ctx.new_page()
            pg.set_default_timeout(60000)

            ok = await goto_dashboard(pg, state)
            if not ok:
                log.error(f"[{state}][T{tab_id}] Dashboard inacessível — abortando aba")
                try:
                    await pg.close()
                except Exception:
                    pass
                return tab_results

            for idx, tnum in enumerate(chunk):
                log.info(f"[{state}][T{tab_id}] Import ({idx+1}/{len(chunk)}) {tnum}")
                for _attempt in range(2):
                    try:
                        await filter_ticket(pg, tnum)
                        if not await pg.get_by_text(tnum, exact=True).count():
                            log.warning(f"[{state}] {tnum}: não encontrado no portal")
                            break
                        await pg.get_by_text(tnum, exact=True).first.click()
                        await wait_stable(pg)

                        tt = pg.locator('[role="tab"]:has-text("Text")').first
                        if await tt.count():
                            await click_and_wait(pg, tt, "tab")

                        body = await pg.locator("body").inner_text()
                        tab_results[tnum] = body
                        await fast_back(pg, state)
                        break
                    except Exception as e:
                        if _attempt == 0 and "Timeout" in str(e):
                            log.warning(f"[{state}][T{tab_id}] {tnum}: Timeout — retry...")
                            try:
                                await back_to_dashboard(pg, state)
                            except Exception:
                                pass
                            continue
                        log.error(f"[{state}][T{tab_id}] {tnum}: ERRO → {e}")
                        try:
                            await back_to_dashboard(pg, state)
                        except Exception:
                            pass

            try:
                await pg.close()
            except Exception:
                pass
            log.info(f"[{state}][T{tab_id}] Import chunk: {len(tab_results)}/{len(chunk)} bodies coletados")
            return tab_results

        try:
            chunk_results = await asyncio.gather(*[_body_chunk(c, i) for i, c in enumerate(chunks)])
            for cr in chunk_results:
                results.update(cr)
        finally:
            try:
                await ctx.close()
            except Exception:
                pass
    return results


async def import_new_tickets(state, triggered_by="manual"):
    """Importa tickets novos do portal 811.

    Fluxo otimizado em 5 fases:
      1. Coleta números de tickets novos (1 aba, serial — lê labels no dashboard)
      2. Scrape bodies em paralelo (NUM_TABS abas simultâneas)
      3. Parse fields de cada body (Python puro, sem browser)
      4. Geocoding em batch (Nominatim, 1.1s entre chamadas)
      5. Batch upsert no Supabase
    """
    log.info(f"[{state}] === Importando tickets novos ===")
    existing = sb_get("tickets", f"&state=eq.{state}")
    existing_nums = {t["ticket"] for t in existing}
    projects = sb_get("projects")
    perfil = _profile_path(state)

    canceled_set = get_canceled_set(state)
    if canceled_set:
        log.info(f"[{state}] Cache: {len(canceled_set)} tickets cancelados conhecidos — serão pulados")

    # ── FASE 1: Coletar números de tickets novos (1 aba, serial) ──
    all_new_nums = []
    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)

        page, ctx = await ensure_login(page, ctx, p, state)
        if not page:
            return 0

        log.info(f"[{state}] Logado para importação")
        ok = await goto_dashboard(page, state)
        if not ok:
            log.error(f"[{state}] Não conseguiu acessar dashboard — abortando importação")
            try:
                await ctx.close()
            except Exception:
                pass
            return 0

        await set_items_per_page(page, 100)

        page_num = 1
        empty_pages = 0
        last_page_tickets = set()

        while page_num <= MAX_IMPORT_PAGES:
            if page_num == MAX_IMPORT_PAGES:
                log.warning(f"[{state}] Limite de {MAX_IMPORT_PAGES} páginas atingido — pode haver mais tickets não importados")

            log.info(f"[{state}] Lendo página {page_num}...")
            ticket_labels = page.locator("label.column-fixed")
            count = await ticket_labels.count()
            page_nums = []
            all_on_page = set()
            collected_set = set(all_new_nums)

            for i in range(count):
                txt = (await ticket_labels.nth(i).inner_text()).strip()
                if re.match(r"^\d{8,12}$", txt):
                    all_on_page.add(txt)
                    if txt in canceled_set:
                        continue
                    if txt not in existing_nums and txt not in collected_set:
                        page_nums.append(txt)
                        collected_set.add(txt)

            # Detecta loop de paginação
            if all_on_page and all_on_page == last_page_tickets:
                log.info(f"[{state}] Página {page_num} idêntica a anterior — fim da paginação")
                break
            last_page_tickets = all_on_page

            log.info(f"[{state}] Pág {page_num}: {len(page_nums)} tickets novos (de {len(all_on_page)} na página)")
            if len(page_nums) == 0:
                empty_pages += 1
            else:
                empty_pages = 0

            all_new_nums.extend(page_nums)

            next_btn = page.get_by_text("Next").first
            if await next_btn.count() and await next_btn.is_enabled():
                await click_and_wait(page, next_btn, "nav")
                await select_office(page, state)
                page_num += 1
            else:
                break

        try:
            await ctx.close()
        except Exception:
            pass

    if not all_new_nums:
        log.info(f"[{state}] Nenhum ticket novo encontrado")
        return 0

    log.info(f"[{state}] FASE 1 OK: {len(all_new_nums)} tickets novos — abrindo scrape paralelo")

    # ── FASE 2: Scrape bodies em paralelo (NUM_TABS abas) ──
    bodies = await _scrape_bodies_parallel(state, all_new_nums)
    log.info(f"[{state}] FASE 2 OK: {len(bodies)}/{len(all_new_nums)} bodies coletados")

    # ── FASE 3: Parse fields de cada body (Python puro, sem browser) ──
    parsed_tickets = []
    for tnum in all_new_nums:
        body = bodies.get(tnum)
        if not body:
            log.warning(f"[{state}] {tnum}: body não obtido — pulando")
            continue

        if is_ticket_canceled(body):
            replaced_match = re.search(r"REPLACED\s+BY\s+TICKET\s+(?:NUMBER:?\s*)(\d+)", body, re.IGNORECASE)
            replaced_by = replaced_match.group(1) if replaced_match else "N/A"
            log.warning(f"[{state}] {tnum}: CANCELADO (substituído por {replaced_by}) — pulando")
            add_to_canceled_cache(state, tnum)
            continue

        def extract(pattern, text, default=""):
            m = re.search(pattern, text, re.IGNORECASE)
            return m.group(1).strip() if m else default

        tnum_ext = extract(r"Ticket\s*:\s*(\d+)", body) or tnum
        state_code = extract(r"State:\s*(\w{2})", body, state)
        city = extract(r"Cityname:\s*([^\n]+)", body)
        if not city:
            city = extract(r"GeoPlace:\s*([^\n]+)", body)
        township = extract(r"Twp:\s*([^\n]+)", body)
        street_name = extract(r"Street\s*:\s*([^\n]+)", body)
        street_number = extract(r"Address\s*:\s*(\d+[^\n]*)", body)
        if street_number and street_name and not street_name[0].isdigit():
            street = f"{street_number.strip()} {street_name.strip()}"
        else:
            street = street_name
        work_type = extract(r"(?:Work Type|Type of Work|Work type)\s*:\s*([^\n]+)", body)
        job_id = extract(r"Job\s*(?:ID|#|Number)?\s*:\s*([^\n]+)", body)

        done_for_raw = extract(r"(?:Done\s*for|Work\s*(?:being\s*)?done\s*for)\s*:\s*([^\n]+)", body)
        client_811 = ""
        prime_811 = ""
        if done_for_raw:
            parts = [p.strip() for p in done_for_raw.replace("\\", "/").split("/") if p.strip()]
            if len(parts) >= 2:
                prime_811 = parts[0]
                client_811 = parts[1]
            elif len(parts) == 1:
                client_811 = parts[0]
            log.info(f"[{state}] {tnum_ext}: Done for → client={client_811}, prime={prime_811}")

        expire_str = normalize_expire(extract_expire_date(body))
        if expire_str:
            log.info(f"[{state}] {tnum_ext}: Expire → {expire_str}")
        location = extract_location_text(body, state=state)

        # Match com projeto — APENAS por Job#.
        project_id = None
        if job_id:
            for proj in projects:
                if job_id.strip() in (proj.get("name", "") + proj.get("description", "")):
                    project_id = proj["id"]
                    break

        # Boundary coords (instant, sem geocoding)
        geo_lat, geo_lon = None, None
        needs_geocode = False
        boundary_match = re.search(
            r"Boundary:\s*n\s*([\d.]+)\s+s\s*([\d.]+)\s+w\s*([-\d.]+)\s+e\s*([-\d.]+)",
            body, re.IGNORECASE
        )
        if boundary_match:
            n, s_val, w, e = (
                float(boundary_match.group(1)), float(boundary_match.group(2)),
                float(boundary_match.group(3)), float(boundary_match.group(4))
            )
            geo_lat = round((n + s_val) / 2, 6)
            geo_lon = round((w + e) / 2, 6)
        elif street and city:
            needs_geocode = True

        # Verificar se substitui ticket anterior
        old_ticket_num = ""
        old_expire_str = ""
        old_status_str = ""
        inherited_path = None
        old_tkt_match = re.search(r"Old\s*Tkt\s*:\s*(\d+)", body, re.IGNORECASE)
        if old_tkt_match:
            old_ticket_num = old_tkt_match.group(1)
        if not old_ticket_num:
            rep_match = re.search(r"Replaces?\s+(?:Ticket\s*)?(?:Number:?\s*)?(\d+)", body, re.IGNORECASE)
            if rep_match:
                old_ticket_num = rep_match.group(1)

        if old_ticket_num:
            log.info(f"[{state}] {tnum_ext}: Substitui ticket anterior {old_ticket_num}")
            try:
                old_tickets = sb_get("tickets", f"&ticket=eq.{_qv(old_ticket_num)}&state=eq.{_qv(state_code)}")
                if old_tickets:
                    ot = old_tickets[0]
                    if ot.get("field_path"):
                        inherited_path = ot["field_path"]
                        log.info(f"[{state}] {tnum_ext}: Trajeto herdado do ticket {old_ticket_num} ({len(inherited_path)} pts)")
                    old_expire_str = normalize_expire(ot.get("expire") or "")
                    old_status_str = (ot.get("status") or "").strip()
                    if old_expire_str or old_status_str:
                        log.info(f"[{state}] {tnum_ext}: Graça capturada — old_status={old_status_str!r}, old_expire={old_expire_str!r}")
            except Exception as e:
                log.debug(f"Erro ao buscar ticket anterior {old_ticket_num}: {e}")

        parsed_tickets.append({
            "tnum_ext": tnum_ext, "state_code": state_code,
            "city": city, "township": township, "street": street,
            "work_type": work_type, "job_id": job_id,
            "client_811": client_811, "prime_811": prime_811,
            "expire_str": expire_str, "location": location,
            "project_id": project_id,
            "geo_lat": geo_lat, "geo_lon": geo_lon, "needs_geocode": needs_geocode,
            "old_ticket_num": old_ticket_num, "old_expire_str": old_expire_str,
            "old_status_str": old_status_str, "inherited_path": inherited_path,
        })

    log.info(f"[{state}] FASE 3 OK: {len(parsed_tickets)} tickets parseados")

    # ── FASE 4: Geocoding em batch (sem browser, só Nominatim) ──
    geocode_count = sum(1 for t in parsed_tickets if t["needs_geocode"])
    if geocode_count:
        log.info(f"[{state}] Geocodando {geocode_count} endereços...")
        done = 0
        for t in parsed_tickets:
            if t["needs_geocode"]:
                t["geo_lat"], t["geo_lon"] = await geocode_address(t["street"], t["city"], t["state_code"])
                done += 1
                if done % 10 == 0:
                    log.info(f"[{state}] Geocoding {done}/{geocode_count}...")

    # ── FASE 5: Build ticket data e batch upsert ──
    new_tickets = []
    for t in parsed_tickets:
        work_type_final = t["work_type"] or "Main line"
        if t["geo_lat"] and t["geo_lon"]:
            t["geo_lat"], t["geo_lon"], work_type_final = adjust_coords_by_location(
                t["geo_lat"], t["geo_lon"], t["location"], work_type_final
            )

        ticket_county = ""
        try:
            loc_for_county = f"{t['city']}, {t['township']}".strip(", ")
            ticket_county = await resolve_county(loc_for_county, t["state_code"], t["geo_lat"], t["geo_lon"])
        except Exception as e:
            log.debug(f"[{state}] {t['tnum_ext']}: erro resolvendo county: {e}")

        history_entries = [
            {"ts": int(datetime.now().timestamp() * 1000), "action": f"Importado 811 - {t['city']}, {t['state_code']}", "color": "#10a574"}
        ]
        if t["inherited_path"]:
            history_entries.append(
                {"ts": int(datetime.now().timestamp() * 1000), "action": f"Trajeto herdado do ticket {t['old_ticket_num']}", "color": "#6d28d9"}
            )
        if t["old_ticket_num"] and (t["old_expire_str"] or t["old_status_str"]):
            history_entries.append(
                {"ts": int(datetime.now().timestamp() * 1000),
                 "action": f"[RENOVAÇÃO] {t['old_ticket_num']} → {t['tnum_ext']} (graça até {t['old_expire_str'] or 'N/A'}, status antigo: {t['old_status_str'] or 'N/A'})",
                 "color": "#7c3aed"}
            )

        ticket_data = {
            "ticket": t["tnum_ext"], "company": "One Drill", "state": t["state_code"],
            "location": f"{t['city']}, {t['township']}".strip(", "), "address": t["street"],
            "work_type": work_type_final,
            "status": "Open", "expire": t["expire_str"], "footage": 0,
            "client": t["client_811"], "prime": t["prime_811"], "tipo": work_type_final,
            "job": t["job_id"] or "", "notes": f"[811 Location] {t['location']}" if t["location"] else "",
            "project_id": t["project_id"], "pending": "", "old_ticket2": t["old_ticket_num"],
            "status_old": t["old_status_str"], "expire_old": t["old_expire_str"], "field_path": t["inherited_path"],
            "geocoded_lat": t["geo_lat"], "geocoded_lon": t["geo_lon"],
            "county": ticket_county,
            "history": history_entries, "attachments": [],
        }
        new_tickets.append(ticket_data)
        log.info(f"[{state}] Preparado: {t['tnum_ext']}  {t['city']}, {t['township']}{' [' + ticket_county + ' Co]' if ticket_county else ''}")

    # Batch upsert
    inserted = 0
    to_insert = [td for td in new_tickets if td['ticket'] not in existing_nums]
    for td in new_tickets:
        if td['ticket'] in existing_nums:
            log.warning(f"[{state}] {td['ticket']}: DUPLICATA — já existe no sistema, pulando")

    if to_insert:
        try:
            for i in range(0, len(to_insert), BATCH_SIZE):
                chunk = to_insert[i:i + BATCH_SIZE]
                sb_upsert("tickets", chunk, on_conflict="ticket")
                inserted += len(chunk)
                for td in chunk:
                    existing_nums.add(td['ticket'])
                if i + BATCH_SIZE < len(to_insert):
                    time.sleep(0.3)
            log.info(f"[{state}] ✅ Batch upsert: {inserted} tickets inseridos em {((len(to_insert)-1)//BATCH_SIZE)+1} request(s)")
        except Exception as e:
            log.warning(f"[{state}] Batch falhou ({e}), tentando 1-por-1 como fallback...")
            inserted = 0
            for td in to_insert:
                try:
                    if td['ticket'] not in existing_nums:
                        sb_insert("tickets", td)
                        existing_nums.add(td['ticket'])
                    inserted += 1
                    log.info(f"[{state}] [OK fallback] {td['ticket']}")
                except Exception as e2:
                    log.error(f"[{state}] Erro inserindo {td['ticket']}: {e2}")

    log.info(f"[{state}] === Importação: {inserted} tickets novos ===")
    return inserted


# ── │ SECTION: RESCRAPE │ REESCRAPER: ATUALIZA NOTES + EXPIRE ─────────────────
async def rescrape_notes(state, force=False):
    perfil = _profile_path(state)
    tickets = sb_get("tickets", f"&state=eq.{state}&status=in.(Open,Damage,Clear)&order=ticket")
    if not tickets:
        log.info(f"[{state}] Nenhum ticket ativo para re-scrape")
        return

    if force:
        to_fix = tickets
    else:
        to_fix = [
            t for t in tickets
            if not (t.get("notes") or "").strip()
            or (t.get("notes") or "").strip() == "[811 Location]"
            or not (t.get("expire") or "").strip()
        ]

    log.info(f"[{state}] Re-scrape{'(FORCE)' if force else ''}: {len(to_fix)} tickets (de {len(tickets)} ativos)")
    if not to_fix:
        log.info(f"[{state}] Nada a fazer")
        return

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)
        page, ctx = await ensure_login(page, ctx, p, state)
        if not page:
            return
        log.info(f"[{state}] Logado para re-scrape")
        ok = await goto_dashboard(page, state)
        if not ok:
            log.error(f"[{state}] Re-scrape: não conseguiu acessar dashboard — abortando")
            await ctx.close()
            return

        fixed = 0
        for idx, t in enumerate(to_fix):
            tnum = t["ticket"]
            tid = t["id"]
            log.info(f"[{state}] Re-scrape ({idx+1}/{len(to_fix)}) {tnum}")
            try:
                await filter_ticket(page, tnum)
                if not await page.get_by_text(tnum, exact=True).count():
                    log.warning(f"[{state}] {tnum}: não encontrado")
                    continue
                await page.get_by_text(tnum, exact=True).first.click()
                await wait_stable(page)

                tt = page.locator('[role="tab"]:has-text("Text")').first
                if await tt.count():
                    await click_and_wait(page, tt, "tab")

                body = await page.locator("body").inner_text()
                location = extract_location_text(body, state=state)
                patch_data = {}

                if location and (not (t.get("notes") or "").strip() or (t.get("notes") or "").strip() == "[811 Location]"):
                    patch_data["notes"] = f"[811 Location] {location}"

                current_exp = (t.get("expire") or "").strip()
                if not current_exp or _is_polluted_expire(current_exp):
                    expire_str = normalize_expire(extract_expire_date(body))
                    if expire_str and expire_str != current_exp:
                        patch_data["expire"] = expire_str

                if location and t.get("geocoded_lat") and t.get("geocoded_lon"):
                    new_lat, new_lon, new_tipo = adjust_coords_by_location(
                        t["geocoded_lat"], t["geocoded_lon"], location, t.get("tipo")
                    )
                    if new_lat != t["geocoded_lat"] or new_lon != t["geocoded_lon"] or new_tipo != t.get("tipo"):
                        patch_data.update({"geocoded_lat": new_lat, "geocoded_lon": new_lon, "tipo": new_tipo})

                if patch_data:
                    sb_patch("tickets", tid, patch_data)
                    fixed += 1
                await back_to_dashboard(page, state)

            except Exception as e:
                log.error(f"[{state}] Re-scrape {tnum}: ERRO → {e}")
                try:
                    await back_to_dashboard(page, state)
                except Exception:
                    pass

        await ctx.close()
    log.info(f"[{state}] === Re-scrape concluído: {fixed}/{len(to_fix)} atualizados ===")


# ── │ SECTION: CLEANUP │ LIMPAR TICKETS CANCELADOS ────────────────────────────
async def cleanup_canceled(state):
    perfil = _profile_path(state)
    canceled = 0

    # Fase 1: REMOVIDA — tickets com status=Cancel ficam no banco como histórico.
    # Política: cancelado no 811 → marca como Cancel, NUNCA deleta.
    # O filter_tickets_for_sync já os exclui do scrape loop, então custo zero.
    # Preserva auditoria, footage, respostas 811 e histórico no OneDrill.

    # Fase 2: Verificar tickets ativos no portal
    tickets = sb_get("tickets", f"&state=eq.{state}&status=in.(Open,Damage,Clear)&order=ticket")
    if not tickets:
        log.info(f"[{state}] Cleanup: nenhum ticket ativo para verificar")
        log.info(f"[{state}] === Cleanup concluído: {canceled} removidos ===")
        return canceled

    log.info(f"[{state}] Cleanup F2: verificando {len(tickets)} tickets no portal...")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)
        page, ctx = await ensure_login(page, ctx, p, state)
        if not page:
            return canceled
        log.info(f"[{state}] Cleanup: logado")
        ok = await goto_dashboard(page, state)
        if not ok:
            log.error(f"[{state}] Cleanup: não conseguiu acessar dashboard — abortando")
            await ctx.close()
            return canceled

        for idx, t in enumerate(tickets):
            tnum = t["ticket"]
            tid = t["id"]
            try:
                await filter_ticket(page, tnum)
                if not await page.get_by_text(tnum, exact=True).count():
                    continue
                await page.get_by_text(tnum, exact=True).first.click()
                await wait_stable(page)

                body = await page.locator("body").inner_text()
                tt = page.locator('[role="tab"]:has-text("Text")').first
                if await tt.count():
                    await click_and_wait(page, tt, "tab")
                    body = await page.locator("body").inner_text()

                if is_ticket_canceled(body):
                    replaced_match = re.search(r"REPLACED\s+BY\s+TICKET\s+(?:NUMBER:?\s*)(\d+)", body, re.IGNORECASE)
                    replaced_by = replaced_match.group(1) if replaced_match else "N/A"
                    try:
                        # NÃO deleta — marca como Cancel pra preservar histórico no OneDrill.
                        # Mantém ticket, respostas 811, footage e histórico acessíveis no app.
                        # add_to_canceled_cache garante que import_new_tickets não re-importa.
                        hist = t.get("history") or []
                        hist.append({
                            "ts": int(datetime.now().timestamp() * 1000),
                            "action": f"[AUTO 811] CANCELADO no portal — substituído por {replaced_by}",
                            "color": "#6d28d9"
                        })
                        sb_patch("tickets", tid, {
                            "status": "Cancel",
                            "history": hist
                        })
                        canceled += 1
                        add_to_canceled_cache(state, tnum)
                        log.warning(f"[{state}] Cleanup: {tnum} marcado como Cancel (substituído por {replaced_by})")
                    except Exception as e:
                        log.error(f"[{state}] Cleanup: erro marcando {tnum}: {e}")

                await back_to_dashboard(page, state)

            except Exception as e:
                log.error(f"[{state}] Cleanup {tnum}: ERRO → {e}")
                try:
                    await back_to_dashboard(page, state)
                except Exception:
                    pass

        await ctx.close()

    log.info(f"[{state}] === Cleanup concluído: {canceled} tickets cancelados removidos ===")
    return canceled


# ── │ SECTION: FILTER_SYNC │ CLEAR TICKET CACHE ───────────────────────────────
def filter_tickets_for_sync(tickets, state):
    """Filtra tickets que realmente precisam de scraping.

    - Open / Damage: SEMPRE verificar
    - Clear SEM expire: SEMPRE verificar (provavelmente ticket renovado
      que teve expire zerado — precisa buscar data nova no portal)
    - Clear RENOVADO (old_ticket2 preenchido): SEMPRE verificar (pode ter
      pendências novas que o cache não detectou porque o registro de Pending
      tem state/ticket_num quebrado em algum canto)
    - Clear: Pular se TODAS utilities já responderam E último sync < CLEAR_CACHE_HOURS
    """
    must_check = [t for t in tickets if t.get("status") in ("Open", "Damage")]
    clear_tickets = [t for t in tickets if t.get("status") == "Clear"]

    # Clear sem expire → ticket renovado esperando atualização pelo scraper.
    # Move imediatamente pra lista de verificação (fura qualquer cache).
    renewal_pending = [t for t in clear_tickets if not (t.get("expire") or "").strip()]
    if renewal_pending:
        nums = ", ".join(t["ticket"] for t in renewal_pending[:5])
        extra = "" if len(renewal_pending) <= 5 else f" (+{len(renewal_pending)-5} mais)"
        log.info(f"[{state}] {len(renewal_pending)} ticket(s) Clear sem expire — "
                 f"forçando verificação: {nums}{extra}")
        must_check.extend(renewal_pending)
    clear_tickets = [t for t in clear_tickets if (t.get("expire") or "").strip()]

    # Clear + renovado → força verificação. O ciclo de resposta do ticket
    # novo pode mudar rápido quando o antigo termina a graça, e o cache
    # de 24h é longo demais pra esse cenário.
    renewed_clear = [t for t in clear_tickets if (t.get("old_ticket2") or "").strip()]
    if renewed_clear:
        nums = ", ".join(t["ticket"] for t in renewed_clear[:5])
        extra = "" if len(renewed_clear) <= 5 else f" (+{len(renewed_clear)-5} mais)"
        log.info(f"[{state}] {len(renewed_clear)} ticket(s) Clear renovados — "
                 f"forçando verificação: {nums}{extra}")
        must_check.extend(renewed_clear)
    clear_tickets = [t for t in clear_tickets if not (t.get("old_ticket2") or "").strip()]

    if not clear_tickets:
        return must_check, 0

    clear_nums = [t["ticket"] for t in clear_tickets]
    try:
        pending_responses = sb_get(
            "ticket_811_responses",
            f"&state=eq.{state}&status=eq.Pending"
            f"&ticket_num=in.({','.join(clear_nums)})"
            "&select=ticket_num"
        )
        tickets_with_pending = {str(r["ticket_num"]) for r in pending_responses}
    except Exception as e:
        log.warning(f"[{state}] Cache: erro ao buscar pendentes, verificando todos: {e}")
        return tickets, 0

    try:
        last_sync = sb_get(
            "sync_811_log",
            f"&state=eq.{state}&status=eq.success&order=finished_at.desc&limit=1"
        )
        if last_sync and last_sync[0].get("finished_at"):
            last_sync_time = datetime.fromisoformat(last_sync[0]["finished_at"])
            if last_sync_time.tzinfo is not None:
                last_sync_time = last_sync_time.replace(tzinfo=None)
        else:
            last_sync_time = None
    except Exception:
        last_sync_time = None

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cache_cutoff = now - timedelta(hours=CLEAR_CACHE_HOURS)

    skipped = 0
    for t in clear_tickets:
        tnum = t["ticket"]

        if tnum in tickets_with_pending:
            must_check.append(t)
            continue

        if not last_sync_time or last_sync_time < cache_cutoff:
            must_check.append(t)
            continue

        skipped += 1

    return must_check, skipped


# ── │ SECTION: SYNC │ SYNC STATE ──────────────────────────────────────────────
async def sync_state(state, triggered_by="manual"):
    log.info(f"{'='*55}")
    log.info(f"  OneDrill 811 Sync  {state}  [{triggered_by}]")
    log.info(f"{'='*55}")
    lid = log_start(state, triggered_by)
    checked = 0
    summary = SyncSummary()
    try:
        if not PORTALS[state]["user"]() or not PORTALS[state]["pass"]():
            raise ValueError(f"Credenciais faltando para {state}")

        all_tickets = sb_get("tickets", f"&state=eq.{state}&status=in.(Open,Damage,Clear)&order=ticket")
        if not all_tickets:
            log.info(f"[{state}] Nenhum ticket ativo")
            log_finish(lid, 0, 0)
            return

        tickets_to_scrape, skipped = filter_tickets_for_sync(all_tickets, state)
        if skipped > 0:
            log.info(f"[{state}] Cache: {skipped} tickets Clear pulados (sem pendências, verificados recentemente)")

        checked = len(tickets_to_scrape)
        if not tickets_to_scrape:
            log.info(f"[{state}] Nenhum ticket precisa verificação agora ({len(all_tickets)} ativos, {skipped} em cache)")
            log_finish(lid, 0, 0)
            return

        nums = [t["ticket"] for t in tickets_to_scrape]

        # ── Inclui tickets ANTIGOS de renovações em carência (respostas podem ter atualizado) ──
        grace_old_map = {}  # old_ticket_num → new_ticket_id
        nums_set = set(nums)
        for t in all_tickets:
            in_grace, old_num = is_in_renewal_grace(t)
            if not in_grace or not old_num or old_num in nums_set:
                continue
            nums.append(old_num)
            nums_set.add(old_num)
            grace_old_map[old_num] = t["id"]
            log.info(f"[{state}] Incluindo ticket antigo {old_num} (carência de {t['ticket']})")

        log.info(f"[{state}] {checked} tickets para verificar (de {len(all_tickets)} ativos)"
                 + (f" + {len(grace_old_map)} antigos em carência" if grace_old_map else ""))
        results = await scrape(state, nums, tickets_data=tickets_to_scrape)

        summary = save_to_supabase(state, results, all_tickets, grace_old_map=grace_old_map)
        log_finish(lid, checked, summary.responses_saved)
        log.info(f"[{state}] CONCLUÍDO  {checked} verificados, {skipped} em cache | {summary}")

    except Exception as e:
        log.error(f"[{state}] FALHOU: {e}")
        log_finish(lid, checked, summary.responses_saved, "error", str(e))


async def validate_sessions():
    """Valida sessões FL e IN sequencialmente antes do sync paralelo.
    Se alguma sessão expirou, abre janela de login uma de cada vez."""
    for state in ["FL", "IN"]:
        perfil = _profile_path(state)
        log.info(f"[{state}] Validando sessão...")
        try:
            async with async_playwright() as p:
                ctx = await p.chromium.launch_persistent_context(
                    perfil, headless=True, args=["--no-sandbox"]
                )
                page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                page.set_default_timeout(15000)
                await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)
                needs_login = "login" in page.url.lower()
                await ctx.close()

            if needs_login:
                log.warning(f"[{state}] Sessão expirada — abrindo login manual...")
                ok = await auto_login_silent(state)

                if not ok:

                    log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")

                    ok = await auto_login(state)
                if ok:
                    log.info(f"[{state}] ✅ Sessão renovada")
                else:
                    log.error(f"[{state}] ❌ Login falhou — sync do {state} pode falhar")
            else:
                log.info(f"[{state}] ✅ Sessão válida")
        except Exception as e:
            log.error(f"[{state}] Erro ao validar sessão: {e}")


async def sync_all(triggered_by="manual"):
    await validate_sessions()
    await asyncio.gather(
        sync_state("IN", triggered_by),
        sync_state("FL", triggered_by)
    )


async def sync_and_import(state, triggered_by="manual"):
    await import_new_tickets(state, triggered_by)
    await sync_state(state, triggered_by)


async def sync_and_import_all(triggered_by="manual"):
    """Roda IN e FL em paralelo."""
    await asyncio.gather(
        sync_and_import("IN", triggered_by),
        sync_and_import("FL", triggered_by)
    )


async def cleanup_all():
    """Cleanup IN e FL em paralelo."""
    await asyncio.gather(
        cleanup_canceled("IN"),
        cleanup_canceled("FL")
    )


# ── │ SECTION: JULIE │ JULIE (Illinois 811) — SCRAPE PÚBLICO ──────────────────

async def scrape_julie_ticket(page, tnum, retry=True):
    """Scrape um ticket no JULIE — retorna dict no formato padrão.

    Navega para a página de busca antes de cada pesquisa (evita stale state).
    Se não encontrar na primeira tentativa, recarrega e tenta mais uma vez.
    """
    result = {"location_text": "", "responses": [], "expire_date": ""}

    # Navega para página de busca (garante estado limpo)
    await page.goto(JULIE_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(1500)

    # Limpa input e digita ticket
    inp = page.locator('input[type="text"], input[type="search"]').first
    if not await inp.count():
        log.warning(f"[IL] {tnum}: input de busca não encontrado — recarregando")
        await page.goto(JULIE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)
        inp = page.locator('input[type="text"], input[type="search"]').first
        if not await inp.count():
            log.error(f"[IL] {tnum}: input de busca não encontrado após reload")
            return result

    await inp.click()
    await inp.fill("")
    await page.wait_for_timeout(200)
    await inp.fill(tnum)

    # Clica busca (ícone de lupa)
    btn = page.locator('button:near(input):visible').first
    try:
        await btn.click()
    except Exception:
        # Fallback: Enter
        await inp.press("Enter")
    await page.wait_for_timeout(4000)
    await wait_stable(page)

    body = await page.locator("body").inner_text()
    result["expire_date"] = extract_expire_date(body)

    # Verifica se o ticket foi encontrado — checa se a tabela "Ticket Details" existe
    # NÃO usar "no matching records" no body inteiro porque esse texto aparece
    # na tabela Responses vazia quando o ticket existe mas nenhuma utility respondeu ainda
    has_ticket_details = await page.locator('text="Ticket Details"').count() > 0
    if not has_ticket_details:
        # Fallback: checa se tem alguma tabela com dados do ticket
        has_any_table = await page.locator('table').count() > 0
        if not has_any_table or ("no matching records" in body.lower() and "ticket details" not in body.lower()):
            # Retry: recarrega e tenta mais uma vez
            if retry:
                log.debug(f"[IL] {tnum}: não encontrado na 1ª tentativa — retry")
                return await scrape_julie_ticket(page, tnum, retry=False)
            log.info(f"[IL] {tnum}: não encontrado no JULIE")
            return result

    # Parse tabelas via JS
    data = await page.evaluate("""() => {
        const tables = document.querySelectorAll('table');
        const result = {details: [], pending: [], responses: []};

        tables.forEach((table) => {
            const rows = table.querySelectorAll('tr');
            const headers = [];
            const data = [];
            rows.forEach((row, ri) => {
                const cells = row.querySelectorAll('th, td');
                const vals = Array.from(cells).map(c => c.innerText.trim());
                if (ri === 0) {
                    headers.push(...vals);
                } else if (vals.some(v => v.length > 0)) {
                    data.push(vals);
                }
            });

            const hdr = headers.join('|').toLowerCase();
            if (hdr.includes('ticket') && hdr.includes('revision') && hdr.includes('address')) {
                result.details = data;
            } else if (hdr.includes('member code') && hdr.includes('facility type') && !hdr.includes('response')) {
                result.pending = data;
            } else if (hdr.includes('response') && hdr.includes('member name') && hdr.includes('responded')) {
                result.responses = data;
            }
        });
        return result;
    }""")

    # Location text: Address + Street + Cross Street
    if data.get("details"):
        row = data["details"][0]
        # Cols: Ticket(0), Revision(1), Address(2), Street(3), Cross Street(4), Company(5), Locate By(6), Attachments(7)
        if len(row) >= 5:
            parts = [p for p in [row[2], row[3], row[4]] if p and p.strip() and p.strip() != "-"]
            result["location_text"] = " / ".join(parts) if parts else ""

    # Pending → No Response
    for row in data.get("pending", []):
        # Cols: Member Code(0), Facility Type(1), Member Name(2), Due(3), Comments(4), Attachments(5)
        if len(row) >= 3:
            member_name = (row[2] or "").strip()
            facility_type = (row[1] or "").strip()
            if member_name and member_name != "-" and "no matching" not in member_name.lower():
                result["responses"].append({
                    "utility": member_name,
                    "status_raw": "No Response",
                    "status": "Pending",
                    "response": "",
                    "comment": f"[{facility_type}]" if facility_type else "",
                })

    # Responses → parse respostas reais
    for row in data.get("responses", []):
        # Cols: Revision(0), Member Code(1), Facility Type(2), Member Name(3), Response(4), Description(5), Responded(6), Comments(7), URL(8), Attachments(9)
        if len(row) >= 7:
            member_name = (row[3] or "").strip()
            response_code = (row[4] or "").strip()
            description = (row[5] or "").strip()
            responded_str = (row[6] or "").strip()
            comment = (row[7] if len(row) > 7 else "").strip()

            if member_name and member_name != "-" and "no matching" not in member_name.lower():
                # ── IGNORAR ENTRADAS DE AGENDAMENTO (IL/JULIE) ──
                # Não são respostas reais de status — são negociações de schedule
                desc_upper = (description or "").upper()
                code_upper = (response_code or "").upper()
                IL_SCHEDULE_PATTERNS = [
                    "DECLINED CODE 50",
                    "ACCEPTED CODE 50",
                    "LOCATOR AND EXCAVATOR AGREED",
                    "DOCUMENTED ALTERNATE MARKING SCHEDULE",
                    "ALTERNATE DATE REQUESTED",
                ]
                is_schedule = any(pat in desc_upper or pat in code_upper for pat in IL_SCHEDULE_PATTERNS)
                if is_schedule:
                    log.debug(f"[IL] {tnum}: IGNORANDO entrada de agendamento — {member_name}: {description[:80]}")
                    continue

                # Parse responded date
                responded_date = None
                if responded_str and responded_str != "-":
                    for fmt in [
                        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p",
                        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
                        "%m/%d/%Y",
                    ]:
                        try:
                            responded_date = datetime.strptime(
                                responded_str[:len(fmt)+4].strip(), fmt
                            ).replace(tzinfo=None).isoformat()
                            break
                        except ValueError:
                            continue

                status, _cls_unrec = classify(response_code, description + " " + comment)
                result["responses"].append({
                    "utility": member_name,
                    "status_raw": response_code,
                    "status": status,
                    "response": description,
                    "comment": comment,
                    "responded_date": responded_date,
                    "_unrecognized": _cls_unrec
                })

    log.info(f"[IL] {tnum}: {len(result['responses'])} utilities")
    return result


async def scrape_il(ticket_numbers, tickets_data=None):
    """Scrape batch de tickets IL no JULIE (público, sem login).

    Usa NUM_TABS abas paralelas para acelerar o processo.
    """
    results = {}
    if not ticket_numbers:
        return results

    # Divide em chunks para abas paralelas
    chunks = [[] for _ in range(NUM_TABS)]
    for i, tnum in enumerate(ticket_numbers):
        chunks[i % NUM_TABS].append(tnum)
    chunks = [c for c in chunks if c]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])

        async def process_chunk(chunk, tab_id):
            """Processa um grupo de tickets em uma aba independente."""
            tab_results = {}
            page = await browser.new_page()
            page.set_default_timeout(30000)

            for idx, tnum in enumerate(chunk):
                log.info(f"[IL][T{tab_id}] ({idx+1}/{len(chunk)}) Ticket {tnum}")
                try:
                    result = await scrape_julie_ticket(page, tnum)
                    tab_results[tnum] = result
                except Exception as e:
                    log.error(f"[IL][T{tab_id}] {tnum}: ERRO → {e}")
                    tab_results[tnum] = {"location_text": "", "responses": [], "expire_date": ""}

            try:
                await page.close()
            except Exception:
                pass
            return tab_results

        log.info(f"[IL] Abrindo {len(chunks)} abas paralelas...")
        chunk_results = await asyncio.gather(*[process_chunk(c, i) for i, c in enumerate(chunks)])
        for cr in chunk_results:
            results.update(cr)

        await browser.close()

    return results


async def sync_il(triggered_by="manual"):
    """Sync completo Illinois — JULIE público (sem login)."""
    log.info(f"{'='*55}")
    log.info(f"  OneDrill 811 Sync  IL (JULIE)  [{triggered_by}]")
    log.info(f"{'='*55}")
    lid = log_start("IL", triggered_by)
    checked = 0
    summary = SyncSummary()
    try:
        all_tickets = sb_get("tickets", "&state=eq.IL&status=in.(Open,Damage,Clear)&order=ticket")
        if not all_tickets:
            log.info("[IL] Nenhum ticket ativo")
            log_finish(lid, 0, 0)
            return

        tickets_to_scrape, skipped = filter_tickets_for_sync(all_tickets, "IL")
        if skipped > 0:
            log.info(f"[IL] Cache: {skipped} tickets Clear pulados")

        checked = len(tickets_to_scrape)
        if not tickets_to_scrape:
            log.info(f"[IL] Nenhum ticket precisa verificação ({len(all_tickets)} ativos, {skipped} em cache)")
            log_finish(lid, 0, 0)
            return

        nums = [t["ticket"] for t in tickets_to_scrape]
        log.info(f"[IL] {checked} tickets para verificar (de {len(all_tickets)} ativos)")
        results = await scrape_il(nums, tickets_data=tickets_to_scrape)

        summary = save_to_supabase("IL", results, all_tickets)
        log_finish(lid, checked, summary.responses_saved)
        log.info(f"[IL] CONCLUÍDO  {checked} verificados, {skipped} em cache | {summary}")

    except Exception as e:
        log.error(f"[IL] FALHOU: {e}")
        log_finish(lid, checked, summary.responses_saved, "error", str(e))


async def save_ticket_pdfs_il(force=False):
    """Salva PDF de tickets IL via page.pdf() — JULIE é público, sem pyautogui.

    Muito mais simples que FL/IN: abre a página de response display no JULIE,
    busca o ticket e gera o PDF direto pelo Playwright (headless).
    """
    all_tickets = sb_get("tickets", "&state=eq.IL&status=in.(Clear,Damage)&order=ticket")
    if not all_tickets:
        log.info("[IL] PDF: nenhum ticket Clear/Damage")
        return

    if not force:
        all_tickets = [t for t in all_tickets if not any(
            (a.get("type") or "") == "ticket_pdf"
            for a in (t.get("attachments") or [])
        )]

    if not all_tickets:
        log.info("[IL] PDF: todos os tickets Clear/Damage já têm PDF")
        return

    log.info("=" * 55)
    log.info(f"  SAVE-PDF IL (JULIE): {len(all_tickets)} tickets Clear/Damage")
    log.info("=" * 55)

    base_dir = os.path.join(BASE_DIR, "pdfs")
    saved = 0
    errors = 0

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        page = await browser.new_page()
        page.set_default_timeout(30000)

        await page.goto(JULIE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(2000)

        for idx, t in enumerate(all_tickets):
            tnum = t["ticket"]
            tid = t["id"]

            # Decide qual número usar — renovado em grace usa o ANTIGO
            query_tnum, used_old = _pdf_query_number(t)
            if used_old:
                log.info(f"  {tnum}: 🔄 RENOVADO em grace — usando número antigo {query_tnum} pro PDF")

            client = re.sub(r'[/\\:*?"<>|]', '-', (t.get("client") or "SemCliente").strip()) or "SemCliente"
            pdf_dir = os.path.join(base_dir, "IL", client)
            os.makedirs(pdf_dir, exist_ok=True)
            pdf_filename = f"{query_tnum}.pdf" if used_old else f"{tnum}.pdf"
            pdf_path = os.path.join(pdf_dir, pdf_filename)
            full_path = os.path.abspath(pdf_path)

            if not force and os.path.exists(pdf_path):
                sz = os.path.getsize(pdf_path)
                if sz > 5000:
                    log.info(f"  {tnum}: PDF já existe ({round(sz/1024)}KB), pulando")
                    continue

            try:
                log.info(f"  ({idx+1}/{len(all_tickets)}) {tnum} (busca: {query_tnum})...")

                # Busca no JULIE (navega para estado limpo)
                await page.goto(JULIE_URL, wait_until="domcontentloaded")
                await page.wait_for_timeout(1500)
                inp = page.locator('input[type="text"], input[type="search"]').first
                await inp.click()
                await inp.fill("")
                await page.wait_for_timeout(200)
                await inp.fill(query_tnum)
                btn = page.locator('button:near(input):visible').first
                try:
                    await btn.click()
                except Exception:
                    await inp.press("Enter")
                await page.wait_for_timeout(4000)
                await wait_stable(page)

                body = await page.locator("body").inner_text()
                if "no matching records" in body.lower():
                    log.warning(f"  {tnum}: não encontrado no JULIE")
                    errors += 1
                    continue

                # Gera PDF via Playwright (headless)
                await page.pdf(path=full_path, format="Letter", print_background=True)

                if os.path.exists(full_path) and os.path.getsize(full_path) > 3000:
                    file_size = os.path.getsize(full_path)
                    log.info(f"  ✅ {tnum}: PDF salvo ({round(file_size/1024)}KB)")

                    attachments = t.get("attachments") or []
                    attachments = [a for a in attachments if a.get("type") != "ticket_pdf"]
                    att = {
                        "name": pdf_filename,
                        "type": "ticket_pdf",
                        "saved_at": datetime.now().isoformat(),
                        "size_kb": round(file_size / 1024, 1)
                    }
                    if used_old:
                        att["old_ticket"] = query_tnum
                        att["new_ticket"] = tnum
                    attachments.append(att)
                    hist = t.get("history") or []
                    action_txt = (f"📄 PDF salvo ({round(file_size/1024)}KB)"
                                  + (f" — usado # antigo {query_tnum}" if used_old else ""))
                    hist.append({
                        "ts": int(datetime.now().timestamp() * 1000),
                        "action": action_txt,
                        "color": "#7c3aed"
                    })
                    sb_patch("tickets", tid, {"attachments": attachments, "history": hist})
                    saved += 1
                else:
                    sz = os.path.getsize(full_path) if os.path.exists(full_path) else 0
                    log.warning(f"  ⚠ {tnum}: PDF não salvo ou pequeno ({sz}B)")
                    errors += 1

            except Exception as e:
                log.error(f"  ❌ {tnum}: {e}")
                errors += 1

        await browser.close()

    log.info("=" * 55)
    log.info(f"  SAVE-PDF IL CONCLUÍDO: {saved} salvos, {errors} erros")
    log.info("=" * 55)


# ── │ SECTION: WI │ WISCONSIN (Diggers Hotline) — SCRAPE PÚBLICO ──────────────

async def _wait_for_diggers_element(page, selector, timeout_s=15):
    """Espera por um elemento (via seletor CSS) em qualquer frame da página.

    Retorna (frame, locator) ou (None, None) se timeout.
    Usado pra encontrar o input ou tabela de resultado em qualquer frame ativo.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        for frame in page.frames:
            try:
                loc = frame.locator(selector).first
                if await loc.count() > 0:
                    return frame, loc
            except Exception:
                continue
        await page.wait_for_timeout(500)
    return None, None


async def scrape_diggers_ticket(page, tnum, retry=True, debug_dump=False):
    """Scrape um ticket no Diggers Hotline (Wisconsin) — formato padrão.

    O portal é uma SPA ExtJS 4.1. Estrutura confirmada:
      - Carregamento inicial: main page (header + toolbar) + iframe MpWelcome
      - Após click em #findTicketsButton-btnEl: ExtJS REMOVE o iframe e renderiza
        a view "Find" DIRETO no main frame (não em iframe novo)
      - Input: <input name="ticket-number"> dentro do main DOM
      - Botão: <button> com texto "Search" (CSS faz lowercase visualmente)
      - Resultado: injetado em <div class="x-gc-mp-pnl-ticket-find-result"> no main

    Status WI conhecidos:
      - "No Response"                        → Pending (utility não respondeu)
      - "Not Participating"                  → Clear   (não atende a área)
      - "Ongoing - Working with Excavator"   → Pending (em andamento)
      - "Marked"/"Cleared"/"Completed"       → Clear   (resposta normal)
      - "Damage"/"Damaged"                   → Damage

    debug_dump: salva screenshot + HTML quando algo falha.
    """
    result = {"location_text": "", "responses": [], "expire_date": ""}

    # ── 1. Navega pro portal ──────────────────────────────────────────────────
    await page.goto(DIGGERS_URL, wait_until="domcontentloaded")
    await page.wait_for_timeout(2500)  # ExtJS demora pra inicializar
    await wait_stable(page)

    # ── 2. Espera ExtJS renderizar o botão Find Tickets ──────────────────────
    try:
        await page.wait_for_selector('#findTicketsButton-btnEl', timeout=15000, state="visible")
    except Exception:
        if debug_dump:
            try:
                base = BASE_DIR
                ss = os.path.join(base, f"debug_wi_{tnum}_no_button.png")
                await page.screenshot(path=ss, full_page=True)
                log.error(f"[WI] {tnum}: botão Find Tickets não apareceu — debug: {ss}")
            except Exception:
                pass
        if retry:
            log.warning(f"[WI] {tnum}: botão Find Tickets não apareceu — retry")
            return await scrape_diggers_ticket(page, tnum, retry=False, debug_dump=True)
        return result

    # ── 3. Clica no botão Find Tickets ───────────────────────────────────────
    try:
        await page.locator('#findTicketsButton-btnEl').click()
    except Exception:
        try:
            await page.evaluate("""() => {
                const btn = document.querySelector('#findTicketsButton-btnEl');
                if (btn) btn.click();
            }""")
        except Exception as e:
            log.error(f"[WI] {tnum}: erro clicando Find Tickets: {e}")
            return result

    await page.wait_for_timeout(1500)

    # ── 4. Encontra o input pelo seletor estável (name="ticket-number") ──────
    # ExtJS gera IDs dinâmicos, mas o atributo name é estável.
    target_frame, inp = await _wait_for_diggers_element(
        page, 'input[name="ticket-number"]', timeout_s=15
    )

    if not target_frame or not inp:
        if debug_dump:
            try:
                base = BASE_DIR
                ss_path = os.path.join(base, f"debug_wi_{tnum}_no_input.png")
                html_path = os.path.join(base, f"debug_wi_{tnum}_no_input.html")
                await page.screenshot(path=ss_path, full_page=True)
                with open(html_path, "w", encoding="utf-8") as f:
                    f.write(await page.content())
                frame_urls = [f.url for f in page.frames]
                log.error(f"[WI] {tnum}: input ticket-number não encontrado.")
                log.error(f"  Debug: {ss_path}")
                log.error(f"  HTML:  {html_path}")
                log.error(f"  Frames: {frame_urls}")
            except Exception as e:
                log.error(f"[WI] {tnum}: erro salvando debug: {e}")
        if retry:
            log.warning(f"[WI] {tnum}: input não apareceu — retry")
            return await scrape_diggers_ticket(page, tnum, retry=False, debug_dump=True)
        return result

    log.debug(f"[WI] {tnum}: input encontrado em {target_frame.url[:100]}")

    # ── 5. Digita o ticket no input ──────────────────────────────────────────
    try:
        await inp.click()
        await inp.fill("")
        await page.wait_for_timeout(200)
        await inp.fill(tnum)
    except Exception as e:
        log.error(f"[WI] {tnum}: erro digitando ticket: {e}")
        return result

    # ── 6. Clica no botão "Search" ───────────────────────────────────────────
    # Texto real é "Search" (S maiúsculo); CSS aplica text-transform pra lowercase.
    # Playwright :has-text() é case-sensitive, então procuramos "Search".
    btn = target_frame.locator('button:has-text("Search")').first
    try:
        if await btn.count():
            await btn.click()
        else:
            await inp.press("Enter")
    except Exception:
        try:
            await inp.press("Enter")
        except Exception:
            log.error(f"[WI] {tnum}: erro clicando Search")
            return result

    # ── 7. Espera resultado carregar ─────────────────────────────────────────
    await page.wait_for_timeout(4000)
    await wait_stable(page)

    # Resultado é injetado no painel de resultado do main frame (ou em iframe novo).
    # Procura "Positive Response" OU "TICKET #" pra confirmar que carregou.
    result_frame, _ = await _wait_for_diggers_element(
        page, 'text=/Positive Response/i', timeout_s=10
    )
    if not result_frame:
        result_frame, _ = await _wait_for_diggers_element(
            page, 'text=/TICKET #/i', timeout_s=5
        )
    if not result_frame:
        result_frame = target_frame

    try:
        body = await result_frame.locator("body").inner_text()
    except Exception:
        body = await page.locator("body").inner_text()

    # Verifica se ticket foi encontrado
    body_lower = body.lower()
    has_ticket = (
        tnum in body
        and ("ticket #" in body_lower or "positive response" in body_lower or "diggers hotline" in body_lower)
    )
    if not has_ticket:
        if retry:
            log.debug(f"[WI] {tnum}: não encontrado na 1ª tentativa — retry")
            return await scrape_diggers_ticket(page, tnum, retry=False)
        log.info(f"[WI] {tnum}: não encontrado no Diggers")
        return result

    # ── 8. Extrai location_text (Address / Place / County) ───────────────────
    location_parts = []
    addr_match = re.search(r"\bAddress\s*:\s*([^\n]+)", body)
    place_match = re.search(r"\bPlace\s*:\s*([^\n]+)", body)
    county_match = re.search(r"\bCounty\s*:\s*([^\n]+)", body)
    if addr_match:
        location_parts.append(addr_match.group(1).strip())
    if place_match:
        location_parts.append(place_match.group(1).strip())
    if county_match:
        location_parts.append(county_match.group(1).strip() + " County")
    if location_parts:
        result["location_text"] = " / ".join(location_parts)

    # ── 9. Parse tabela Positive Response via JS dentro do frame ────────────
    js_extract = """() => {
        const out = {responses: []};
        const tables = document.querySelectorAll('table');
        for (const table of tables) {
            const txt = (table.innerText || '').toLowerCase();
            if (!txt.includes('status') || !txt.includes('name')) continue;
            if (!txt.includes('facilities') && !txt.includes('phone') && !txt.includes('code')) continue;

            const rows = Array.from(table.querySelectorAll('tr'));
            let headers = [];
            let headerIdx = -1;
            for (let ri = 0; ri < rows.length; ri++) {
                const cells = rows[ri].querySelectorAll('th, td');
                const vals = Array.from(cells).map(c => (c.innerText || '').trim());
                const lower = vals.map(v => v.toLowerCase());
                if (lower.some(v => v === 'status') && lower.some(v => v === 'name')) {
                    headers = lower;
                    headerIdx = ri;
                    break;
                }
            }
            if (headerIdx < 0) continue;

            const idx = (key) => headers.findIndex(h => h.includes(key));
            const iStatus = idx('status'),
                  iCode   = idx('code'),
                  iName   = idx('name'),
                  iFac    = idx('facilities'),
                  iPhone  = idx('phone');

            for (let ri = headerIdx + 1; ri < rows.length; ri++) {
                const cells = rows[ri].querySelectorAll('td');
                const vals = Array.from(cells).map(c => (c.innerText || '').trim());
                if (vals.length < 3) continue;
                if (vals.every(v => !v)) continue;

                const get = (i) => (i >= 0 && i < vals.length) ? vals[i] : '';
                const status = get(iStatus);
                const code   = get(iCode);
                let name     = get(iName);
                let facilities = get(iFac);
                const phone  = get(iPhone);

                if (!name) continue;

                // CRÍTICO: a célula "name" pode conter múltiplas linhas (nome + bullets de eventos).
                // Pegamos só a 1ª linha não-vazia — o nome real da utility.
                // Mesmo tratamento pra status e facilities (que podem ter eventos).
                name       = name.split('\\n').map(s => s.trim()).filter(Boolean)[0] || '';
                facilities = facilities.split('\\n').map(s => s.trim()).filter(Boolean)[0] || '';

                // Procura TODOS os eventos "Mmm DD, YYYY H:MM AM/PM || comentário" e pega o MAIS RECENTE.
                // Múltiplos eventos podem aparecer na mesma célula (ex: TIME WARNER CABLE com 3 eventos).
                const fullText = vals.join('\\n');
                const eventRegex = /([A-Za-z]{3,9}\\s+\\d{1,2},\\s+\\d{4}\\s+\\d{1,2}:\\d{2}\\s*[AP]M)\\s*\\|\\|\\s*([^\\n|]+)/g;
                const matches = [...fullText.matchAll(eventRegex)];
                let respondedDate = null;
                let comment = '';
                if (matches.length > 0) {
                    let bestIdx = 0;
                    let bestDate = new Date(matches[0][1]);
                    for (let mi = 1; mi < matches.length; mi++) {
                        const d = new Date(matches[mi][1]);
                        if (!isNaN(d) && (isNaN(bestDate) || d > bestDate)) {
                            bestDate = d;
                            bestIdx = mi;
                        }
                    }
                    respondedDate = matches[bestIdx][1].trim();
                    comment = matches[bestIdx][2].trim();
                }

                out.responses.push({status, code, name, facilities, phone, comment, respondedDate});
            }
        }
        return out;
    }"""

    try:
        data = await result_frame.evaluate(js_extract)
    except Exception:
        data = await page.evaluate(js_extract)

    # ── 10. Processa respostas ──────────────────────────────────────────────
    for row in data.get("responses", []):
        name = (row.get("name") or "").strip()
        code = (row.get("code") or "").strip()
        status_raw = (row.get("status") or "").strip()
        facilities = (row.get("facilities") or "").strip()
        comment = (row.get("comment") or "").strip()
        rd_str = row.get("respondedDate")

        if not name:
            continue

        if name.lower() in ("name", "status", "code", "facilities", "phone"):
            continue

        # Remove sufixo de código do nome (3 padrões observados no Diggers):
        #   1) " - CODE"   → "TIME WARNER CABLE - TWC30"
        #   2) " (CODE)"   → "WISCONSIN DOT - ITS EQUIPMENT (ITS02)"
        #   3) " CODE"     → "RACINE WATER UTILITY RWU01" / "WINDSTREAM NRL02"
        if code:
            for pat in [
                re.compile(r'\s*-\s*' + re.escape(code) + r'\s*$'),
                re.compile(r'\s*\(' + re.escape(code) + r'\)\s*$'),
                re.compile(r'\s+' + re.escape(code) + r'\s*$'),
            ]:
                if pat.search(name):
                    name = pat.sub('', name).strip(' -')
                    break

        # Catch-all fallback: remove "(CODE)" ou " CODE" no fim, mesmo sem code definido.
        # CODE = 2-5 letras maiúsculas + 1-4 dígitos (típico Diggers: RWU01, TWC30, ITS02, NRL02, DOT02).
        name = re.sub(r"\s*\([A-Z]{2,5}\d{1,4}\)\s*$", "", name).strip()
        name = re.sub(r"\s+[A-Z]{2,5}\d{1,4}\s*$", "", name).strip(' -')

        if not _is_valid_utility_name(name):
            log.debug(f"[WI] {tnum}: nome inválido descartado: {name!r}")
            continue

        # Parse responded_date (formato Diggers: "May 04, 2026 9:21 AM")
        responded_date = None
        if rd_str:
            for fmt in [
                "%b %d, %Y %I:%M %p",
                "%B %d, %Y %I:%M %p",
                "%b %d, %Y %I:%M:%S %p",
                "%B %d, %Y %I:%M:%S %p",
            ]:
                try:
                    responded_date = datetime.strptime(rd_str.strip(), fmt).replace(tzinfo=None).isoformat()
                    break
                except ValueError:
                    continue

        cls_status, cls_unrec = classify(status_raw, comment + " " + facilities)
        response_text = comment if comment else status_raw

        result["responses"].append({
            "utility": name,
            "status_raw": status_raw,
            "status": cls_status,
            "response": response_text,
            "comment": comment,
            "responded_date": responded_date,
            "_unrecognized": cls_unrec,
        })

    log.info(f"[WI] {tnum}: {len(result['responses'])} utilities")
    return result


async def scrape_wi(ticket_numbers, tickets_data=None):
    """Scrape batch de tickets WI no Diggers Hotline (público, sem login).

    Por enquanto roda SERIAL (1 aba) — portais legados costumam ter problemas
    de concorrência. Se ficar lento, dá pra aumentar pra NUM_TABS depois.
    """
    results = {}
    if not ticket_numbers:
        return results

    WI_NUM_TABS = 1  # serial por enquanto — sobe depois se funcionar

    chunks = [[] for _ in range(WI_NUM_TABS)]
    for i, tnum in enumerate(ticket_numbers):
        chunks[i % WI_NUM_TABS].append(tnum)
    chunks = [c for c in chunks if c]

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])

        async def process_chunk(chunk, tab_id):
            tab_results = {}
            page = await browser.new_page()
            page.set_default_timeout(30000)

            for idx, tnum in enumerate(chunk):
                log.info(f"[WI][T{tab_id}] ({idx+1}/{len(chunk)}) Ticket {tnum}")
                try:
                    debug = (idx < 2)
                    result = await scrape_diggers_ticket(page, tnum, debug_dump=debug)
                    tab_results[tnum] = result
                except Exception as e:
                    log.error(f"[WI][T{tab_id}] {tnum}: ERRO → {e}")
                    tab_results[tnum] = {"location_text": "", "responses": [], "expire_date": ""}

            try:
                await page.close()
            except Exception:
                pass
            return tab_results

        log.info(f"[WI] Abrindo {len(chunks)} aba(s) — modo {'serial' if WI_NUM_TABS == 1 else 'paralelo'}")
        chunk_results = await asyncio.gather(*[process_chunk(c, i) for i, c in enumerate(chunks)])
        for cr in chunk_results:
            results.update(cr)

        await browser.close()

    return results


async def sync_wi(triggered_by="manual"):
    """Sync completo Wisconsin — Diggers Hotline público (sem login)."""
    log.info(f"{'='*55}")
    log.info(f"  OneDrill 811 Sync  WI (Diggers Hotline)  [{triggered_by}]")
    log.info(f"{'='*55}")
    lid = log_start("WI", triggered_by)
    checked = 0
    summary = SyncSummary()
    try:
        all_tickets = sb_get("tickets", "&state=eq.WI&status=in.(Open,Damage,Clear)&order=ticket")
        if not all_tickets:
            log.info("[WI] Nenhum ticket ativo")
            log_finish(lid, 0, 0)
            return

        tickets_to_scrape, skipped = filter_tickets_for_sync(all_tickets, "WI")
        if skipped > 0:
            log.info(f"[WI] Cache: {skipped} tickets Clear pulados")

        checked = len(tickets_to_scrape)
        if not tickets_to_scrape:
            log.info(f"[WI] Nenhum ticket precisa verificação ({len(all_tickets)} ativos, {skipped} em cache)")
            log_finish(lid, 0, 0)
            return

        nums = [t["ticket"] for t in tickets_to_scrape]
        log.info(f"[WI] {checked} tickets para verificar (de {len(all_tickets)} ativos)")
        results = await scrape_wi(nums, tickets_data=tickets_to_scrape)

        summary = save_to_supabase("WI", results, all_tickets)
        log_finish(lid, checked, summary.responses_saved)
        log.info(f"[WI] CONCLUÍDO  {checked} verificados, {skipped} em cache | {summary}")

    except Exception as e:
        log.error(f"[WI] FALHOU: {e}")
        log_finish(lid, checked, summary.responses_saved, "error", str(e))


# ── │ SECTION: CONTACTS_FL │ SCRAPE CONTATOS DE UTILITIES (FL - Sunshine 811) 
def get_already_processed_tickets(state="FL"):
    """Retorna set de ticket_ref que já têm contatos salvos."""
    try:
        encoded_state = urllib.parse.quote(state, safe='')
        data = sb_get("utility_contacts", f"&state=eq.{encoded_state}&select=ticket_ref&ticket_ref=not.is.null")
        refs = {row["ticket_ref"] for row in data if row.get("ticket_ref")}
        log.info(f"[{state}] [SKIP-CHECK] {len(refs)} tickets já têm contatos salvos")
        return refs
    except Exception as e:
        log.warning(f"[{state}] Não foi possível checar tickets processados: {e}")
        return set()


def sb_upsert_contact(data):
    """Upsert contato — on_conflict utility_name+contact_name+state."""
    h = {**SB_H, "Prefer": "resolution=merge-duplicates,return=minimal"}
    allowed_fields = {"utility_name", "state", "phone_main", "phone_alt", "phone_emergency", "ticket_ref", "contact_name", "notes"}
    clean_data = {k: v for k, v in data.items() if k in allowed_fields and v}
    r = _sb_request(
        requests.post,
        f"{SB_URL}/rest/v1/utility_contacts?on_conflict=utility_name,contact_name,state",
        headers=h, json=clean_data, timeout=20
    )
    return r


def parse_contact_table(body, ticket_num, state):
    """Parse contatos de utilities do body da página Find Ticket (FL) ou Responses (IN).

    Inclui validação: descarta blocos com confiança baixa (utility name duvidoso, sem telefone).
    """
    contacts = []
    lines_list = body.split("\n")

    start_idx = -1
    for i, line in enumerate(lines_list):
        if "Positive Response" in line or ("Service Area" in line and "Contact" in line):
            start_idx = i + 1
            break

    if start_idx < 0:
        return contacts

    # Extrai blocos (cada bloco começa com No ou Yes)
    blocks = []
    current_block = []
    for i in range(start_idx, len(lines_list)):
        line = lines_list[i].strip()
        if not line:
            continue
        if line in ("No", "Yes") and current_block:
            blocks.append(current_block)
            current_block = [line]
        else:
            current_block.append(line)
    if current_block:
        blocks.append(current_block)

    phone_pat = re.compile(r'\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}')

    # Palavras que NÃO são nomes de utility (falsos positivos comuns)
    INVALID_UTILITY_NAMES = {
        "POSITIVE RESPONSE", "NO RESPONSE", "SERVICE AREA", "CONTACT",
        "DATE", "STATUS", "RESPONSE", "ENTRY METHOD", "COMMENTS",
        "TICKET", "SEARCH", "HOME", "DASHBOARD", "FILTER",
    }

    for block in blocks:
        if len(block) < 5:
            continue

        util_name = ""
        svc_code = ""
        util_type = ""

        # Encontra utility name com validação
        for j, bline in enumerate(block):
            if bline in ("No", "Yes"):
                continue
            # Validação: >5 chars, all caps, não é telefone, não é data, não é falso positivo
            if (len(bline) > 5
                    and bline == bline.upper()
                    and not phone_pat.search(bline)
                    and not bline.startswith("Date")
                    and bline.strip() not in INVALID_UTILITY_NAMES
                    and not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}", bline)):  # Não é data
                util_name = bline
                if j + 1 < len(block):
                    next_l = block[j + 1].strip()
                    if len(next_l) <= 10 and next_l == next_l.upper():
                        svc_code = next_l
                    if j + 2 < len(block):
                        type_l = block[j + 2].strip()
                        if (type_l == type_l.upper()
                                and not phone_pat.search(type_l)
                                and not type_l.startswith("Date")
                                and len(type_l) > 2):
                            util_type = type_l
                break

        if not util_name:
            continue

        # Encontra telefones com nome anterior
        phone_entries = []
        for j, bline in enumerate(block):
            phones = phone_pat.findall(bline)
            if phones:
                phone = phones[0].strip()
                name = ""
                if j > 0:
                    prev = block[j - 1].strip()
                    if not phone_pat.search(prev) and prev not in ("No", "Yes") and not prev.startswith("Date"):
                        name = prev
                name_in_line = bline.split(phone)[0].strip().rstrip("(").strip()
                if name_in_line and len(name_in_line) > 2:
                    name = name_in_line
                phone_entries.append({"name": name, "phone": phone})

        # Encontra respondent
        respondent = ""
        for bline in block:
            resp_match = re.search(r'Respondent[:\s]*(.+)', bline, re.IGNORECASE)
            if resp_match:
                respondent = resp_match.group(1).strip()
                if "(" in respondent:
                    respondent = respondent.split("(")[0].strip()
                break

        # Validação: bloco sem telefone E sem respondent = provavelmente lixo
        if not phone_entries and not respondent:
            log.debug(f"[{state}] {ticket_num}: bloco '{util_name[:30]}' descartado — sem telefone nem respondent")
            continue

        # Cria registro por PESSOA
        seen_names = set()
        roles = ["Contact", "Alternate", "Emergency"]
        for k, entry in enumerate(phone_entries[:3]):
            name = entry["name"]
            phone = entry["phone"]
            if not name or name == util_name or name == svc_code or name == util_type:
                continue
            name_key = name.upper().strip("* ").strip()
            if name_key in seen_names:
                continue
            seen_names.add(name_key)
            contacts.append({
                "utility_name": util_name,
                "service_area_code": svc_code,
                "contact_name": name.strip("* ").strip(),
                "phone_main": phone,
                "state": state,
                "ticket_ref": ticket_num,
                "notes": (roles[k] if k < 3 else "Other") + " | " + util_type,
            })

        if respondent:
            resp_key = respondent.upper().strip()
            if resp_key not in seen_names:
                contacts.append({
                    "utility_name": util_name,
                    "service_area_code": svc_code,
                    "contact_name": respondent,
                    "phone_main": "",
                    "state": state,
                    "ticket_ref": ticket_num,
                    "notes": "Respondent | " + util_type,
                })

    return contacts


async def scrape_contacts(state="FL", ticket_numbers=None, force=False):
    """Scrape contatos de utilities via Find Ticket (FL + IN)."""
    perfil = _profile_path(state)

    if not ticket_numbers:
        tickets_db = sb_get("tickets", f"&state=eq.{state}&status=in.(Open,Damage,Clear)&order=ticket")
        ticket_numbers = [t["ticket"] for t in tickets_db]

    if not ticket_numbers:
        log.info(f"[{state}] Nenhum ticket para buscar contatos")
        return 0

    log.info(f"[{state}] === Scraping contatos de {len(ticket_numbers)} tickets ===")
    saved = 0

    if not force:
        already_done = get_already_processed_tickets(state)
        ticket_numbers = [t for t in ticket_numbers if t not in already_done]
        log.info(f"[{state}] Após skip: {len(ticket_numbers)} tickets a processar")
    else:
        log.info(f"[{state}] --force ativo: processando todos os {len(ticket_numbers)} tickets")

    if not ticket_numbers:
        log.info(f"[{state}] Todos tickets já processados — nada a fazer")
        return 0

    # Pré-carrega contatos existentes em memória
    existing_contacts_set = set()
    try:
        all_existing = sb_get("utility_contacts", f"&state=eq.{state}&select=utility_name,contact_name")
        for row in all_existing:
            key = (row.get("utility_name", "").strip().upper(), row.get("contact_name", "").strip().upper())
            existing_contacts_set.add(key)
        log.info(f"[{state}] {len(existing_contacts_set)} contatos existentes em memória")
    except Exception as e:
        log.warning(f"[{state}] Não foi possível pré-carregar contatos: {e}")

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=False, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)

        if "login" in page.url.lower():
            await ctx.close()
            await asyncio.sleep(1)
            ok = await auto_login_silent(state)

            if not ok:

                log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")

                ok = await auto_login(state)
            if not ok:
                return 0
            await asyncio.sleep(1)
            ctx = await p.chromium.launch_persistent_context(perfil, headless=False, args=["--no-sandbox"])
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)

        for idx, tnum in enumerate(ticket_numbers):
            log.info(f"[{state}] Contatos ({idx+1}/{len(ticket_numbers)}) Ticket {tnum}")
            try:
                if state == "FL":
                    find_url = "https://exactix.sunshine811.com/findTicketByNumberAndPhone"
                    await page.goto(find_url, wait_until="domcontentloaded")
                    await page.wait_for_timeout(2500)

                    if "login" in page.url.lower():
                        sl = page.locator('a:has-text("Search here")').first
                        if await sl.count():
                            await sl.click()
                            await page.wait_for_timeout(2500)

                    # Mouse click em cada campo + digitar (simula pessoa real)
                    inputs = page.locator('input:visible')
                    ic = await inputs.count()

                    if ic >= 2:
                        box1 = await inputs.nth(0).bounding_box()
                        if box1:
                            await page.mouse.click(box1['x'] + box1['width']/2, box1['y'] + box1['height']/2)
                            await page.wait_for_timeout(200)
                            await page.keyboard.type(tnum, delay=40)
                            await page.wait_for_timeout(300)

                        # Preenche telefone via JavaScript (bypass Angular mask que adiciona "x")
                        phone_input = inputs.nth(1)
                        await phone_input.evaluate("""el => {
                            // Seta valor nativo via Angular
                            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(
                                window.HTMLInputElement.prototype, 'value'
                            ).set;
                            nativeInputValueSetter.call(el, '(321) 947-3131');
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                            el.dispatchEvent(new Event('blur', { bubbles: true }));
                        }""")
                        await page.wait_for_timeout(300)

                        log.info(f"[{state}] {tnum}: campos preenchidos via mouse+keyboard")

                    search_btn = page.locator('button:has-text("Search")').first
                    box3 = await search_btn.bounding_box() if await search_btn.count() else None
                    if box3:
                        await page.mouse.click(box3['x'] + box3['width']/2, box3['y'] + box3['height']/2)
                        log.info(f"[{state}] {tnum}: Search clicado")

                    try:
                        await page.wait_for_selector('text="Positive Response", mat-card, table', timeout=8000)
                    except Exception:
                        await page.wait_for_timeout(3000)

                    if idx < 2:
                        ss2 = os.path.join(BASE_DIR, f"debug_after_search_{state}_{tnum}.png")
                        await page.screenshot(path=ss2, full_page=True)
                        log.info(f"[{state}] Screenshots salvos")

                    await page.wait_for_timeout(500)

                elif state == "IN":
                    await page.goto(f"https://811.indiana811.org/tickets/{tnum}", wait_until="domcontentloaded")
                    await wait_stable(page)
                    resp_tab = page.locator('a:has-text("Responses"), [role="tab"]:has-text("Responses")').first
                    if await resp_tab.count():
                        await resp_tab.click()
                        await wait_stable(page, timeout=5000)

                body = await page.locator("body").inner_text()

                if idx < 2:
                    debug_path = os.path.join(BASE_DIR, f"debug_contacts_{state}_{tnum}.txt")
                    with open(debug_path, "w", encoding="utf-8") as f:
                        f.write(f"URL: {page.url}\n\n{body}")
                    log.info(f"[{state}] Debug salvo: {debug_path}")

                contacts = parse_contact_table(body, tnum, state)

                if contacts:
                    for contact in contacts:
                        db_record = {
                            "utility_name": contact.get("utility_name", ""),
                            "contact_name": contact.get("contact_name", ""),
                            "state": state,
                            "phone_main": contact.get("phone_main", ""),
                            "ticket_ref": tnum,
                            "notes": contact.get("notes", ""),
                        }
                        try:
                            cname = (db_record.get("contact_name", "") or "").strip()
                            uname = (db_record.get("utility_name", "") or "").strip()
                            dedup_key = (uname.upper(), cname.upper())
                            if cname and uname and dedup_key in existing_contacts_set:
                                continue
                            sb_upsert_contact(db_record)
                            if cname and uname:
                                existing_contacts_set.add(dedup_key)
                            saved += 1
                            log.info(f"[{state}] SALVO: {uname} / {cname} / {db_record.get('phone_main','')}")
                        except Exception as e:
                            log.error(f"[{state}] Erro salvando contato {db_record.get('utility_name','?')} / {db_record.get('contact_name','?')}: {e}")
                            if saved == 0:
                                log.error(f"[{state}] Data enviado: {db_record}")
                    log.info(f"[{state}] {tnum}: {len(contacts)} contatos extraídos")
                else:
                    log.warning(f"[{state}] {tnum}: nenhum contato encontrado")

            except Exception as e:
                log.error(f"[{state}] Contatos {tnum}: ERRO → {e}")

        try:
            await ctx.close()
        except Exception:
            pass

    log.info(f"[{state}] === Contatos: {saved} salvos/atualizados de {len(ticket_numbers)} tickets ===")
    return saved


# ── │ SECTION: UTILITY_HELPERS │ UTILITY HELPERS ──────────────────────────────
def get_contacts_for_utility(utility_name, state="FL"):
    try:
        data = sb_get("utility_contacts", f"&utility_name=eq.{_qv(utility_name)}&state=eq.{_qv(state)}")
        return data[0] if data else None
    except Exception:
        return None


def get_all_contacts(state="FL"):
    try:
        return sb_get("utility_contacts", f"&state=eq.{state}&order=utility_name")
    except Exception:
        return []


# ── │ SECTION: EXPORT_EXCEL │ EXPORT EXCEL ────────────────────────────────────
def export_excel():
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment

        log.info("Exportando Excel...")
        tickets = sb_get("tickets", "&order=ticket")
        projects = sb_get("projects", "&order=name")
        proj_map = {p["id"]: p["name"] for p in projects}
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Tickets"

        headers = [
            "Ticket #", "Projeto", "Cliente", "Prime", "Estado", "Local",
            "Status", "Footage", "Expira", "Tipo", "Endereço", "Job #",
            "Pending", "Empresa",
        ]
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1a6cf0")
            cell.alignment = Alignment(horizontal="center")

        STATUS_COLOR = {"Open": "FFCCCC", "Clear": "CCFFCC", "Damage": "FFE5B4", "Closed": "EEEEEE"}
        for row, t in enumerate(tickets, 2):
            vals = [
                t.get("ticket", ""), proj_map.get(t.get("project_id", ""), ""),
                t.get("client", ""), t.get("prime", ""), t.get("state", ""),
                t.get("location", ""), t.get("status", ""), t.get("footage", 0),
                t.get("expire", ""), t.get("tipo", ""), t.get("address", ""),
                t.get("job", ""), t.get("pending", ""), t.get("company", ""),
            ]
            for col, val in enumerate(vals, 1):
                cell = ws.cell(row=row, column=col, value=val)
                cell.fill = PatternFill("solid", fgColor=STATUS_COLOR.get(t.get("status", ""), "FFFFFF"))

        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = 18

        # Aba Projetos
        wp = wb.create_sheet("Projetos")
        ph = ["Nome", "Cliente", "Estado", "Status", "Total Feet", "Tickets"]
        for col, h in enumerate(ph, 1):
            cell = wp.cell(row=1, column=col, value=h)
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="1a6cf0")

        ticket_count = {}
        for t in tickets:
            pid = t.get("project_id", "")
            ticket_count[pid] = ticket_count.get(pid, 0) + 1

        for row, p_data in enumerate(projects, 2):
            for col, val in enumerate([
                p_data.get("name", ""), p_data.get("client", ""), p_data.get("state", ""),
                p_data.get("status", ""), p_data.get("total_feet", 0),
                ticket_count.get(p_data["id"], 0),
            ], 1):
                wp.cell(row=row, column=col, value=val)

        fname = f"OneDrill_Tickets_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
        wb.save(fname)
        log.info(f"Excel salvo: {fname}")
        return fname
    except Exception as e:
        log.error(f"Erro ao exportar Excel: {e}")
        return None


# ── │ SECTION: DEBUG_SCREENSHOT │ DEBUG ───────────────────────────────────────
async def debug_screenshot(state):
    perfil = _profile_path(state)
    if not os.path.exists(perfil):
        print(f"Perfil não encontrado: {perfil}")
        return
    async with playwright_context(state, headless=False) as (p, ctx, page):
        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_nav(page)
        log.info(f"[{state}] URL: {page.url}")
        if "login" in page.url.lower():
            log.warning(f"[{state}] Sessão expirada!")
            return
        await page.screenshot(path=f"debug_2_after_login_{state}.png")
        for sel in ['text="Go to Ticket Dashboard"', 'a:has-text("Ticket Dashboard")']:
            if await page.locator(sel).count():
                await click_and_wait(page, page.locator(sel), "nav")
                break
        await page.screenshot(path=f"debug_3_dashboard_{state}.png")
        with open(f"debug_page_{state}.html", "w", encoding="utf-8") as f:
            f.write(await page.content())
        await page.wait_for_timeout(20000)


# ── │ SECTION: BACKFILL │ BACKFILL HISTORY ────────────────────────────────────
def backfill_history():
    """Adiciona eventos de clear no histórico para tickets Clear que não têm.
    Usa data real da última resposta 811 (não datetime.now).
    """
    log.info("=== BACKFILL: Adicionando eventos de clear no histórico ===")
    all_clear = sb_get("tickets", "&status=eq.Clear&order=ticket")
    if not all_clear:
        log.info("Nenhum ticket Clear encontrado")
        return

    fixed = 0
    for t in all_clear:
        tid = t["id"]
        tnum = t["ticket"]
        state = t.get("state", "")
        hist = t.get("history") or []

        has_clear_evt = any(
            "auto 811" in (h.get("action", "")).lower() and "revertido" not in (h.get("action", "")).lower()
            for h in hist
        )
        has_clear_evt = has_clear_evt or any("→ clear" in (h.get("action", "")).lower() for h in hist)
        if has_clear_evt:
            continue

        # ── Busca data real da última resposta 811 ──
        # Filtra datas corrompidas (responded_at ≈ synced_at = fallback do sync)
        backfill_dt = None
        try:
            resps = sb_get(
                "ticket_811_responses",
                f"&ticket_num=eq.{tnum}&state=eq.{state}"
                "&responded_at=not.is.null&select=responded_at,synced_at"
            )
            real_dates = []
            for r in resps:
                ra = r.get("responded_at")
                sa = r.get("synced_at")
                if not ra:
                    continue
                try:
                    ra_dt = datetime.fromisoformat(str(ra).replace("Z", ""))
                    if sa:
                        sa_dt = datetime.fromisoformat(str(sa).replace("Z", ""))
                        if abs((ra_dt - sa_dt).total_seconds()) / 3600 < 6:
                            continue
                    real_dates.append(ra_dt)
                except Exception:
                    continue
            if real_dates:
                backfill_dt = max(real_dates)
        except Exception as e:
            log.debug(f"  {tnum}: erro ao buscar respostas: {e}")

        # Fallback: tenta extrair das notas
        if not backfill_dt:
            notes_text = t.get("notes") or ""
            date_match = re.search(r'\[AUTO 811\] Clear em (\d{1,2}/\d{1,2}/\d{4})', notes_text)
            if date_match:
                try:
                    backfill_dt = datetime.strptime(date_match.group(1), "%m/%d/%Y")
                except Exception:
                    pass

        # Último fallback: agora
        if not backfill_dt:
            backfill_dt = datetime.now()

        backfill_ts = int(backfill_dt.timestamp() * 1000)
        backfill_label = backfill_dt.strftime('%m/%d/%Y')

        hist.append({"ts": backfill_ts, "action": f"[AUTO 811] Clear em {backfill_label}", "color": "#16a34a"})
        sb_patch("tickets", tid, {"history": hist})
        log.info(f"  {tnum}: BACKFILL → {backfill_label}")
        fixed += 1

    log.info(f"=== BACKFILL CONCLUÍDO: {fixed} tickets corrigidos (de {len(all_clear)} Clear) ===")


def fix_clear_dates():
    """Corrige datas de clear no histórico de tickets que têm a data do sync
    em vez da data real da última resposta 811.

    Para cada ticket Clear:
    1. Busca o evento [AUTO 811] ou → clear no history
    2. Busca a data real da última resposta na tabela ticket_811_responses
    3. Se a data do history for diferente da data real, corrige o timestamp
    """
    log.info("=" * 55)
    log.info("  FIX-DATES: Corrigindo datas de clear no histórico")
    log.info("=" * 55)

    all_clear = sb_get("tickets", "&status=eq.Clear&order=ticket")
    if not all_clear:
        log.info("Nenhum ticket Clear encontrado")
        return

    fixed = 0
    skipped = 0
    no_response = 0

    for t in all_clear:
        tid = t["id"]
        tnum = t["ticket"]
        state = t.get("state", "")
        hist = t.get("history") or []

        # Encontra o evento de clear no history
        clear_evt_idx = None
        for i, h in enumerate(hist):
            a = (h.get("action", "") or "").lower()
            if ("auto 811" in a and "revertido" not in a) or "→ clear" in a or "auto-clear" in a:
                clear_evt_idx = i
                # Pega o ÚLTIMO evento de clear (mais recente)

        if clear_evt_idx is None:
            skipped += 1
            continue

        current_ts = hist[clear_evt_idx].get("ts", 0)

        # Busca data real da última resposta 811.
        # IMPORTANTE: responded_at pode estar corrompido (= synced_at quando parser
        # não capturou a data). Filtra apenas respostas onde responded_at difere
        # de synced_at por mais de 6h (indicando data real do portal).
        real_dt = None
        try:
            resps = sb_get(
                "ticket_811_responses",
                f"&ticket_num=eq.{tnum}&state=eq.{state}"
                "&responded_at=not.is.null&select=responded_at,synced_at"
            )
            real_dates = []
            for r in resps:
                ra = r.get("responded_at")
                if not ra:
                    continue
                try:
                    ra_dt = datetime.fromisoformat(str(ra).replace("Z", ""))
                    # Aceita todas as datas — o parser agora captura datas reais.
                    # Antes filtrava diff<6h vs synced_at, mas isso descartava
                    # respostas legítimas do mesmo dia.
                    real_dates.append(ra_dt)
                except Exception:
                    continue
            if real_dates:
                real_dt = max(real_dates)
        except Exception as e:
            log.debug(f"  {tnum}: erro ao buscar respostas: {e}")

        if not real_dt:
            no_response += 1
            continue

        real_ts = int(real_dt.timestamp() * 1000)
        real_label = real_dt.strftime('%m/%d/%Y')

        # Compara: se a diferença for > 1 hora, corrige
        diff_hours = abs(current_ts - real_ts) / 3600000
        if diff_hours < 1:
            skipped += 1
            continue

        # Corrige o timestamp e o texto do evento
        old_label = datetime.fromtimestamp(current_ts / 1000).strftime('%m/%d/%Y') if current_ts else "?"
        hist[clear_evt_idx]["ts"] = real_ts
        hist[clear_evt_idx]["action"] = f"[AUTO 811] Clear em {real_label}"

        # Também corrige nas notas se existir
        notes = t.get("notes") or ""
        if f"[AUTO 811] Clear em {old_label}" in notes:
            notes = notes.replace(
                f"[AUTO 811] Clear em {old_label}",
                f"[AUTO 811] Clear em {real_label}"
            )

        patch_data = {"history": hist, "updated_at": real_dt.isoformat()}
        if notes != (t.get("notes") or ""):
            patch_data["notes"] = notes

        sb_patch("tickets", tid, patch_data)
        log.info(f"  {tnum}: {old_label} → {real_label} (corrigido, diff={diff_hours:.0f}h)")
        fixed += 1

    log.info(f"{'=' * 55}")
    log.info(f"  FIX-DATES CONCLUÍDO:")
    log.info(f"    {fixed} corrigidos")
    log.info(f"    {skipped} já corretos / sem evento clear")
    log.info(f"    {no_response} sem data de resposta no banco")
    log.info(f"    {len(all_clear)} total Clear")
    log.info(f"{'=' * 55}")






# ── SECTION: LOCAL_OVERRIDES ────────────────────────────────────
# Regras manuais aplicadas DEPOIS do classify do portal 811.
# Match case-insensitive + substring em location + utility.
# Pra adicionar regra: editar local_overrides.json e rodar --apply-overrides 1 vez.

_OVERRIDES_JSON = os.path.join(BASE_DIR, "local_overrides.json")

def _load_overrides():
    if os.path.exists(_OVERRIDES_JSON):
        try:
            with open(_OVERRIDES_JSON, "r", encoding="utf-8") as f:
                data = _json.load(f)
            if isinstance(data, list) and data:
                log.info(f"[Overrides] {len(data)} regra(s) carregada(s) de local_overrides.json")
                return data
        except Exception as e:
            log.warning(f"[Overrides] Erro ao ler local_overrides.json: {e} — usando fallback hardcoded")
    return [
        {
            "name": "Frontier Terre Haute",
            "location_match": "terre haute",
            "utility_match": "frontier",
            "force_status": "Clear",
            "reason": "Override local: Frontier sempre Clear em Terre Haute (validado em campo)",
        },
    ]

LOCAL_OVERRIDES = _load_overrides()


def _apply_local_overrides(ticket, deduped_responses):
    """Aplica LOCAL_OVERRIDES no array de responses (in-place)."""
    location = (ticket.get("location") or "").lower()
    if not location:
        return 0
    altered = 0
    for ovr in LOCAL_OVERRIDES:
        if ovr["location_match"].lower() not in location:
            continue
        for resp in deduped_responses:
            uname = (resp.get("utility") or "").lower()
            if ovr["utility_match"].lower() not in uname:
                continue
            old_status = resp.get("status")
            if old_status == ovr["force_status"]:
                continue
            log.info(
                f"  [OVERRIDE] {ticket.get('ticket')} - "
                f"{resp.get('utility')}: {old_status} -> {ovr['force_status']} ({ovr['name']})"
            )
            resp["status"] = ovr["force_status"]
            resp["response"] = ovr["reason"]
            resp["comment"] = ovr["reason"]
            resp["status_raw"] = ovr["reason"]
            resp["_override"] = ovr["name"]
            altered += 1
    return altered


def apply_overrides_now(target_state=None):
    """Aplica LOCAL_OVERRIDES retroativamente em tickets existentes.

    Tambem dispara AUTO-CLEAR se todas as responses ficaram Clear
    (sem isso, dashboard 'Cleared Hoje' nao mostra o ticket).

    Inclui RENOVACOES: percorre tanto tickets antigos quanto renovados,
    aplica override em todos os que batem com location_match.
    """
    log.info("=" * 55)
    log.info("  APPLY-OVERRIDES: aplicando overrides retroativamente")
    if target_state:
        log.info(f"  Estado: {target_state}")
    for ovr in LOCAL_OVERRIDES:
        log.info(f"  - {ovr['name']}: '{ovr['location_match']}' + '{ovr['utility_match']}' = {ovr['force_status']}")
    log.info("=" * 55)

    query = "&status=in.(Open,Damage,Clear)&order=ticket&select=id,ticket,location,state,status,history,notes,old_ticket2"
    if target_state:
        query += f"&state=eq.{target_state}"

    tickets = sb_get("tickets", query)
    if not tickets:
        log.info("Nenhum ticket")
        return

    # Log dos tickets que VAO ser afetados (antes de mexer)
    candidates = []
    for _t in tickets:
        _loc = (_t.get("location") or "").lower()
        for _ovr in LOCAL_OVERRIDES:
            if _ovr["location_match"].lower() in _loc:
                _renew = _t.get("old_ticket2")
                _ri = f" (renovou {_renew})" if _renew else ""
                candidates.append(f"  [{_t.get('state','?')}] {_t['ticket']} status={_t.get('status')}{_ri}")
                break
    if candidates:
        log.info(f"Tickets candidatos a override ({len(candidates)}):")
        for c in candidates:
            log.info(c)
    else:
        log.info("Nenhum ticket bateu location_match")
        return

    total_updated = 0
    tickets_with_match = 0
    tickets_auto_cleared = 0

    for t in tickets:
        location = (t.get("location") or "").lower()
        if not location:
            continue

        matched_overrides = [o for o in LOCAL_OVERRIDES if o["location_match"].lower() in location]
        if not matched_overrides:
            continue

        responses = sb_get(
            "ticket_811_responses",
            f"&ticket_num=eq.{t['ticket']}&select=id,utility_name,status,response_text"
        )

        ticket_changed = False

        for ovr in matched_overrides:
            for r in responses:
                uname = (r.get("utility_name") or "").lower()
                if ovr["utility_match"].lower() not in uname:
                    continue
                if r.get("status") == ovr["force_status"]:
                    continue
                try:
                    sb_patch("ticket_811_responses", r["id"], {
                        "status": ovr["force_status"],
                        "response_text": ovr["reason"],
                        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat(),
                    })
                    log.info(
                        f"  [{t['state']}] {t['ticket']} - {r['utility_name']}: "
                        f"{r['status']} -> {ovr['force_status']} ({ovr['name']})"
                    )
                    r["status"] = ovr["force_status"]
                    total_updated += 1
                    ticket_changed = True
                except Exception as e:
                    log.error(f"  erro ao atualizar response {r['id']}: {e}")

        if ticket_changed:
            tickets_with_match += 1

        # AUTO-CLEAR + BACKFILL HISTORICO
        # Cobre 2 cenarios:
        #   1. Ticket Open/Damage com todas responses Clear -> muda pra Clear + adiciona entry
        #   2. Ticket JA Clear sem entry [AUTO 811] no historico -> so adiciona entry
        #      (pra aparecer em 'Cleared Hoje' do dashboard)
        try:
            n_responses = len(responses)
            statuses_summary = sorted(set((r.get("status") or "?") for r in responses))
            all_clear = n_responses > 0 and all(r.get("status") == "Clear" for r in responses)
            hist = t.get("history") or []
            has_clear_entry = any(
                "[AUTO 811] Clear em" in (h.get("action") or "")
                and "Revertido" not in (h.get("action") or "")
                for h in hist
            )
            log.info(
                f"    [DBG] {t['ticket']}: status={t.get('status')} "
                f"responses={n_responses} statuses={statuses_summary} "
                f"all_clear={all_clear} has_clear_entry={has_clear_entry}"
            )

            # Apenas auto-clear quando Open/Damage com todas responses Clear,
            # ou backfill quando Clear sem entry historico. NUNCA adiciona entry de HOJE
            # se ticket clareou em data ANTERIOR (pois seria mentira no historico).
            if n_responses > 0 and all_clear:
                needs_action = False
                patch_data = {}
                action_type = ""

                if t.get("status") in ("Open", "Damage"):
                    needs_action = True
                    patch_data["status"] = "Clear"
                    action_type = "AUTO-CLEAR"
                elif t.get("status") == "Clear" and not has_clear_entry:
                    needs_action = True
                    action_type = "BACKFILL HISTORICO"

                if needs_action:
                    now_dt = datetime.now()
                    now_ts = int(now_dt.timestamp() * 1000)
                    clear_label = now_dt.strftime("%m/%d/%Y")
                    clear_note = f"[AUTO 811] Clear em {clear_label} (override local)"
                    hist.append({"ts": now_ts, "action": clear_note, "color": "#16a34a"})
                    new_notes = append_auto_note(t.get("notes"), clear_note)
                    patch_data["history"] = hist
                    patch_data["notes"] = new_notes
                    try:
                        sb_patch("tickets", t["id"], patch_data)
                        log.info(f"  [{t['state']}] {t['ticket']}: {action_type}")
                        tickets_auto_cleared += 1
                    except Exception as e:
                        log.error(f"  [{t['ticket']}] erro: {e}")
        except Exception as e:
            log.warning(f"  [{t['ticket']}] erro check auto-clear: {e}")

    log.info("=" * 55)
    log.info(f"  APPLY-OVERRIDES CONCLUIDO:")
    log.info(f"    Tickets candidatos:      {len(candidates)}")
    log.info(f"    Tickets com mudanca:     {tickets_with_match}")
    log.info(f"    Responses atualizadas:   {total_updated}")
    log.info(f"    Tickets auto-clarados:   {tickets_auto_cleared}")
    log.info("=" * 55)




def undo_fake_overrides_today(target_state=None):
    """Desfaz entries falsas '(override local)' adicionadas HOJE em historico.

    Necessario apos rodar apply_overrides_now versao com bug RE-CONFIRMA HOJE,
    que adicionou entries de hoje em tickets que clarearam em outras datas.
    """
    log.info("=" * 55)
    log.info("  UNDO-FAKE-OVERRIDES: removendo entries falsas de hoje")
    log.info("=" * 55)

    today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_ms = int(today_midnight.timestamp() * 1000)

    query = "&status=in.(Open,Damage,Clear)&order=ticket&select=id,ticket,state,history"
    if target_state:
        query += f"&state=eq.{target_state}"

    tickets = sb_get("tickets", query)
    if not tickets:
        log.info("Nenhum ticket")
        return

    fixed = 0
    for t in tickets:
        hist = t.get("history") or []
        if not hist:
            continue

        # Filtra entries que NAO sao (override local) com ts >= hoje
        new_hist = []
        removed = 0
        for h in hist:
            action = h.get("action") or ""
            ts = h.get("ts", 0)
            is_override_today = (
                "(override local)" in action
                and ts >= today_ms
            )
            if is_override_today:
                removed += 1
            else:
                new_hist.append(h)

        if removed > 0:
            try:
                sb_patch("tickets", t["id"], {"history": new_hist})
                log.info(f"  [{t.get('state','?')}] {t['ticket']}: removidas {removed} entries falsas")
                fixed += 1
            except Exception as e:
                log.error(f"  [{t['ticket']}] erro: {e}")

    log.info("=" * 55)
    log.info(f"  UNDO concluido: {fixed} tickets ajustados")
    log.info("=" * 55)



def debug_ticket_history(ticket_num):
    """Mostra historico completo de um ticket pra debug.

    Lista todas entries do history com ts formatado, action e cor.
    Util pra entender por que ticket aparece (ou nao) em Cleared Hoje.
    """
    log.info("=" * 55)
    log.info(f"  DEBUG HISTORICO - Ticket: {ticket_num}")
    log.info("=" * 55)

    tickets = sb_get("tickets", f"&ticket=eq.{_qv(ticket_num)}&select=id,ticket,state,status,location,history,old_ticket2")
    if not tickets:
        log.info(f"Ticket {ticket_num} nao achado")
        return

    t = tickets[0]
    log.info(f"Estado: {t.get('state')}")
    log.info(f"Status: {t.get('status')}")
    log.info(f"Location: {t.get('location')}")
    log.info(f"Old ticket: {t.get('old_ticket2') or '(nenhum)'}")
    log.info("")

    hist = t.get("history") or []
    if not hist:
        log.info("(historico vazio)")
        return

    log.info(f"History ({len(hist)} entries):")
    for i, h in enumerate(hist, 1):
        ts = h.get("ts", 0)
        try:
            dt_str = datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            dt_str = f"ts={ts}"
        action = h.get("action", "")
        color = h.get("color", "")
        log.info(f"  {i}. [{dt_str}] {action} (color={color})")

    log.info("")
    log.info("=" * 55)
    log.info("  Responses (utilities):")
    log.info("=" * 55)
    responses = sb_get("ticket_811_responses", f"&ticket_num=eq.{_qv(ticket_num)}&select=utility_name,status,response_text,synced_at&order=utility_name")
    for r in responses:
        log.info(f"  {r.get('utility_name'):40} {r.get('status'):8} synced={r.get('synced_at','')[:19]}")
        rt = (r.get('response_text') or '')[:80]
        if rt:
            log.info(f"      response: {rt}")
    log.info("=" * 55)


def apply_today_clears(target_state=None, target_date=None):
    """Adiciona entry '[AUTO 811] Last utility Clear em hoje' no historico de
    tickets que ja estavam Clear mas cujas utilities responderam HOJE.

    Cenario: ticket era Clear (status), mas no portal 811 utilities responderam
    HOJE. O sync detecta as responses mas nao adiciona entry no historico
    porque o status nao mudou. Resultado: dashboard 'Cleared Hoje' nao pega.

    Esse script roda 1x e adiciona a entry retroativamente.
    """
    log.info("=" * 55)
    log.info("  APPLY-TODAY-CLEARS: marcando tickets com atividade hoje")
    if target_state:
        log.info(f"  Estado: {target_state}")
    log.info("=" * 55)

    if target_date:
        try:
            target_dt = datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError:
            log.error(f"Data invalida: {target_date} (use YYYY-MM-DD)")
            return
        log.info(f"  Data alvo: {target_date}")
    else:
        target_dt = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    today_midnight = target_dt
    today_ms = int(today_midnight.timestamp() * 1000)
    today_key = today_midnight.strftime("%Y-%m-%d")
    entry_ts = int(datetime.now().timestamp() * 1000)
    entry_label = datetime.now().strftime("%m/%d/%Y")

    query = "&status=eq.Clear&order=ticket&select=id,ticket,state,history,notes"
    if target_state:
        query += f"&state=eq.{target_state}"

    tickets = sb_get("tickets", query)
    if not tickets:
        log.info("Nenhum ticket Clear")
        return

    log.info(f"Analisando {len(tickets)} tickets Clear...")

    updated = 0
    skipped_no_today_response = 0
    skipped_already_today = 0

    for t in tickets:
        # Verifica se TEM resposta de hoje (responded_at >= hoje)
        responses = sb_get(
            "ticket_811_responses",
            f"&ticket_num=eq.{t['ticket']}&status=eq.Clear&order=responded_at.desc&limit=1&select=responded_at,utility_name"
        )
        if not responses:
            continue

        latest = responses[0].get("responded_at", "")
        if not latest or latest[:10] != today_key:
            skipped_no_today_response += 1
            continue

        # Tem resposta de hoje! Verifica se historico ja tem entry de hoje
        hist = t.get("history") or []
        has_today_entry = False
        for h in hist:
            ts = h.get("ts", 0)
            action = h.get("action") or ""
            if ts >= today_ms and "[AUTO 811]" in action and "Revertido" not in action:
                has_today_entry = True
                break

        if has_today_entry:
            skipped_already_today += 1
            continue

        utility = responses[0].get("utility_name", "?")
        clear_note = f"[AUTO 811] Last utility Clear em {today_key} ({utility})"
        hist.append({"ts": entry_ts, "action": clear_note, "color": "#16a34a"})

        try:
            sb_patch("tickets", t["id"], {"history": hist})
            log.info(f"  [{t.get('state','?')}] {t['ticket']}: entry adicionada ({utility} respondeu {latest[:10]})")
            updated += 1
        except Exception as e:
            log.error(f"  [{t['ticket']}] erro: {e}")

    log.info("=" * 55)
    log.info(f"  APPLY-TODAY-CLEARS CONCLUIDO:")
    log.info(f"    Tickets atualizados:           {updated}")
    log.info(f"    Sem resposta de hoje:          {skipped_no_today_response}")
    log.info(f"    Ja tinha entry de hoje:        {skipped_already_today}")
    log.info("=" * 55)


# ── SECTION: EMAIL_SCANNER ──────────────────────────────────────
def _decode_email_header(h):
    """Decodifica header de email com multiple encodings."""
    if not h:
        return ""
    try:
        import email
        from email.header import decode_header
        parts = decode_header(h)
        out = ""
        for txt, enc in parts:
            if isinstance(txt, bytes):
                out += txt.decode(enc or "utf-8", errors="replace")
            else:
                out += txt
        return out
    except Exception:
        return str(h)


def _extract_email_body(msg):
    """Extrai body de email (plain ou HTML reduzido)."""
    if msg.is_multipart():
        for part in msg.walk():
            ctype = part.get_content_type()
            if ctype in ("text/plain", "text/html"):
                try:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
                        if ctype == "text/html":
                            body = re.sub(r"<[^>]+>", " ", body)
                            body = re.sub(r"&nbsp;", " ", body)
                            body = re.sub(r"\s+", " ", body)
                        return body
                except Exception:
                    pass
    else:
        try:
            payload = msg.get_payload(decode=True)
            if payload:
                body = payload.decode(msg.get_content_charset() or "utf-8", errors="replace")
                if msg.get_content_type() == "text/html":
                    body = re.sub(r"<[^>]+>", " ", body)
                    body = re.sub(r"\s+", " ", body)
                return body
        except Exception:
            return ""
    return ""


def scan_emails_for_responses(commit=False, state_filter=None, days_back=7):
    """Varre Gmail buscando emails de status change das utilities 811.

    Eric reportou que utilities respondem 'Not Participating' no portal
    mas mandam email confirmando status. Esse scanner faz parser desses
    emails e atualiza no banco SE a utility ta como Not Participating.

    Args:
        commit: Se True, atualiza banco. Se False (default), so loga (dry run).
        state_filter: Se passado, filtra so tickets desse estado.
        days_back: Quantos dias pra tras varrer emails (default 7).
    """
    import imaplib
    import email as email_mod

    GMAIL_USER = os.getenv("GMAIL_USER")
    GMAIL_PASS = os.getenv("GMAIL_PASS")

    if not GMAIL_USER or not GMAIL_PASS:
        log.error("[EmailScan] GMAIL_USER/GMAIL_PASS nao definidos no .env")
        return

    log.info("=" * 55)
    log.info(f"  EMAIL SCANNER {'(DRY RUN)' if not commit else '(COMMIT)'}")
    log.info(f"  Buscando emails dos ultimos {days_back} dias")
    if state_filter:
        log.info(f"  Estado: {state_filter}")
    log.info("=" * 55)

    try:
        imap = imaplib.IMAP4_SSL("imap.gmail.com")
        imap.login(GMAIL_USER, GMAIL_PASS)
        imap.select("INBOX")
    except Exception as e:
        log.error(f"[EmailScan] Erro IMAP login: {e}")
        return

    since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
    status, msg_ids = imap.search(None, f'(SINCE {since_date})')

    if status != "OK" or not msg_ids[0]:
        log.info("[EmailScan] Nenhum email no periodo")
        imap.logout()
        return

    ids = msg_ids[0].split()
    log.info(f"[EmailScan] Analisando {len(ids)} emails...")

    matched = 0
    updated = 0
    skipped_neg = 0
    skipped_no_ticket = 0
    skipped_no_match = 0
    errors = 0

    for msg_id in ids:
        try:
            status, msg_data = imap.fetch(msg_id, "(RFC822)")
            if status != "OK" or not msg_data or not msg_data[0]:
                continue

            raw = msg_data[0][1]
            msg = email_mod.message_from_bytes(raw)

            subject = _decode_email_header(msg.get("Subject", ""))
            sender = _decode_email_header(msg.get("From", ""))

            # Filtro grosso por subject
            subj_lower = subject.lower()
            if not any(kw in subj_lower for kw in ["ticket", "status change", "cleared", "positive response"]):
                continue

            body = _extract_email_body(msg)
            if not body:
                continue

            # Parser
            ticket_m = re.search(r"Ticket\s*#?:?\s*(\d{8,})", body)
            if not ticket_m:
                # Tenta no subject
                ticket_m = re.search(r"Ticket\s*#?\s*(\d{8,})", subject)
            member_m = re.search(r"Member\s*Code:?\s*(\w+)", body)
            response_m = re.search(r"Response:?\s*_?(\w+)", body)
            if not response_m:
                # Tenta "Work Performed"
                response_m = re.search(r"Work\s*Performed:?\s*(\w+)", body)

            if not (ticket_m and member_m and response_m):
                continue

            ticket_num = ticket_m.group(1)
            member_code = member_m.group(1).upper()
            response = response_m.group(1).upper()

            matched += 1
            log.info(f"  [{ticket_num}] member={member_code} response={response} from={sender[:40]}")

            if response not in ("CLEARED", "MARKED", "COMPLETE", "CLEAR", "COMPLETED"):
                log.info(f"    -> response nao-positiva, pula")
                skipped_neg += 1
                continue

            tickets = sb_get("tickets", f"&ticket=eq.{_qv(ticket_num)}&select=id,state,ticket")
            if not tickets:
                log.warning(f"    -> ticket {ticket_num} nao achado no banco")
                skipped_no_ticket += 1
                continue

            t = tickets[0]
            if state_filter and t.get("state") != state_filter:
                continue

            responses_at_ticket = sb_get(
                "ticket_811_responses",
                f"&ticket_num=eq.{ticket_num}&select=id,utility_name,status,response_text"
            )

            target = None
            for r in responses_at_ticket:
                uname = (r.get("utility_name") or "").upper()
                rt = (r.get("response_text") or "").lower()
                if member_code in uname:
                    if "not participating" in rt or "not service provider" in rt or r.get("status") == "Pending":
                        target = r
                        break

            if not target:
                log.info(f"    -> nenhuma utility com '{member_code}' marcada como Not Participating/Pending no ticket")
                skipped_no_match += 1
                continue

            log.info(f"    -> MATCH: {target['utility_name']} (era: '{(target.get('response_text') or '')[:50]}')")

            if commit:
                new_response_text = f"Email confirmation ({member_code}): {response}"
                try:
                    sb_patch("ticket_811_responses", target["id"], {
                        "status": "Clear",
                        "response_text": new_response_text,
                        "synced_at": datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
                    })
                    log.info(f"    -> ATUALIZADO no banco")
                    updated += 1
                except Exception as e:
                    log.error(f"    -> ERRO ao atualizar: {e}")
                    errors += 1
            else:
                log.info(f"    -> [DRY RUN] atualizaria pra Clear")
                updated += 1
        except Exception as e:
            log.warning(f"  Erro processando email: {e}")
            errors += 1
            continue

    try:
        imap.logout()
    except Exception:
        pass

    log.info("=" * 55)
    log.info(f"  EMAIL SCANNER CONCLUIDO:")
    log.info(f"    Emails analisados:        {len(ids)}")
    log.info(f"    Parser identificou:       {matched}")
    log.info(f"    Updated{' (DRY)' if not commit else ''}:                  {updated}")
    log.info(f"    Skipped (resp negativa):  {skipped_neg}")
    log.info(f"    Skipped (ticket no banco):{skipped_no_ticket}")
    log.info(f"    Skipped (sem match):      {skipped_no_match}")
    log.info(f"    Erros:                    {errors}")
    log.info("=" * 55)



# ── │ SECTION: PDF_SAVE │ SAVE PDF (via Print Dialog — simula humano) ────────


def fix_clear_ts(target_state=None):
    """Fix 2026-05-14: corrige timestamp dos [AUTO 811] Clear no historico.

    Tickets gravados pelo codigo antigo ficaram com ts = data da resposta da
    utility (pode ser ontem) em vez do momento do auto-clear (hoje). Resultado:
    aparecem no dia errado no dashboard 'Cleared Hoje'.

    Aproximacao: usa synced_at MAX das responses Clear do ticket como ts.
    """
    log.info("=" * 55)
    log.info("  FIX-CLEAR-TS: Corrigindo timestamp dos [AUTO 811] Clear")
    if target_state:
        log.info(f"  Estado: {target_state}")
    log.info("=" * 55)

    query = "&status=in.(Clear,Damage)&order=ticket"
    if target_state:
        query += f"&state=eq.{target_state}"

    all_tickets = sb_get("tickets", query)
    if not all_tickets:
        log.info("Nenhum ticket encontrado")
        return

    log.info(f"Processando {len(all_tickets)} tickets...")
    fixed = 0
    skipped_no_problem = 0
    skipped_no_response = 0
    skipped_no_history = 0

    for t in all_tickets:
        hist = t.get("history") or []
        if not hist:
            skipped_no_history += 1
            continue

        changed = False

        for entry in hist:
            action = entry.get("action", "")
            ts = entry.get("ts", 0)

            if "[AUTO 811] Clear em" not in action or "Revertido" in action:
                continue

            m = re.search(r"\[AUTO 811\] Clear em (\d{1,2}/\d{1,2}/\d{4})", action)
            if not m:
                continue

            try:
                label_dt = datetime.strptime(m.group(1), "%m/%d/%Y")
                label_ts = int(label_dt.timestamp() * 1000)
            except ValueError:
                continue

            # Se ts ja eh maior que label + 36h, foi corrigido
            if ts > label_ts + 36 * 3600 * 1000:
                skipped_no_problem += 1
                continue

            responses = sb_get(
                "ticket_811_responses",
                f"&ticket_num=eq.{t['ticket']}&status=eq.Clear"
                f"&order=synced_at.desc&limit=1&select=synced_at"
            )

            if not responses or not responses[0].get("synced_at"):
                skipped_no_response += 1
                continue

            try:
                synced_str = responses[0]["synced_at"]
                if synced_str.endswith("Z"):
                    synced_str = synced_str[:-1] + "+00:00"
                synced_dt = datetime.fromisoformat(synced_str)
                new_ts = int(synced_dt.timestamp() * 1000)

                if new_ts > ts:
                    log.info(
                        f"  [{t.get('state','?')}] {t['ticket']}: "
                        f"ts {datetime.fromtimestamp(ts/1000).strftime('%Y-%m-%d')} -> "
                        f"{datetime.fromtimestamp(new_ts/1000).strftime('%Y-%m-%d')}"
                    )
                    entry["ts"] = new_ts
                    changed = True
                    fixed += 1
                else:
                    skipped_no_problem += 1
            except Exception as e:
                log.warning(f"  [{t['ticket']}] erro parse synced_at: {e}")
                skipped_no_response += 1

        if changed:
            try:
                sb_patch("tickets", t["id"], {"history": hist})
            except Exception as e:
                log.error(f"  [{t['ticket']}] erro ao salvar: {e}")

    log.info("=" * 55)
    log.info(f"  FIX-CLEAR-TS CONCLUIDO:")
    log.info(f"    {fixed} corrigidos")
    log.info(f"    {skipped_no_problem} ja corretos")
    log.info(f"    {skipped_no_response} sem synced_at no banco")
    log.info(f"    {skipped_no_history} sem historico")
    log.info("=" * 55)



def _find_chrome_hwnd():
    """Encontra a janela do Chrome/Chromium via Win32 API."""
    import ctypes
    import ctypes.wintypes

    results = []

    @ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.wintypes.HWND, ctypes.wintypes.LPARAM)
    def callback(hwnd, _):
        if ctypes.windll.user32.IsWindowVisible(hwnd):
            buf = ctypes.create_unicode_buffer(512)
            ctypes.windll.user32.GetWindowTextW(hwnd, buf, 512)
            title = buf.value
            if any(kw in title for kw in ['Chrome', 'Chromium', 'Ticket', 'Sunshine', '811']):
                results.append((hwnd, title))
        return True

    ctypes.windll.user32.EnumWindows(callback, 0)
    if results:
        # Prioriza janela com "Ticket" no título (é a que está com o print dialog)
        for hwnd, title in results:
            if 'Ticket' in title:
                return hwnd, title
        return results[0]
    return None, ""


def _get_window_rect(hwnd):
    """Retorna (left, top, right, bottom) da janela."""
    import ctypes
    import ctypes.wintypes
    rect = ctypes.wintypes.RECT()
    ctypes.windll.user32.GetWindowRect(hwnd, ctypes.byref(rect))
    return rect.left, rect.top, rect.right, rect.bottom


def _bring_to_front(hwnd):
    """Traz janela pro foreground."""
    import ctypes
    ctypes.windll.user32.SetForegroundWindow(hwnd)


def _pdf_query_number(t):
    """Decide qual número usar pra buscar o ticket nos portais durante geração de PDF.

    Pra tickets RENOVADOS ainda em grace period, o ticket NOVO foi enviado mas
    pode não ter respostas das utilities (que responderam no ANTIGO). Pra
    salvar o PDF com evidência legal das respostas Clear, busca pelo número
    ANTIGO durante grace. Fora de grace, usa o NOVO normal.

    Retorna (query_number, is_old). is_old=True significa que usou o antigo.
    """
    tnum_new = (t.get("ticket") or "").strip()
    in_grace, old_num = is_in_renewal_grace(t)
    if in_grace and old_num:
        return old_num, True
    return tnum_new, False


async def save_ticket_pdfs(state="FL", force=False):
    """Salva PDF de tickets Clear e Damage simulando humano: clica impressora → Save as PDF.

    Produz PDF idêntico ao salvo manualmente — válido para evidência legal.
    Requer: pip install pyautogui
    NOTA: NÃO USE mouse/teclado enquanto roda. Rode após expediente.

    Fluxo por ticket:
      1. Dashboard → Filtrar ticket → Abrir → Aba Text
      2. JS click no FAB verde (impressora)
      3. Encontra janela do Chrome via Win32 API
      4. Clica Destination → Save as PDF → Save
      5. Cola path do arquivo → Enter
    """
    try:
        import pyautogui
    except ImportError:
        log.error("[PDF] pyautogui não instalado. Rode: pip install pyautogui")
        return

    pyautogui.FAILSAFE = True
    pyautogui.PAUSE = 0.3

    all_tickets = sb_get("tickets", f"&state=eq.{state}&status=in.(Clear,Damage)&order=ticket")
    if not all_tickets:
        log.info(f"[{state}] PDF: nenhum ticket Clear/Damage")
        return

    if not force:
        all_tickets = [t for t in all_tickets if not any(
            (a.get("type") or "") == "ticket_pdf"
            for a in (t.get("attachments") or [])
        )]

    if not all_tickets:
        log.info(f"[{state}] PDF: todos os tickets Clear/Damage já têm PDF")
        return

    log.info("=" * 55)
    log.info(f"  SAVE-PDF: {len(all_tickets)} tickets Clear/Damage ({state})")
    log.info(f"  ⚠ NÃO USE mouse/teclado enquanto roda")
    log.info("=" * 55)

    base_dir = os.path.join(BASE_DIR, "pdfs")
    perfil = _profile_path(state)
    saved = 0
    errors = 0

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(
            perfil, headless=False, args=["--no-sandbox", "--start-maximized"],
            no_viewport=True  # Usa tela inteira, não viewport fixo
        )
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)

        if "login" in page.url.lower():
            log.warning(f"[{state}] PDF: sessão expirada — tentando login...")
            await ctx.close()
            await asyncio.sleep(1)
            ok = await auto_login_silent(state)

            if not ok:

                log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")

                ok = await auto_login(state)
            if not ok:
                log.error(f"[{state}] PDF: login falhou")
                return
            await asyncio.sleep(1)
            ctx = await p.chromium.launch_persistent_context(
                perfil, headless=False, args=["--no-sandbox", "--start-maximized"],
                no_viewport=True
            )
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)

        ok = await goto_dashboard(page, state)
        if not ok:
            log.error(f"[{state}] PDF: dashboard inacessível")
            await ctx.close()
            return

        log.info(f"[{state}] PDF: dashboard OK — processando {len(all_tickets)} tickets")

        # Flag: se Save as PDF já foi selecionado 1x, Chrome lembra nas próximas
        pdf_dest_selected = False

        for idx, t in enumerate(all_tickets):
            tnum = t["ticket"]
            tid = t["id"]

            # Decide qual número usar pra busca/PDF — ticket renovado em grace
            # usa o ANTIGO (que tem as respostas Clear das utilities)
            query_tnum, used_old = _pdf_query_number(t)
            if used_old:
                log.info(f"  {tnum}: 🔄 RENOVADO em grace — usando número antigo {query_tnum} pro PDF")

            client = re.sub(r'[/\\:*?"<>|]', '-', (t.get("client") or "SemCliente").strip()) or "SemCliente"
            pdf_dir = os.path.join(base_dir, state, client)
            os.makedirs(pdf_dir, exist_ok=True)
            # Nome do arquivo PDF: sempre o número ANTIGO se usado (evidência das utilities)
            # + alias com número NOVO via attachment metadata. Assim PDF fica buscável pelos 2.
            pdf_filename = f"{query_tnum}.pdf" if used_old else f"{tnum}.pdf"
            pdf_path = os.path.join(pdf_dir, pdf_filename)
            full_path = os.path.abspath(pdf_path)

            if not force and os.path.exists(pdf_path):
                sz = os.path.getsize(pdf_path)
                if sz > 10000:
                    log.info(f"  {tnum}: PDF já existe ({round(sz/1024)}KB), pulando")
                    continue

            try:
                log.info(f"  ({idx+1}/{len(all_tickets)}) {tnum} (busca: {query_tnum})...")

                # 1. Filtrar no dashboard
                await filter_ticket(page, query_tnum)

                # 2. Clicar no ticket
                ticket_link = page.get_by_text(query_tnum, exact=True)
                if not await ticket_link.count():
                    log.warning(f"  {tnum} (busca {query_tnum}): não encontrado no dashboard")
                    errors += 1
                    continue
                await ticket_link.first.click()
                await wait_stable(page)

                # 3. Clicar na aba "Text"
                text_tab = page.locator('[role="tab"]:has-text("Text")').first
                if await text_tab.count():
                    await click_and_wait(page, text_tab, "tab")

                # 4. Esperar conteúdo carregar
                for _ in range(15):
                    body = await page.locator("body").inner_text()
                    if len(body) > 500 and ("Ticket" in body or "NOTICE" in body):
                        break
                    await page.wait_for_timeout(500)
                await page.wait_for_timeout(1000)

                # 5. JS click no FAB verde (impressora)
                clicked = await page.evaluate("""() => {
                    const sels = ['button[mat-fab]', 'button.mat-fab', '.mat-fab',
                                  'button.mdc-fab'];
                    for (const s of sels) {
                        const el = document.querySelector(s);
                        if (el) { el.click(); return true; }
                    }
                    return false;
                }""")

                if not clicked:
                    log.warning(f"  {tnum}: FAB impressora não encontrado")
                    errors += 1
                    continue

                log.info(f"  {tnum}: FAB clicado — aguardando print dialog...")
                time.sleep(5)

                # 6. Chrome já tem "Save as PDF" selecionado (lembra da última vez)
                #    Só pressiona Enter pra abrir o file dialog "Save As"
                pyautogui.press('enter')
                log.info(f"  {tnum}: Enter → abrindo Save As...")
                time.sleep(4)

                # 7. No file dialog: colar path e salvar
                import subprocess
                subprocess.run(
                    ["powershell", "-command",
                     f'Set-Clipboard -Value "{full_path}"'],
                    capture_output=True, timeout=5
                )
                time.sleep(0.5)

                pyautogui.hotkey('alt', 'n')   # Foca campo "File name"
                time.sleep(0.3)
                pyautogui.hotkey('ctrl', 'a')  # Seleciona tudo
                time.sleep(0.2)
                pyautogui.hotkey('ctrl', 'v')  # Cola path completo
                time.sleep(1)
                pyautogui.press('enter')       # Salva
                log.info(f"  {tnum}: Path: {full_path}")
                time.sleep(3)

                # Se pergunta "substituir?", confirma
                pyautogui.press('enter')
                time.sleep(2)

                # 10. Verificar se salvou
                if os.path.exists(full_path) and os.path.getsize(full_path) > 10000:
                    file_size = os.path.getsize(full_path)
                    log.info(f"  ✅ {tnum}: PDF salvo ({round(file_size/1024)}KB)")

                    attachments = t.get("attachments") or []
                    attachments = [a for a in attachments if a.get("type") != "ticket_pdf"]
                    att = {
                        "name": pdf_filename,
                        "type": "ticket_pdf",
                        "saved_at": datetime.now().isoformat(),
                        "size_kb": round(file_size / 1024, 1)
                    }
                    if used_old:
                        # Pra ticket renovado em grace, gravar ambos os números no metadata
                        # pra busca futura achar o PDF tanto pelo número antigo quanto pelo novo
                        att["old_ticket"] = query_tnum
                        att["new_ticket"] = tnum
                    attachments.append(att)
                    hist = t.get("history") or []
                    action_txt = (f"📄 PDF salvo ({round(file_size/1024)}KB)"
                                  + (f" — usado # antigo {query_tnum}" if used_old else ""))
                    hist.append({
                        "ts": int(datetime.now().timestamp() * 1000),
                        "action": action_txt,
                        "color": "#7c3aed"
                    })
                    sb_patch("tickets", tid, {"attachments": attachments, "history": hist})
                    saved += 1
                else:
                    sz = os.path.getsize(full_path) if os.path.exists(full_path) else 0
                    log.warning(f"  ⚠ {tnum}: PDF não salvo ou pequeno ({sz}B)")
                    errors += 1
                    # Fecha dialogs residuais
                    pyautogui.press('escape')
                    time.sleep(1)
                    pyautogui.press('escape')
                    time.sleep(1)

                # 11. Volta ao dashboard
                await back_to_dashboard(page, state)

            except Exception as e:
                log.error(f"  ❌ {tnum}: {e}")
                errors += 1
                try:
                    pyautogui.press('escape')
                    time.sleep(0.5)
                    pyautogui.press('escape')
                    time.sleep(0.5)
                except Exception:
                    pass
                try:
                    await back_to_dashboard(page, state)
                except Exception:
                    pass

            await page.wait_for_timeout(500)

        await ctx.close()

    log.info("=" * 55)
    log.info(f"  SAVE-PDF CONCLUÍDO: {saved} salvos, {errors} erros")
    log.info("=" * 55)


# ── │ SECTION: BACKUP │ BACKUP ────────────────────────────────────────────────
def backup_database(backup_dir=None, keep_days=30):
    """Exporta todas as tabelas do Supabase para JSON datado.
    
    Estrutura: backups/2026-04-08/tickets.json, projects.json, etc.
    Mantém os últimos 'keep_days' dias de backup.
    """
    import json, shutil
    from datetime import timedelta

    today = datetime.now().strftime("%Y-%m-%d")
    base = backup_dir or os.path.join(BASE_DIR, "backups")
    folder = os.path.join(base, today)
    os.makedirs(folder, exist_ok=True)

    tables = [
        ("tickets", "&order=id"),
        ("projects", "&order=id"),
        ("ticket_811_responses", "&order=id"),
        ("utility_contacts", "&order=id"),
        ("sync_811_log", "&order=id"),
        ("app_roles", ""),
    ]

    log.info("=" * 55)
    log.info("  BACKUP: Exportando banco de dados")
    log.info(f"  Destino: {folder}")
    log.info("=" * 55)

    total_rows = 0
    for table_name, query in tables:
        try:
            # Paginate: Supabase returns max 1000 rows per request
            all_rows = []
            offset = 0
            batch_size = 1000
            while True:
                data = sb_get(table_name, f"{query}&limit={batch_size}&offset={offset}")
                if not data:
                    break
                all_rows.extend(data)
                if len(data) < batch_size:
                    break
                offset += batch_size

            filepath = os.path.join(folder, f"{table_name}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(all_rows, f, ensure_ascii=False, indent=2, default=str)
            
            total_rows += len(all_rows)
            log.info(f"  ✅ {table_name}: {len(all_rows)} registros")
        except Exception as e:
            log.error(f"  ❌ {table_name}: {e}")

    # Gerar resumo
    summary = {
        "date": today,
        "tables": {},
        "total_rows": total_rows,
    }
    for table_name, _ in tables:
        fp = os.path.join(folder, f"{table_name}.json")
        if os.path.exists(fp):
            summary["tables"][table_name] = {
                "rows": len(json.load(open(fp, encoding="utf-8"))),
                "size_kb": round(os.path.getsize(fp) / 1024, 1),
            }
    with open(os.path.join(folder, "_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    log.info(f"  📊 Total: {total_rows} registros exportados")

    # Cleanup: manter apenas os últimos 'keep_days' dias
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    removed = 0
    if os.path.isdir(base):
        for d in sorted(os.listdir(base)):
            dpath = os.path.join(base, d)
            if os.path.isdir(dpath) and d < cutoff and d != today:
                try:
                    shutil.rmtree(dpath)
                    removed += 1
                except Exception:
                    pass
    if removed:
        log.info(f"  🗑️ {removed} backup(s) antigo(s) removido(s) (>{keep_days} dias)")

    log.info("=" * 55)
    log.info(f"  BACKUP CONCLUÍDO: {folder}")
    log.info("=" * 55)
    return folder


# ── │ SECTION: SELF_TESTS │ SELF TESTS (rodar com --selftest) ────────────────
def run_self_tests():
    """Testes internos para classify(), needs_private_locator(), append_auto_note() etc."""
    passed = 0
    failed = 0

    def _assert(label, expected, actual):
        nonlocal passed, failed
        if expected == actual:
            passed += 1
            print(f"  ✅ {label}")
        else:
            failed += 1
            print(f"  ❌ {label}: esperado={expected!r}, recebeu={actual!r}")

    print("\n=== OneDrill 811 Self-Tests ===\n")

    # ── classify() tests ──
    print("classify():")
    # CLEAR codes
    _assert("Clear: 1 Marked", "Clear", classify("Current", "1: Marked - Underground facilities have been marked")[0])
    _assert("Clear: 1B High-Profile", "Clear", classify("Current", "1B: Marked with Exceptions - High-Profile Utility")[0])
    _assert("Clear: 1C Work by Owner", "Clear", classify("Current", "1C: Work Being Done by Facility Owner")[0])
    _assert("Clear: 2 Clear", "Clear", classify("Current", "2: Clear - No underground facilities in the proposed excavation")[0])
    _assert("Clear: 2E Marked Exceptions", "Clear", classify("Current", "2E: Marked with Exceptions - Marked within confines")[0])
    _assert("Clear: 3E Already Performed", "Clear", classify("Current", "3E: Unmarked - Excavation Already Performed or Canceled")[0])
    _assert("Clear: 3U Not service", "Clear", classify("Positive Response", "3U: Unmarked - Not service provider for this location")[0])
    _assert("Clear: 3H Private", "Clear", classify("Current", "3H: Unmarked - Privately owned facilities on property")[0])
    _assert("Clear: 4 Clear No Facilities", "Clear", classify("Current", "4: Clear No Facilities")[0])
    _assert("Clear: 4 Private Line", "Clear", classify("Current", "4: Private Line - not responsibility of Indiana 811")[0])
    _assert("Clear: 5 No Conflict", "Clear", classify("Current", "5: No Conflict - utility is outside of the requested work site")[0])
    _assert("Clear: 5A Documents", "Clear", classify("Current", "5A: Design Notice - Documents Provided")[0])
    _assert("Clear: 5B Design Marked", "Clear", classify("Current", "5B: Design Notice - Marked")[0])
    _assert("Clear: 6C Joint Meet Complete", "Clear", classify("Current", "6C: Joint Meet Complete")[0])
    _assert("Clear: status=Clear", "Clear", classify("Clear", "")[0])
    _assert("Clear: no facilit", "Clear", classify("Current", "No facilities in area")[0])
    # PENDING codes
    _assert("Pending: 1A Do Not Excavate", "Pending", classify("Current", "1A: Marked with Exceptions - Do Not Excavate, High-Profile Utility")[0])
    _assert("Pending: 3A No Access", "Pending", classify("Current", "3A: Unmarked - Could Not Gain Access to Property")[0])
    _assert("Pending: 3B Incorrect Address", "Pending", classify("Current", "3B: Unmarked - Incorrect Address Information")[0])
    _assert("Pending: 3C Marking Delay", "Pending", classify("Current", "3C: Unmarked - Marking Delay - Do not excavate until resolved")[0])
    _assert("Pending: 3D Instructions Unclear", "Pending", classify("Current", "3D: Unmarked - Marking Instructions are Unclear")[0])
    _assert("Pending: 3F Untonable", "Pending", classify("Current", "3F: Unmarked - Line is untonable")[0])
    _assert("Pending: 3G Ongoing", "Pending", classify("Current", "3G: Unmarked - Ongoing - partially marked")[0])
    _assert("Pending: 6A Joint Meet Conflict", "Pending", classify("Current", "6A: Joint Meet Conflict")[0])
    _assert("Pending: 6A Active Facilities FL", "Pending", classify("Current", "6A: Active Facilities are present - DO NOT demolish until the member notifies you the site is clear")[0])
    _assert("Pending: 6B Joint Meet Accepted", "Pending", classify("Current", "6B: Joint Meet Accepted")[0])
    _assert("Pending: 8 Ongoing Job", "Pending", classify("Current", "8: Ongoing Job - locate technician and excavator have established")[0])
    _assert("Pending: No Response", "Pending", classify("No Response", "")[0])
    _assert("Pending: unmarked generic", "Pending", classify("Current", "unmarked")[0])
    _assert("Pending: positive ambiguous", "Pending", classify("Positive Response", "")[0])
    # UNRECOGNIZED flag tests
    _assert("Recognized: 1 Marked → False", False, classify("Current", "1: Marked - Underground facilities have been marked")[1])
    _assert("Recognized: No Response → False", False, classify("No Response", "")[1])
    _assert("Unrecognized: empty Positive Response → True", True, classify("Positive Response", "")[1])
    _assert("Unrecognized: unknown text → True", True, classify("SomeWeirdStatus", "Facilities identified - excavator responsibility")[1])
    _assert("Recognized: 3A No Access → False", False, classify("Current", "3A: Unmarked - Could Not Gain Access to Property")[1])
    # IL/JULIE scheduling entries (recognized, not unrecognized)
    _assert("IL schedule: DECLINED CODE 50", "Pending", classify("950", "DECLINED CODE 50")[0])
    _assert("IL schedule: DECLINED CODE 50 recognized", False, classify("950", "DECLINED CODE 50")[1])
    _assert("IL schedule: ACCEPTED CODE 50", "Pending", classify("850", "ACCEPTED CODE 50")[0])
    _assert("IL schedule: ACCEPTED CODE 50 recognized", False, classify("850", "ACCEPTED CODE 50")[1])
    _assert("IL schedule: ALTERNATE SCHEDULE", "Pending", classify("50", "LOCATOR AND EXCAVATOR AGREED TO A DOCUMENTED ALTERNATE MARKING SCHEDULE")[0])
    _assert("IL schedule: ALTERNATE SCHEDULE recognized", False, classify("50", "LOCATOR AND EXCAVATOR AGREED TO A DOCUMENTED ALTERNATE MARKING SCHEDULE")[1])

    # ── needs_private_locator() tests ──
    print("\nneeds_private_locator():")
    _assert("3H detected", True, needs_private_locator("3H - Privately owned facility"))
    _assert("privately owned", True, needs_private_locator("This is a privately owned line"))
    _assert("private facility owner", True, needs_private_locator("Contact private facility owner"))
    _assert("normal response", False, needs_private_locator("1 - Marked"))
    _assert("empty string", False, needs_private_locator(""))

    # ── needs_watch_and_protect() tests ──
    print("\nneeds_watch_and_protect():")
    _assert("W&P detected", True, needs_watch_and_protect("WATCH AND PROTECT"))
    _assert("W&P in sentence", True, needs_watch_and_protect("60 - Watch and Protect required"))
    _assert("normal response", False, needs_watch_and_protect("1: Marked"))
    _assert("empty string", False, needs_watch_and_protect(""))
    # classify: code 60 W&P → Clear, recognized
    _assert("Clear: W&P code 60", "Clear", classify("60", "WATCH AND PROTECT")[0])
    _assert("Clear: W&P recognized", False, classify("60", "WATCH AND PROTECT")[1])

    # ── is_ticket_canceled() tests ──
    print("\nis_ticket_canceled():")
    _assert("CANCEL header", True, is_ticket_canceled("CANCEL\nSome body text"))
    _assert("FUNCTION: CANCEL", True, is_ticket_canceled("Header\nFunction: CANCEL\nMore text"))
    _assert("replaced by", True, is_ticket_canceled("CANCELED ticket\nREPLACED BY TICKET NUMBER 12345678"))
    _assert("normal ticket", False, is_ticket_canceled("Normal ticket body\nLocation: Main St"))
    _assert("empty", False, is_ticket_canceled(""))

    # ── is_in_renewal_grace() tests ──
    from datetime import date
    print("\nis_in_renewal_grace():")
    today = date(2026, 5, 21)
    _assert("sem old_ticket2", (False, ""), is_in_renewal_grace({}, ref_date=today))
    _assert("com old mas sem expire", (False, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": ""}, ref_date=today))
    _assert("expire_old = traço", (False, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "—"}, ref_date=today))
    _assert("em grace (futuro)", (True, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "06/15/2026"}, ref_date=today))
    _assert("em grace (hoje)", (True, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "05/21/2026"}, ref_date=today))
    _assert("fora de grace (ontem)", (False, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "05/20/2026"}, ref_date=today))
    _assert("fora de grace (passado)", (False, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "01/01/2026"}, ref_date=today))
    _assert("expire poluído Time:", (True, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "06/15/26 Time: 23:59"}, ref_date=today))
    _assert("chain com seta", (True, "11111111"), is_in_renewal_grace({"old_ticket2": "11111111 → 22222222", "expire_old": "06/15/2026"}, ref_date=today))
    _assert("expire lixo", (False, "12345678"), is_in_renewal_grace({"old_ticket2": "12345678", "expire_old": "nao e data"}, ref_date=today))

    # ── _pdf_query_number() tests ──
    print("\n_pdf_query_number():")
    _assert("sem renovação", ("NEW123", False), _pdf_query_number({"ticket": "NEW123"}))
    _assert("renovado em grace", ("OLD123", True), _pdf_query_number({"ticket": "NEW123", "old_ticket2": "OLD123", "expire_old": "12/31/2030"}))
    _assert("renovado fora de grace", ("NEW123", False), _pdf_query_number({"ticket": "NEW123", "old_ticket2": "OLD123", "expire_old": "01/01/2020"}))
    _assert("renovado sem expire", ("NEW123", False), _pdf_query_number({"ticket": "NEW123", "old_ticket2": "OLD123", "expire_old": ""}))

    # ── append_auto_note() tests ──
    print("\nappend_auto_note():")
    _assert("adds to empty", "[AUTO 811] Clear", append_auto_note("", "[AUTO 811] Clear"))
    _assert("dedup exact", "existing\n[AUTO 811] Clear", append_auto_note("existing\n[AUTO 811] Clear", "[AUTO 811] Clear"))
    _assert("preserves manual", True, "manual note" in append_auto_note("manual note", "[AUTO 811] New"))
    _assert("truncates excess", True, len(append_auto_note(
        "\n".join(f"[AUTO 811] Note {i}" for i in range(15)),
        "[AUTO 811] Note 15"
    ).split("\n")) <= MAX_AUTO_NOTES + 1)

    # ── _get_latest_response_date() tests ──
    print("\n_get_latest_response_date():")
    _assert("ISO dates: picks latest", True,
        _get_latest_response_date([
            {"utility": "A", "responded_date": "2026-03-23T10:11:00"},
            {"utility": "B", "responded_date": "2026-03-25T09:30:00"},
            {"utility": "C", "responded_date": "2026-03-24T08:50:00"},
        ])[0].strftime('%m/%d/%Y') == "03/25/2026")
    _assert("ISO dates: not fallback", False,
        _get_latest_response_date([
            {"utility": "A", "responded_date": "2026-03-25T09:30:00"},
        ])[1])
    _assert("mixed formats", True,
        _get_latest_response_date([
            {"utility": "A", "responded_date": "03/28/2026"},
            {"utility": "B", "responded_date": "2026-03-25T09:30:00"},
        ])[0].strftime('%m/%d/%Y') == "03/28/2026")
    _assert("no dates: returns now (fallback)", True,
        _get_latest_response_date([
            {"utility": "A", "status": "Clear"},
        ])[0].date() == datetime.now().date())
    _assert("no dates: is_fallback=True", True,
        _get_latest_response_date([
            {"utility": "A", "status": "Clear"},
        ])[1])
    _assert("empty list: returns now", True,
        _get_latest_response_date([])[0].date() == datetime.now().date())
    _assert("empty list: is_fallback=True", True,
        _get_latest_response_date([])[1])

    # ── extract_expire_date() tests ──
    print("\nextract_expire_date():")
    _assert("mm/dd/yyyy", "04/15/2026", extract_expire_date("Expire on: 04/15/2026\nNext line"))
    # Due Date ISOLADO deve ser IGNORADO (era bug antigo) — retorna vazio
    _assert("due date isolado: IGNORADO", "", extract_expire_date("Due Date: 03/20/2026\nBody"))
    _assert("no date", "", extract_expire_date("No expiration info here"))
    # Formato Sunshine FL (Exp Date + Due Date na mesma linha)
    sunshine_body = "Due Date : 04/15/26 Time: 23:59ET  Exp Date : 05/13/26 Time: 23:59ET"
    _assert("sunshine FL: Exp Date", "05/13/2026", extract_expire_date(sunshine_body))
    # Ticket Expires com hora AM/PM
    _assert("Indiana: Ticket Expires PM", "05/13/2026",
            extract_expire_date("Ticket Expires: 05/13/2026 11:59 PM"))
    # Formato com "at"
    _assert("expires at", "05/13/2026", extract_expire_date("Expires: 05/13/2026 at 11:59 PM"))

    # ── normalize_expire() tests ──
    print("\nnormalize_expire():")
    _assert("formato poluído legado",     "04/15/2026", normalize_expire("04/15/26 Time: 23:59"))
    _assert("já normalizado",             "05/13/2026", normalize_expire("05/13/2026"))
    _assert("2 dígitos de ano",           "05/13/2026", normalize_expire("05/13/26"))
    _assert("com sufixo ET",              "04/15/2026", normalize_expire("04/15/26 Time: 23:59ET"))
    _assert("com AM/PM",                  "05/13/2026", normalize_expire("05/13/2026 11:59 PM"))
    _assert("vazio",                      "",           normalize_expire(""))
    _assert("traço",                      "",           normalize_expire("—"))
    _assert("None",                       "",           normalize_expire(None))
    # Fix bug #24: casos de paridade JS ↔ Python.
    # Se alguém mudar Python sem mudar JS (ou vice-versa), aqui disparamos.
    # Os mesmos 23 casos estão em testNormalizeExpireParity() no app.js.
    _assert("parity: 5/13/2026",          "05/13/2026", normalize_expire("5/13/2026"))
    _assert("parity: 5/13/26",            "05/13/2026", normalize_expire("5/13/26"))
    _assert("parity: ISO 2026-05-13",     "05/13/2026", normalize_expire("2026-05-13"))
    _assert("parity: ISO 2026-5-13",      "05/13/2026", normalize_expire("2026-5-13"))
    _assert("parity: May 13, 2026",       "05/13/2026", normalize_expire("May 13, 2026"))
    _assert("parity: Jan 5, 2026",        "01/05/2026", normalize_expire("Jan 5, 2026"))
    _assert("parity: December 31, 2025",  "12/31/2025", normalize_expire("December 31, 2025"))
    _assert("parity: Feb 29, 2024",       "02/29/2024", normalize_expire("Feb 29, 2024"))
    _assert("parity: 05/13/2026 23:59",   "05/13/2026", normalize_expire("05/13/2026 23:59"))
    _assert("parity: 05/13/26 23:59ET",   "05/13/2026", normalize_expire("05/13/26 23:59ET"))
    _assert("parity: at 23:59",           "05/13/2026", normalize_expire("05/13/2026 at 23:59"))
    _assert("parity: N/A → vazio",        "",           normalize_expire("N/A"))
    _assert("parity: None str → vazio",   "",           normalize_expire("None"))
    _assert("parity: null str → vazio",   "",           normalize_expire("null"))
    _assert("parity: -  → vazio",         "",           normalize_expire("-"))
    _assert("parity: lixo → vazio",       "",           normalize_expire("lixo que não é data"))
    _assert("parity: pt-BR ambíguo",      "",           normalize_expire("13/05/2026"))

    # ── _is_polluted_expire() tests ──
    print("\n_is_polluted_expire():")
    _assert("formato legado 'Time:'", True,  _is_polluted_expire("04/15/26 Time: 23:59"))
    _assert("formato limpo",          False, _is_polluted_expire("05/13/2026"))
    _assert("ano 2 dígitos",          True,  _is_polluted_expire("05/13/26"))
    _assert("vazio",                  False, _is_polluted_expire(""))
    _assert("com hora bruta",         True,  _is_polluted_expire("05/13/2026 11:59"))
    _assert("com ET",                 True,  _is_polluted_expire("05/13/2026 11:59ET"))

    # ── _is_valid_utility_name() tests ──
    print("\n_is_valid_utility_name():")
    # Válidos (utilities reais)
    _assert("DUKE ENERGY",              True,  _is_valid_utility_name("DUKE ENERGY"))
    _assert("COMCAST NORTH",            True,  _is_valid_utility_name("COMCAST NORTH"))
    _assert("IN AMERICAN WATER",        True,  _is_valid_utility_name("IN AMERICAN WATER"))
    _assert("NIPSCO GAS & ELECTRIC",    True,  _is_valid_utility_name("NIPSCO GAS & ELECTRIC (VALPARAISO)"))
    _assert("FRONTIER",                 True,  _is_valid_utility_name("FRONTIER"))
    _assert("AT&T",                     True,  _is_valid_utility_name("AT&T"))
    # Inválidos (lixo de UI — era o bug)
    _assert("All (6)",                  False, _is_valid_utility_name("All (6)"))
    _assert("All (7)",                  False, _is_valid_utility_name("All (7)"))
    _assert("All (9)",                  False, _is_valid_utility_name("All (9)"))
    _assert("Current (3)",              False, _is_valid_utility_name("Current (3)"))
    _assert("Show all (9)",             False, _is_valid_utility_name("Show all (9)"))
    _assert("All",                      False, _is_valid_utility_name("All"))
    _assert("Event",                    False, _is_valid_utility_name("Event"))
    _assert("Positive Response",        False, _is_valid_utility_name("Positive Response"))
    _assert("No Response",              False, _is_valid_utility_name("No Response"))
    _assert("Status",                   False, _is_valid_utility_name("Status"))
    _assert("Service Area",             False, _is_valid_utility_name("Service Area"))
    # Inválidos (códigos/IDs)
    _assert("ID2227",                   False, _is_valid_utility_name("ID2227"))
    _assert("NI0005",                   False, _is_valid_utility_name("NI0005"))
    _assert("COMCN",                    False, _is_valid_utility_name("COMCN"))
    # Inválidos (vazio/curto)
    _assert("vazio",                    False, _is_valid_utility_name(""))
    _assert("None",                     False, _is_valid_utility_name(None))
    _assert("muito curto",              False, _is_valid_utility_name("AB"))

    print(f"\n{'='*40}")
    print(f"Resultado: {passed} passou, {failed} falhou")
    if failed == 0:
        print("✅ Todos os testes passaram!")
    else:
        print("❌ Alguns testes falharam.")
        sys.exit(1)
    print()


# ── │ SECTION: FIX_RENEWALS │ FIX RENEWALS: corrigir expire_old de tickets renovados 
async def fix_renewals():
    """Corrige expire_old de tickets renovados buscando a data real no portal.

    Bug: no merge de renovação, o código antigo sobrescrevia t.expire com o
    expire do ticket novo ANTES de capturar oldExpire, fazendo expire_old
    ficar igual ao expire (data do ticket novo, não do antigo).

    Este comando:
    1. Busca todos os tickets com old_ticket2 (renovados)
    2. Para cada um, extrai o número do ticket antigo
    3. Scrapa o ticket antigo no portal 811 para obter a data de expiração real
    4. Se expire_old estiver errado, corrige
    """
    all_renewed = sb_get("tickets", "&old_ticket2=not.is.null&select=id,ticket,state,old_ticket2,expire,expire_old")
    if not all_renewed:
        log.info("[FixRenewals] Nenhum ticket renovado encontrado")
        return

    log.info(f"[FixRenewals] {len(all_renewed)} tickets renovados encontrados")

    # Agrupa por estado
    by_state = {}
    for t in all_renewed:
        state = t.get("state", "")
        if state not in by_state:
            by_state[state] = []
        # Extrai número do ticket antigo (primeiro da cadeia)
        old_chain = (t.get("old_ticket2") or "").strip()
        old_num = old_chain.split(" → ")[0].strip() if old_chain else ""
        if not old_num:
            continue
        by_state[state].append({
            "id": t["id"],
            "ticket": t["ticket"],
            "old_num": old_num,
            "expire": t.get("expire", ""),
            "expire_old": t.get("expire_old", ""),
        })

    fixed = 0
    skipped = 0
    errors = 0

    for state, items in by_state.items():
        if state not in PORTALS:
            log.warning(f"[FixRenewals] Estado {state} sem portal configurado — pulando {len(items)} tickets")
            skipped += len(items)
            continue

        log.info(f"[FixRenewals] [{state}] Verificando {len(items)} tickets renovados...")
        perfil = _profile_path(state)

        async with async_playwright() as p:
            ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)

            await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
            await wait_stable(page)

            if "login" in page.url.lower():
                await ctx.close()
                ok = await auto_login_silent(state)

                if not ok:

                    log.warning(f"[{state}] auto_login_silent falhou, tentando manual...")

                    ok = await auto_login(state)
                if not ok:
                    log.error(f"[FixRenewals] [{state}] Login falhou — pulando")
                    errors += len(items)
                    continue
                ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
                page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                page.set_default_timeout(TIMEOUT_PAGE)

            ok = await goto_dashboard(page, state)
            if not ok:
                log.error(f"[FixRenewals] [{state}] Dashboard inacessível — pulando")
                await ctx.close()
                errors += len(items)
                continue

            for idx, item in enumerate(items):
                old_num = item["old_num"]
                ticket = item["ticket"]
                tid = item["id"]
                current_expire_old = item["expire_old"]

                log.info(f"[FixRenewals] [{state}] ({idx+1}/{len(items)}) {ticket} → antigo {old_num} (expire_old atual: {current_expire_old})")

                try:
                    await filter_ticket(page, old_num)

                    if not await page.get_by_text(old_num, exact=True).count():
                        log.warning(f"[FixRenewals] {old_num}: não encontrado no portal")
                        skipped += 1
                        continue

                    await page.get_by_text(old_num, exact=True).first.click()
                    await wait_stable(page)

                    # Vai na aba Text pra pegar o body completo
                    tt = page.locator('[role="tab"]:has-text("Text")').first
                    if await tt.count():
                        await click_and_wait(page, tt, "tab")

                    body = await page.locator("body").inner_text()
                    real_expire = normalize_expire(extract_expire_date(body))

                    if not real_expire:
                        log.warning(f"[FixRenewals] {old_num}: expire não encontrado no body")
                        skipped += 1
                        await back_to_dashboard(page, state)
                        continue

                    # Compara (normaliza também o que está no banco pra comparação justa)
                    current_normalized = normalize_expire(current_expire_old)
                    if real_expire == current_normalized:
                        log.info(f"[FixRenewals] {ticket}: expire_old OK ({real_expire})")
                        skipped += 1
                    else:
                        log.warning(f"[FixRenewals] {ticket}: CORRIGINDO expire_old {current_expire_old} → {real_expire}")
                        sb_patch("tickets", tid, {"expire_old": real_expire})
                        fixed += 1

                    await back_to_dashboard(page, state)

                except Exception as e:
                    log.error(f"[FixRenewals] {old_num}: ERRO → {e}")
                    errors += 1
                    try:
                        await back_to_dashboard(page, state)
                    except Exception:
                        pass

            await ctx.close()

    log.info(f"[FixRenewals] === CONCLUÍDO: {fixed} corrigidos, {skipped} OK/pulados, {errors} erros ===")


# ── │ SECTION: FIX_EXPIRES │ FIX EXPIRES: corrigir data de vencimento de tickets com formato antigo 
async def fix_expires(target_ticket=None):
    """Re-scrapa a data de vencimento (expire) de tickets ativos no portal.

    Usado pra corrigir tickets que:
      - Foram importados por versão antiga do parser (formato "MM/DD/YY Time: HH:MM")
      - Tiveram expire capturada errada (pegou Due Date ou Work Date por engano)
      - Têm expire vazia

    Se `target_ticket` for informado, corrige apenas esse ticket.
    """
    log.info("=" * 55)
    log.info(f"  FIX-EXPIRES: Corrigindo data de vencimento")
    if target_ticket:
        log.info(f"  Ticket alvo: {target_ticket}")
    log.info("=" * 55)

    qs = "&status=in.(Open,Damage,Clear)&select=id,ticket,state,expire"
    if target_ticket:
        qs += f"&ticket=eq.{target_ticket}"
    all_active = sb_get("tickets", qs)

    if not all_active:
        log.info("[FixExpires] Nenhum ticket ativo encontrado")
        return

    # Agrupa por estado
    by_state = {}
    for t in all_active:
        state = t.get("state", "")
        if state not in PORTALS:
            continue  # IL (JULIE) não tem expire no body
        by_state.setdefault(state, []).append(t)

    if not by_state:
        log.warning("[FixExpires] Nenhum ticket em estado com portal (FL/IN)")
        return

    fixed = 0
    unchanged = 0
    errors = 0

    for state, items in by_state.items():
        log.info(f"[FixExpires] [{state}] {len(items)} tickets a verificar")
        perfil = _profile_path(state)

        async with async_playwright() as p:
            ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
            page = ctx.pages[0] if ctx.pages else await ctx.new_page()
            page.set_default_timeout(TIMEOUT_PAGE)

            await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
            await wait_stable(page)
            page, ctx = await ensure_login(page, ctx, p, state)
            if not page:
                log.error(f"[FixExpires] [{state}] Login falhou — pulando estado")
                errors += len(items)
                continue

            ok = await goto_dashboard(page, state)
            if not ok:
                log.error(f"[FixExpires] [{state}] Dashboard inacessível — pulando")
                await ctx.close()
                errors += len(items)
                continue

            for idx, t in enumerate(items):
                tnum = t["ticket"]
                tid = t["id"]
                current = (t.get("expire") or "").strip()

                try:
                    await filter_ticket(page, tnum)
                    if not await page.get_by_text(tnum, exact=True).count():
                        log.warning(f"[FixExpires] {tnum}: não encontrado no portal")
                        errors += 1
                        continue

                    await page.get_by_text(tnum, exact=True).first.click()
                    await wait_stable(page)

                    tt = page.locator('[role="tab"]:has-text("Text")').first
                    if await tt.count():
                        await click_and_wait(page, tt, "tab")

                    body = await page.locator("body").inner_text()
                    new_expire = normalize_expire(extract_expire_date(body, ticket_num=tnum, debug=True))
                    current_normalized = normalize_expire(current)

                    if not new_expire and current_normalized and _is_polluted_expire(current):
                        # Parser não achou no portal, mas dá pra limpar o valor do banco
                        log.warning(f"[FixExpires] {tnum}: scraper falhou, mas NORMALIZANDO {current} → {current_normalized}")
                        sb_patch("tickets", tid, {"expire": current_normalized})
                        fixed += 1
                    elif not new_expire:
                        log.warning(f"[FixExpires] {tnum}: novo expire vazio — mantendo atual ({current})")
                        errors += 1
                    elif new_expire == current:
                        log.info(f"[FixExpires] {tnum}: OK ({current})")
                        unchanged += 1
                    else:
                        log.warning(f"[FixExpires] {tnum}: CORRIGINDO {current} → {new_expire}")
                        sb_patch("tickets", tid, {"expire": new_expire})
                        fixed += 1

                    await back_to_dashboard(page, state)
                except Exception as e:
                    log.error(f"[FixExpires] {tnum}: ERRO → {e}")
                    errors += 1
                    try:
                        await back_to_dashboard(page, state)
                    except Exception:
                        pass

            await ctx.close()

    log.info(f"[FixExpires] === CONCLUÍDO: {fixed} corrigidos, {unchanged} OK, {errors} erros ===")


# ── │ SECTION: POPULATE_COUNTIES │ POPULATE COUNTIES: backfill de county em tickets antigos ──
async def populate_counties(target_state=None, force=False):
    """Popula o campo `county` em tickets que ainda não têm.

    Usa resolve_county() com estratégia 3-em-cascata (base estática → variantes → Nominatim).
    Por default processa só tickets com county IS NULL/vazio. Use force=True pra re-resolver todos.

    Args:
        target_state: se informado ('FL'/'IN'/'IL'), só processa aquele estado. None = todos.
        force: se True, re-processa tickets que já têm county (útil se base foi atualizada).
    """
    log.info("=" * 55)
    log.info(f"  POPULATE-COUNTIES: backfill de county")
    log.info(f"  Estado: {target_state or 'TODOS'} | Force: {force}")
    log.info("=" * 55)

    # Query com filtro
    qs = "&select=id,ticket,state,location,geocoded_lat,geocoded_lon,county"
    if target_state:
        qs += f"&state=eq.{_qv(target_state.upper())}"
    # Sem force, só pega os que precisam (county vazio ou null)
    if not force:
        qs += "&or=(county.is.null,county.eq.)"

    tickets_list = sb_get("tickets", qs)
    if not tickets_list:
        log.info("[PopCounties] Nenhum ticket pra processar.")
        return

    log.info(f"[PopCounties] {len(tickets_list)} tickets a processar")

    resolved = 0
    unchanged = 0
    unresolved = 0
    by_source = {"static": 0, "nominatim": 0, "none": 0}

    for i, t in enumerate(tickets_list, 1):
        tid = t["id"]
        tnum = t.get("ticket", "?")
        state = t.get("state", "")
        location = t.get("location", "")
        lat = t.get("geocoded_lat")
        lon = t.get("geocoded_lon")
        current = (t.get("county") or "").strip()

        # Pra contabilizar qual estratégia resolveu, fazemos lookup estático primeiro
        # direto (sem o fallback Nominatim) pra saber se veio da base.
        db = _load_counties_db()
        static_hit = ""
        city_raw = (location or "").split(",")[0].strip()
        if city_raw and state:
            for variant in _city_variants(city_raw):
                if variant in db.get(state.upper(), {}):
                    static_hit = db[state.upper()][variant]
                    break

        if static_hit:
            new_county = static_hit
            by_source["static"] += 1
        else:
            # Fallback Nominatim (só se tem lat/lon)
            new_county = await resolve_county(location, state, lat, lon)
            if new_county:
                by_source["nominatim"] += 1
            else:
                by_source["none"] += 1

        if new_county and new_county != current:
            try:
                sb_patch("tickets", tid, {"county": new_county})
                resolved += 1
                log.info(f"[PopCounties] [{i}/{len(tickets_list)}] {tnum} ({state}): {location!r} → {new_county}")
            except Exception as e:
                log.error(f"[PopCounties] Erro salvando {tnum}: {e}")
        elif new_county == current and current:
            unchanged += 1
        else:
            unresolved += 1
            log.debug(f"[PopCounties] [{i}/{len(tickets_list)}] {tnum} ({state}): {location!r} → não resolvido")

    log.info("=" * 55)
    log.info(f"[PopCounties] CONCLUÍDO")
    log.info(f"  Resolvidos:   {resolved}")
    log.info(f"  Já corretos:  {unchanged}")
    log.info(f"  Sem match:    {unresolved}")
    log.info(f"  Origem — base estática: {by_source['static']} | Nominatim: {by_source['nominatim']} | none: {by_source['none']}")
    log.info("=" * 55)


# ── │ SECTION: REBUILD_COVERAGE │ REBUILD COVERAGE: auto-derive utility × county ──
# Fase 2 do filtro por county: analisa ticket_811_responses × tickets.county
# e constroi a tabela utility_county_coverage (qual utility atende qual county).

# Threshold mínimo de respostas pra uma utility ser considerada "atende" um county.
# Evita falsos positivos de utility que respondeu 1-2x por engano em county adjacente.
COVERAGE_MIN_RESPONSES = 5

# Status que NÃO contam pra inferência (são eventos administrativos, não cobertura real):
#   "Private Locator" = sinalização de que precisa locator privado (código 3H)
#   "Watch and Protect" = representante obrigatório (código 60 IL)
#   "Unrecognized" = parser não entendeu a resposta (não usar pra inferir)
COVERAGE_EXCLUDED_STATUSES = {"Private Locator", "Watch and Protect", "Unrecognized"}


def rebuild_utility_county_coverage():
    """Reconstroi a tabela utility_county_coverage a partir das respostas históricas.

    Estratégia (pull-based — pega tudo do banco e agrega localmente pra evitar queries N+1):
      1) Busca TODOS os tickets com county preenchido (select: ticket, county, state)
      2) Busca TODAS as respostas válidas (select: ticket_num, utility_name, status)
      3) Cruza em memória: pra cada (utility, county, state) conta quantas respostas
      4) Filtra pelos que atingiram threshold
      5) Upsert em utility_county_coverage (sobrescreve, não acumula entre execuções)

    Safety: antes de substituir, deleta só as linhas cobertas pela nova análise —
    evita apagar dados se a query retornar vazio por bug.
    """
    log.info("=" * 55)
    log.info(f"  REBUILD-COVERAGE: auto-derive utility × county")
    log.info(f"  Threshold: ≥{COVERAGE_MIN_RESPONSES} respostas | Excluídos: {COVERAGE_EXCLUDED_STATUSES}")
    log.info("=" * 55)

    # 1. Tickets com county válido
    tickets_with_county = sb_get("tickets", "&select=ticket,county,state&county=not.is.null&county=neq.")
    if not tickets_with_county:
        log.warning("[Coverage] Nenhum ticket com county preenchido — rodar --populate-counties primeiro")
        return

    # Map: ticket_num → (county, state)
    ticket_map = {}
    for t in tickets_with_county:
        tn = (t.get("ticket") or "").strip()
        c = (t.get("county") or "").strip()
        s = (t.get("state") or "").strip().upper()
        if tn and c and s:
            ticket_map[tn] = (c, s)
    log.info(f"[Coverage] {len(ticket_map)} tickets com county válido carregados")

    # 2. Respostas em lotes (pagina pra evitar timeout em bases grandes)
    all_responses = []
    offset = 0
    page_size = 1000
    while True:
        page = sb_get(
            "ticket_811_responses",
            f"&select=ticket_num,utility_name,status&order=id&limit={page_size}&offset={offset}"
        )
        if not page:
            break
        all_responses.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    log.info(f"[Coverage] {len(all_responses)} respostas totais carregadas")

    # 3. Agregação em memória
    # Chave: (utility_name, county, state) → count
    counts = {}
    skipped_invalid_utility = 0
    skipped_excluded_status = 0
    skipped_no_county = 0

    for r in all_responses:
        utility = (r.get("utility_name") or "").strip()
        status = (r.get("status") or "").strip()
        ticket_num = (r.get("ticket_num") or "").strip()

        # Filtra utility lixo (UI, códigos puros, etc)
        if not _is_valid_utility_name(utility):
            skipped_invalid_utility += 1
            continue

        # Filtra status administrativos
        if status in COVERAGE_EXCLUDED_STATUSES:
            skipped_excluded_status += 1
            continue

        # Liga ticket → county/state
        if ticket_num not in ticket_map:
            skipped_no_county += 1
            continue
        county, state = ticket_map[ticket_num]

        key = (utility, county, state)
        counts[key] = counts.get(key, 0) + 1

    log.info(f"[Coverage] Agregação concluída:")
    log.info(f"  Combinações (utility × county × state): {len(counts)}")
    log.info(f"  Respostas skipadas — utility inválida:  {skipped_invalid_utility}")
    log.info(f"  Respostas skipadas — status excluído:   {skipped_excluded_status}")
    log.info(f"  Respostas skipadas — ticket sem county: {skipped_no_county}")

    # 4. Filtra pelo threshold
    qualified = [(u, c, s, cnt) for (u, c, s), cnt in counts.items() if cnt >= COVERAGE_MIN_RESPONSES]
    log.info(f"[Coverage] {len(qualified)} combinações atingiram threshold de {COVERAGE_MIN_RESPONSES}+ respostas")

    if not qualified:
        log.warning("[Coverage] Nenhuma combinação atingiu o threshold. Nada a gravar.")
        return

    # 5. Upsert em batches
    # Nota: como a unique key é (utility_name, county, state), upsert com on_conflict
    # sobrescreve o response_count e last_seen — comportamento desejado.
    records = []
    from datetime import datetime as _dt, timezone as _tz
    now_iso = _dt.now(_tz.utc).isoformat().replace("+00:00", "Z")
    for u, c, s, cnt in qualified:
        records.append({
            "utility_name": u,
            "county": c,
            "state": s,
            "response_count": cnt,
            "last_seen": now_iso,
        })

    # Upsert em lotes de 100
    batch_size = 100
    inserted = 0
    errors = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            # Bug fix: _sb_request espera função (requests.post), não string "POST".
            # on_conflict via query string (padrão do resto do código em sb_upsert).
            url = f"{SB_URL}/rest/v1/utility_county_coverage?on_conflict=utility_name,county,state"
            headers = {**SB_H, "Prefer": "resolution=merge-duplicates,return=minimal"}
            resp = _sb_request(requests.post, url, headers=headers, json=batch, timeout=30)
            if resp.status_code in (200, 201, 204):
                inserted += len(batch)
            else:
                log.error(f"[Coverage] Erro batch {i}-{i+len(batch)}: HTTP {resp.status_code} — {resp.text[:200]}")
                errors += len(batch)
        except Exception as e:
            log.error(f"[Coverage] Exceção batch {i}: {e}")
            errors += len(batch)

    # Top 10 coberturas pra amostra no log
    qualified_sorted = sorted(qualified, key=lambda x: -x[3])
    log.info("[Coverage] Top 10 coberturas (por nº de respostas):")
    for u, c, s, cnt in qualified_sorted[:10]:
        log.info(f"  {cnt:>4}x  {u} → {c} County, {s}")

    log.info("=" * 55)
    log.info(f"[Coverage] CONCLUÍDO: {inserted} gravados, {errors} erros")
    log.info("=" * 55)


# ── │ SECTION: AUDIT │ CLEAN GHOST UTILITIES: limpa respostas fantasmas do banco 
def _audit_collect():
    """Coleta anomalias do banco sem apresentar.

    Retorna dict com 6 listas (A..F), contagens e contexto.
    Reutilizável pra print e pra email.
    """
    tickets = sb_get("tickets", "&status=in.(Open,Damage,Clear)&select=id,ticket,state,status,expire,expire_old,old_ticket2,status_old")

    all_utils = []
    offset = 0
    while True:
        page = sb_get("ticket_811_responses", f"&select=id,ticket_num,utility_name,status&limit=1000&offset={offset}")
        if not page:
            break
        all_utils.extend(page)
        if len(page) < 1000:
            break
        offset += 1000

    pending_by_ticket = {}
    for u in all_utils:
        if u.get("status") == "Pending":
            tn = str(u.get("ticket_num", "")).strip()
            if not tn:
                continue
            pending_by_ticket.setdefault(tn, []).append(u.get("utility_name", ""))

    a_expire_poluted = []
    b_expire_old_poluted = []
    c_expire_empty = []
    d_false_clear = []
    e_ghost_utils = []
    f_renewed_no_expire_old = []

    for t in tickets:
        tnum = t.get("ticket", "")
        exp = (t.get("expire") or "").strip()
        exp_old = (t.get("expire_old") or "").strip()
        is_renewed_t = bool(t.get("old_ticket2"))
        status = t.get("status", "")

        if exp and _is_polluted_expire(exp):
            a_expire_poluted.append({"ticket": tnum, "state": t.get("state"), "expire": exp, "normalized": normalize_expire(exp)})
        if is_renewed_t and exp_old and _is_polluted_expire(exp_old):
            b_expire_old_poluted.append({"ticket": tnum, "state": t.get("state"), "expire_old": exp_old, "normalized": normalize_expire(exp_old)})
        if not exp and status in ("Open", "Damage"):
            c_expire_empty.append({"ticket": tnum, "state": t.get("state"), "status": status})
        if status == "Clear":
            pend = pending_by_ticket.get(str(tnum).strip(), [])
            if pend:
                d_false_clear.append({"ticket": tnum, "state": t.get("state"), "pending_utils": pend})
        if is_renewed_t and not exp_old:
            f_renewed_no_expire_old.append({"ticket": tnum, "state": t.get("state"), "old_ticket2": t.get("old_ticket2")})

    for u in all_utils:
        name = u.get("utility_name", "")
        if not _is_valid_utility_name(name):
            e_ghost_utils.append({
                "id": u.get("id"),
                "ticket_num": u.get("ticket_num"),
                "utility_name": name,
                "status": u.get("status"),
            })

    total_affected = len(a_expire_poluted) + len(b_expire_old_poluted) + len(c_expire_empty) + len(d_false_clear) + len(f_renewed_no_expire_old)
    total_issues = total_affected + len(e_ghost_utils)

    return {
        "tickets_checked": len(tickets),
        "responses_checked": len(all_utils),
        "A_expire_poluted": a_expire_poluted,
        "B_expire_old_poluted": b_expire_old_poluted,
        "C_expire_empty": c_expire_empty,
        "D_false_clear": d_false_clear,
        "E_ghost_utils": e_ghost_utils,
        "F_renewed_no_expire_old": f_renewed_no_expire_old,
        "total_affected_tickets": total_affected,
        "total_issues": total_issues,
    }


def _audit_format_report(data, max_show=20):
    """Formata o resultado de _audit_collect como string texto (pra print ou email).

    max_show = 0 retorna só o resumo (sem listagens).
    """
    lines = []
    lines.append("=" * 60)
    lines.append("           RELATÓRIO DE AUDITORIA DO BANCO")
    lines.append("=" * 60)
    lines.append(f"Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    lines.append("")

    sections = [
        ("A. EXPIRE POLUÍDO (formato antigo tipo '04/15/26 Time: 23:59')",
            data["A_expire_poluted"], "python 811_sync.py --fix-expires"),
        ("B. EXPIRE_OLD POLUÍDO (renovados com formato antigo)",
            data["B_expire_old_poluted"], "python 811_sync.py --fix-renewals"),
        ("C. EXPIRE VAZIO em ticket Open/Damage (scraper não achou data)",
            data["C_expire_empty"], "editar manualmente no app OU aguardar próximo sync"),
        ("D. FALSO CLEAR (status=Clear mas utilities Pending - perigoso!)",
            data["D_false_clear"], "scraper reverte automaticamente no próximo sync"),
        ("E. GHOST UTILITIES (lixo de UI: 'All (N)', 'Event', etc)",
            data["E_ghost_utils"], "python 811_sync.py --clean-ghost-utilities"),
        ("F. RENOVADO SEM expire_old (cadeia de renovação quebrada)",
            data["F_renewed_no_expire_old"], "python 811_sync.py --fix-renewals"),
    ]

    for label, items, fix in sections:
        lines.append(f"── {label} ──")
        lines.append(f"  Total: {len(items)}")
        if items and max_show > 0:
            lines.append(f"  Como corrigir: {fix}")
            lines.append(f"  Primeiros {min(max_show, len(items))}:")
            for it in items[:max_show]:
                bits = [f"{k}={v!r}" for k, v in it.items() if k != "id"]
                lines.append(f"    - {' · '.join(bits)}")
            if len(items) > max_show:
                lines.append(f"    ... + {len(items) - max_show} outros")
        lines.append("")

    lines.append("=" * 60)
    lines.append(f"RESUMO: {data['tickets_checked']} tickets ativos auditados")
    lines.append(f"  A. expire poluído:              {len(data['A_expire_poluted']):4d}")
    lines.append(f"  B. expire_old poluído:          {len(data['B_expire_old_poluted']):4d}")
    lines.append(f"  C. expire vazio (Open/Damage):  {len(data['C_expire_empty']):4d}")
    lines.append(f"  D. falso Clear:                 {len(data['D_false_clear']):4d}")
    lines.append(f"  E. ghost utilities:             {len(data['E_ghost_utils']):4d}")
    lines.append(f"  F. renovado sem expire_old:     {len(data['F_renewed_no_expire_old']):4d}")
    lines.append(f"  ─────────────────────────────────────")
    lines.append(f"  Tickets com algum problema:     {data['total_affected_tickets']} (pode haver sobreposição)")
    lines.append(f"  Respostas utilities ghost:      {len(data['E_ghost_utils'])}")
    lines.append("=" * 60)
    return "\n".join(lines)


def audit_health():
    """Audita o banco e imprime relatório de anomalias SEM alterar nada.

    Detecta 6 problemas comuns em tickets ativos (Open/Damage/Clear).
    Só LEITURA. Pra corrigir, usar --fix-expires, --fix-renewals, --clean-ghost-utilities.
    """
    log.info("=" * 55)
    log.info(f"  AUDIT-HEALTH: diagnóstico de anomalias (read-only)")
    log.info("=" * 55)
    data = _audit_collect()
    log.info(f"[Audit] {data['tickets_checked']} tickets ativos, {data['responses_checked']} respostas verificadas")
    print()
    print(_audit_format_report(data))
    print()


def audit_health_and_email():
    """Roda auditoria e envia email SE houver anomalia.

    Pensado pra agendamento diário (Task Scheduler).
    Se total_issues == 0, não envia email — silêncio é bom.
    """
    log.info("=" * 55)
    log.info(f"  AUDIT-HEALTH-EMAIL: verificação diária com alerta")
    log.info("=" * 55)
    data = _audit_collect()
    log.info(f"[AuditEmail] {data['tickets_checked']} tickets, {data['responses_checked']} respostas")
    log.info(f"[AuditEmail] Total de anomalias: {data['total_issues']}")

    if data["total_issues"] == 0:
        log.info("[AuditEmail] ✅ Banco limpo — nenhum email necessário")
        return

    log.warning(f"[AuditEmail] ⚠ {data['total_issues']} anomalia(s) detectada(s) — enviando email")

    import smtplib
    from email.mime.text import MIMEText

    gmail_user = os.getenv("GMAIL_USER")
    gmail_pass = os.getenv("GMAIL_PASS")
    alert_to = os.getenv("AUDIT_EMAIL") or os.getenv("ALERT_EMAIL") or "engineering@onedrill.us"

    if not all([gmail_user, gmail_pass]):
        log.error("[AuditEmail] GMAIL_USER/GMAIL_PASS não configurados no .env — email não enviado")
        log.info("[AuditEmail] Relatório (sem enviar):")
        print(_audit_format_report(data))
        return

    # Prioridade por severidade
    critical = len(data["D_false_clear"]) + len(data["F_renewed_no_expire_old"])
    high = len(data["A_expire_poluted"]) + len(data["B_expire_old_poluted"])
    medium = len(data["E_ghost_utils"]) + len(data["C_expire_empty"])

    prefix = "🔴 CRÍTICO" if critical > 0 else ("🟡 ALTO" if high > 0 else "🟢 MÉDIO")

    subject = f"[OneDrill 811] {prefix} — {data['total_issues']} anomalia(s) no banco ({datetime.now().strftime('%d/%m/%Y')})"

    body = _audit_format_report(data)
    body += "\n\n"
    body += "=" * 60 + "\n"
    body += "AÇÕES RECOMENDADAS (em ordem):\n"
    body += "=" * 60 + "\n"
    if data["E_ghost_utils"]:
        body += f"1. Limpar ghost utilities ({len(data['E_ghost_utils'])} respostas):\n"
        body += "   python 811_sync.py --clean-ghost-utilities\n\n"
    if data["A_expire_poluted"]:
        body += f"2. Corrigir expire poluído ({len(data['A_expire_poluted'])} tickets):\n"
        body += "   python 811_sync.py --fix-expires\n\n"
    if data["B_expire_old_poluted"] or data["F_renewed_no_expire_old"]:
        n = len(data["B_expire_old_poluted"]) + len(data["F_renewed_no_expire_old"])
        body += f"3. Corrigir renovações quebradas ({n} tickets):\n"
        body += "   python 811_sync.py --fix-renewals\n\n"
    if data["D_false_clear"]:
        body += f"4. Falsos Clear ({len(data['D_false_clear'])} tickets): o scraper novo reverte automaticamente no próximo ciclo. Se persistir após 2 dias, investigar.\n\n"
    if data["C_expire_empty"]:
        body += f"5. Expire vazio ({len(data['C_expire_empty'])} tickets): editar manualmente no app ou aguardar sync.\n\n"

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = alert_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(gmail_user, gmail_pass)
            s.send_message(msg)
        log.info(f"[AuditEmail] ✅ Email enviado pra {alert_to}")
    except Exception as e:
        log.error(f"[AuditEmail] Erro ao enviar email: {e}")
        log.info("[AuditEmail] Relatório que seria enviado:")
        print(body)


def check_expiring_tickets(days_ahead=5, target_state=None):
    """Alerta por email sobre tickets Open/Damage que vencem nos próximos N dias.

    Pensado pra rodar diário (Task Scheduler) ou antes de cada sync.
    Se nenhum ticket expirando, silêncio total.
    """
    log.info("=" * 55)
    log.info(f"  CHECK-EXPIRING: tickets vencendo em {days_ahead} dias")
    log.info("=" * 55)

    query = "&status=in.(Open,Damage)&order=expire"
    if target_state:
        query += f"&state=eq.{_qv(target_state)}"

    tickets_list = sb_get("tickets", query)
    if not tickets_list:
        log.info("[Expiring] Nenhum ticket Open/Damage no banco")
        return

    today = datetime.now().date()
    cutoff = today + timedelta(days=days_ahead)

    expiring = []
    already_expired = []

    for t in tickets_list:
        raw_expire = (t.get("expire") or "").strip()
        if not raw_expire:
            continue
        norm = normalize_expire(raw_expire)
        if not norm:
            continue
        try:
            m = re.match(r"(\d{2})/(\d{2})/(\d{4})", norm)
            if not m:
                continue
            exp_date = datetime(int(m.group(3)), int(m.group(1)), int(m.group(2))).date()
        except (ValueError, IndexError):
            continue

        if exp_date < today:
            already_expired.append((t, exp_date))
        elif exp_date <= cutoff:
            expiring.append((t, exp_date))

    total = len(expiring) + len(already_expired)
    if total == 0:
        log.info(f"[Expiring] ✅ Nenhum ticket vencendo até {cutoff.strftime('%m/%d/%Y')}")
        return

    log.warning(f"[Expiring] ⚠ {len(expiring)} vencendo em {days_ahead}d + {len(already_expired)} já vencido(s)")

    lines = []
    lines.append(f"OneDrill 811 — Alerta de Vencimento ({datetime.now().strftime('%d/%m/%Y %H:%M')})")
    lines.append("=" * 60)

    if already_expired:
        lines.append(f"\n⛔ JÁ VENCIDOS ({len(already_expired)}):")
        lines.append("-" * 40)
        for t, d in sorted(already_expired, key=lambda x: x[1]):
            days_ago = (today - d).days
            lines.append(f"  [{t.get('state','?')}] {t['ticket']}  venceu {d.strftime('%m/%d/%Y')} ({days_ago}d atrás)  — {t.get('status')} — {t.get('location','')}")

    if expiring:
        lines.append(f"\n⚠ VENCENDO EM {days_ahead} DIAS ({len(expiring)}):")
        lines.append("-" * 40)
        for t, d in sorted(expiring, key=lambda x: x[1]):
            days_left = (d - today).days
            lines.append(f"  [{t.get('state','?')}] {t['ticket']}  vence {d.strftime('%m/%d/%Y')} ({days_left}d)  — {t.get('status')} — {t.get('location','')}")

    lines.append("\n" + "=" * 60)
    lines.append("Ação: renovar tickets antes do vencimento no portal 811.")
    report = "\n".join(lines)

    print(report)

    import smtplib
    from email.mime.text import MIMEText

    gmail_user = os.getenv("GMAIL_USER")
    gmail_pass = os.getenv("GMAIL_PASS")
    alert_to = os.getenv("AUDIT_EMAIL") or os.getenv("ALERT_EMAIL")

    if not all([gmail_user, gmail_pass, alert_to]):
        log.info("[Expiring] Email não configurado — alerta só no console")
        return

    prefix = "⛔" if already_expired else "⚠"
    subject = f"[OneDrill] {prefix} {total} ticket(s) vencendo/vencido(s) — {datetime.now().strftime('%d/%m/%Y')}"

    msg = MIMEText(report, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = alert_to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(gmail_user, gmail_pass)
            s.send_message(msg)
        log.info(f"[Expiring] ✅ Alerta enviado pra {alert_to}")
    except Exception as e:
        log.warning(f"[Expiring] Erro ao enviar email: {e}")


def clean_ghost_utilities():
    """Remove respostas de utilities fantasmas (lixo de UI) do Supabase.

    Um bug antigo no parser captava strings de UI do portal Indiana
    (ex: "All (6)", "Current (3)", "Event") como se fossem utilities reais.
    Essas linhas foram gravadas em ticket_811_responses e aparecem no app
    como utilities pendentes inexistentes.

    Esta função:
      - Lista todas as utilities no banco
      - Filtra pelas que FALHAM no _is_valid_utility_name (lixo de UI)
      - Mostra quantas vai deletar e de quais tickets
      - Deleta em batch

    SEGURO: só apaga respostas cujo utility_name casa com padrões de UI.
    Utilities reais (DUKE ENERGY, COMCAST, etc) nunca são tocadas.
    """
    log.info("=" * 55)
    log.info(f"  CLEAN GHOST UTILITIES: limpando respostas fantasmas")
    log.info("=" * 55)

    # Puxa todas as respostas em páginas de 1000
    all_resps = []
    offset = 0
    while True:
        page = sb_get("ticket_811_responses", f"&select=id,ticket_num,utility_name,status,state&order=id&limit=1000&offset={offset}")
        if not page:
            break
        all_resps.extend(page)
        if len(page) < 1000:
            break
        offset += 1000

    log.info(f"[CleanGhost] {len(all_resps)} respostas totais no banco")

    # Identifica fantasmas
    ghosts = []
    for r in all_resps:
        name = r.get("utility_name", "")
        if not _is_valid_utility_name(name):
            ghosts.append(r)

    if not ghosts:
        log.info("[CleanGhost] ✅ Nenhuma utility fantasma encontrada — banco limpo")
        return

    # Mostra preview
    log.info(f"[CleanGhost] ⚠ {len(ghosts)} respostas fantasmas encontradas:")
    by_name = {}
    by_ticket = {}
    for r in ghosts:
        name = r.get("utility_name", "(vazio)")
        tnum = r.get("ticket_num", "?")
        by_name[name] = by_name.get(name, 0) + 1
        by_ticket.setdefault(tnum, []).append(name)

    log.info(f"[CleanGhost] Nomes fantasmas encontrados:")
    for name, count in sorted(by_name.items(), key=lambda x: -x[1]):
        log.info(f"  - '{name}' ({count}x)")

    log.info(f"[CleanGhost] {len(by_ticket)} tickets afetados")

    # Confirmação
    print()
    print(f"⚠ Esta ação vai DELETAR {len(ghosts)} respostas do banco.")
    print(f"   Tickets afetados: {len(by_ticket)}")
    print(f"   Exemplos de nomes a deletar: {list(by_name.keys())[:5]}")
    print()
    resp = input("Confirma exclusão? (digite 'SIM' pra continuar): ").strip()
    if resp != "SIM":
        log.info("[CleanGhost] Cancelado pelo usuário")
        return

    # Deleta em batch por ID
    deleted = 0
    errors = 0
    batch_size = 50
    for i in range(0, len(ghosts), batch_size):
        batch = ghosts[i:i+batch_size]
        ids = [r["id"] for r in batch]
        # Supabase DELETE com filtro in.()
        id_list = ",".join(str(x) for x in ids)
        try:
            sb_delete("ticket_811_responses", f"&id=in.({id_list})")
            deleted += len(batch)
            log.info(f"[CleanGhost] {deleted}/{len(ghosts)} deletados")
        except Exception as e:
            log.error(f"[CleanGhost] Erro no batch {i}-{i+batch_size}: {e}")
            errors += len(batch)

    log.info(f"[CleanGhost] === CONCLUÍDO: {deleted} deletados, {errors} erros ===")


# ── │ SECTION: DEBUG_EXPIRE │ DEBUG EXPIRE: mostra o body e testa todos os patterns 
async def debug_expire(target_ticket, state=None):
    """Abre o ticket no portal, mostra o body cru e testa todos os patterns.

    Útil pra diagnosticar quando extract_expire_date está pegando data errada.
    """
    if not state:
        # Auto-detecta o estado consultando o banco
        rows = sb_get("tickets", f"&ticket=eq.{_qv(target_ticket)}&select=state&limit=1")
        if not rows:
            log.error(f"[DebugExpire] Ticket {target_ticket} não encontrado no banco")
            return
        state = rows[0].get("state", "")

    if state not in PORTALS:
        log.error(f"[DebugExpire] Estado '{state}' sem portal (FL/IN suportados)")
        return

    log.info("=" * 55)
    log.info(f"  DEBUG-EXPIRE: {target_ticket} ({state})")
    log.info("=" * 55)

    perfil = _profile_path(state)

    async with async_playwright() as p:
        ctx = await p.chromium.launch_persistent_context(perfil, headless=True, args=["--no-sandbox"])
        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
        page.set_default_timeout(TIMEOUT_PAGE)

        await page.goto(PORTALS[state]["home"], wait_until="domcontentloaded")
        await wait_stable(page)
        page, ctx = await ensure_login(page, ctx, p, state)
        if not page:
            log.error(f"[DebugExpire] Login falhou")
            return

        ok = await goto_dashboard(page, state)
        if not ok:
            log.error(f"[DebugExpire] Dashboard inacessível")
            await ctx.close()
            return

        try:
            await filter_ticket(page, target_ticket)
            if not await page.get_by_text(target_ticket, exact=True).count():
                log.error(f"[DebugExpire] Ticket {target_ticket} não encontrado")
                await ctx.close()
                return

            await page.get_by_text(target_ticket, exact=True).first.click()
            await wait_stable(page)

            tt = page.locator('[role="tab"]:has-text("Text")').first
            if await tt.count():
                await click_and_wait(page, tt, "tab")

            body = await page.locator("body").inner_text()

            # Salva body cru num arquivo pra inspeção
            dump_path = os.path.join(BASE_DIR, f"debug_expire_{target_ticket}.txt")
            with open(dump_path, "w", encoding="utf-8") as f:
                f.write(body)
            log.info(f"Body cru salvo em: {dump_path}")

            # Mostra todas as linhas que contêm "expir", "due", "date", "work"
            print("\n" + "=" * 55)
            print("LINHAS RELEVANTES DO BODY:")
            print("=" * 55)
            keywords = ["expir", "due", "work date", "legal", "response", "start", "renewal"]
            for line_num, line in enumerate(body.split("\n"), 1):
                l = line.lower()
                if any(kw in l for kw in keywords):
                    print(f"  L{line_num:4d}: {line.strip()[:100]}")

            # Testa cada pattern e mostra o que bateu
            print("\n" + "=" * 55)
            print("TESTE DE PATTERNS:")
            print("=" * 55)
            test_patterns = [
                (r"Ticket\s+Expires?\s*(?:on)?\s*:\s*([^\n]+)",    "✓ Ticket Expires"),
                (r"Expiration\s+Date\s*:\s*([^\n]+)",               "✓ Expiration Date"),
                (r"Expiration\s*:\s*([^\n]+)",                       "✓ Expiration"),
                (r"(?<!\w)Expires?\s+on\s*:\s*([^\n]+)",             "✓ Expires on"),
                (r"(?<!\w)Expires\s*:\s*([^\n]+)",                   "✓ Expires"),
                (r"(?<!\w)Expire\s*:\s*([^\n]+)",                    "✓ Expire"),
                (r"Due\s*Date\s*:\s*([^\n]+)",                       "✗ Due Date (IGNORADO — data errada)"),
                (r"Work\s*[Dd]ate\s*:\s*([^\n]+)",                   "✗ Work Date (IGNORADO — data errada)"),
                (r"Legal\s*Date\s*:\s*([^\n]+)",                     "✗ Legal Date (IGNORADO — data errada)"),
            ]
            for pat, label in test_patterns:
                m = re.search(pat, body, re.IGNORECASE)
                val = m.group(1).strip()[:80] if m else "—"
                print(f"  {label:50s}: {val}")

            # Resultado final
            print("\n" + "=" * 55)
            print("RESULTADO DO PARSER:")
            print("=" * 55)
            result = extract_expire_date(body, ticket_num=target_ticket, debug=True)
            print(f"  extract_expire_date() → '{result}'")

            # Compara com banco
            rows = sb_get("tickets", f"&ticket=eq.{_qv(target_ticket)}&select=expire&limit=1")
            if rows:
                db_val = rows[0].get("expire", "")
                print(f"  Valor no banco        → '{db_val}'")
                if result != db_val:
                    print(f"  ⚠ DIFERENTE — rode '--fix-expires --ticket {target_ticket}' pra corrigir")

            print("=" * 55 + "\n")

        except Exception as e:
            log.error(f"[DebugExpire] ERRO: {e}")
        finally:
            try:
                await ctx.close()
            except Exception:
                pass


# ── │ SECTION: CLI │ CLI ──────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OneDrill - 811 Sync v2")
    parser.add_argument("--state",      choices=["FL", "IN", "IL", "WI"], help="Estado a sincronizar")
    parser.add_argument("--all",        action="store_true", help="Sincronizar FL + IN")
    parser.add_argument("--imp",        action="store_true", help="Importar tickets novos + respostas")
    parser.add_argument("--imp_all",    action="store_true", help="Importar FL + IN")
    parser.add_argument("--excel",      action="store_true", help="Exportar Excel")
    parser.add_argument("--debug",      action="store_true", help="Debug: screenshots")
    parser.add_argument("--rescrape",   action="store_true", help="Re-scrape notes")
    parser.add_argument("--cleanup",    action="store_true", help="Excluir tickets cancelados")
    parser.add_argument("--cleanup_all", action="store_true", help="Cleanup IN + FL em paralelo")
    parser.add_argument("--contacts",   action="store_true", help="Scrape contatos de utilities (FL)")
    parser.add_argument("--force",      action="store_true", help="Forçar re-scrape de TODOS")
    parser.add_argument("--backfill",   action="store_true", help="Backfill: adicionar eventos de clear no histórico")
    parser.add_argument("--fix-dates",  action="store_true", help="Corrigir datas de clear usando data real da última resposta 811")
    parser.add_argument("--fix-clear-ts", action="store_true", help="Fix 2026-05-14: ajusta ts dos AUTO 811 Clear pra synced_at")
    parser.add_argument("--scan-email", action="store_true", help="Varre Gmail buscando status change de utilities (dry run por default)")
    parser.add_argument("--apply-overrides", action="store_true", help="Aplica LOCAL_OVERRIDES + auto-clear")
    parser.add_argument("--undo-fake-overrides", action="store_true", help="Remove entries (override local) com ts=hoje (cleanup do bug RE-CONFIRMA)")
    parser.add_argument("--debug-history", type=str, help="Mostra historico completo de um ticket. Ex: --debug-history 26031001348")
    parser.add_argument("--apply-today-clears", action="store_true", help="Adiciona entry no historico de tickets ja Clear cujas utilities responderam HOJE")
    parser.add_argument("--target-date", type=str, default=None, help="YYYY-MM-DD pra apply-today-clears (default: hoje)")
    parser.add_argument("--commit", action="store_true", help="Usado com --scan-email: atualiza banco (sem ele eh dry run)")
    parser.add_argument("--days-back", type=int, default=7, help="Dias pra tras pra varrer emails (default 7)")
    parser.add_argument("--fix-renewals", action="store_true", help="Corrigir expire_old de tickets renovados (busca data real no portal)")
    parser.add_argument("--fix-expires", action="store_true", help="Corrigir data de vencimento (expire) de tickets com formato antigo ou errado")
    parser.add_argument("--populate-counties", action="store_true", help="Backfill de county em tickets antigos (usa base cidade→county + Nominatim fallback)")
    parser.add_argument("--rebuild-coverage", action="store_true", help="Reconstroi tabela utility_county_coverage a partir de ticket_811_responses × tickets.county")
    parser.add_argument("--clean-ghost-utilities", action="store_true", help="Remove respostas de utilities fantasmas (lixo 'All (N)', 'Event', etc)")
    parser.add_argument("--audit-health", action="store_true", help="Relatório de anomalias do banco (read-only, não altera nada)")
    parser.add_argument("--audit-health-email", action="store_true", help="Auditoria + email se anomalias > 0 (pra agendador)")
    parser.add_argument("--check-expiring", action="store_true", help="Alerta de tickets Open/Damage vencendo nos próximos N dias")
    parser.add_argument("--days-ahead", type=int, default=5, help="Dias pra frente pra checar vencimento (default 5)")
    parser.add_argument("--debug-expire", action="store_true", help="Diagnosticar extração de expire de um ticket — requer --ticket")
    parser.add_argument("--ticket",     type=str, help="Número do ticket alvo (usado com --fix-expires ou --debug-expire)")
    parser.add_argument("--no-cache",   action="store_true", help="Forçar re-scrape de todos (incluindo Clear em cache)")
    parser.add_argument("--selftest",   action="store_true", help="Rodar testes internos (classify, parser)")
    parser.add_argument("--backup",     action="store_true", help="Backup completo do banco de dados (JSON)")
    parser.add_argument("--save-pdf",   action="store_true", help="Salvar PDF de tickets Clear/Damage via impressora")
    parser.add_argument("--sync-il",    action="store_true", help="Sincronizar respostas JULIE (Illinois)")
    parser.add_argument("--sync-wi",    action="store_true", help="Sincronizar respostas Diggers Hotline (Wisconsin)")
    parser.add_argument("--no-lock",    action="store_true", help="Pular verificação de lock file")
    args = parser.parse_args()

    # Lock file — pula pra debug e selftest
    use_lock = not args.no_lock and not args.debug and not args.selftest

    def _run():
        if args.selftest:
            run_self_tests()
        elif args.backup:
            backup_database()
        elif getattr(args, 'save_pdf', False):
            if args.state == "IL":
                asyncio.run(save_ticket_pdfs_il(force=args.force))
            elif args.state:
                asyncio.run(save_ticket_pdfs(args.state, force=args.force))
            else:
                asyncio.run(save_ticket_pdfs("FL", force=args.force))
                asyncio.run(save_ticket_pdfs("IN", force=args.force))
        elif getattr(args, 'sync_il', False):
            asyncio.run(sync_il())
        elif getattr(args, 'sync_wi', False):
            asyncio.run(sync_wi())
        elif getattr(args, 'fix_renewals', False):
            asyncio.run(fix_renewals())
        elif getattr(args, 'fix_expires', False):
            asyncio.run(fix_expires(target_ticket=args.ticket))
        elif getattr(args, 'populate_counties', False):
            asyncio.run(populate_counties(target_state=args.state, force=args.force))
        elif getattr(args, 'rebuild_coverage', False):
            rebuild_utility_county_coverage()
        elif getattr(args, 'clean_ghost_utilities', False):
            clean_ghost_utilities()
        elif getattr(args, 'audit_health', False):
            audit_health()
        elif getattr(args, 'audit_health_email', False):
            audit_health_and_email()
        elif getattr(args, 'check_expiring', False):
            check_expiring_tickets(
                days_ahead=getattr(args, 'days_ahead', 5),
                target_state=args.state
            )
        elif getattr(args, 'debug_expire', False):
            if not args.ticket:
                log.error("--debug-expire requer --ticket NNNNNN")
                sys.exit(1)
            asyncio.run(debug_expire(args.ticket, state=args.state))
        elif args.debug:
            asyncio.run(debug_screenshot(args.state or "IN"))
        elif args.excel:
            export_excel()
        elif args.contacts:
            asyncio.run(scrape_contacts(args.state or "FL", force=args.force))
        elif args.cleanup_all:
            asyncio.run(cleanup_all())
        elif args.cleanup:
            asyncio.run(cleanup_canceled(args.state or "IN"))
        elif args.rescrape:
            asyncio.run(rescrape_notes(args.state or "FL", force=args.force))
        elif args.backfill:
            backfill_history()
        elif getattr(args, 'fix_dates', False):
            fix_clear_dates()
        elif getattr(args, 'fix_clear_ts', False):
            fix_clear_ts(target_state=args.state)
        elif getattr(args, 'apply_overrides', False):
            apply_overrides_now(target_state=args.state)
        elif getattr(args, 'undo_fake_overrides', False):
            undo_fake_overrides_today(target_state=args.state)
        elif getattr(args, 'debug_history', None):
            debug_ticket_history(args.debug_history)
        elif getattr(args, 'apply_today_clears', False):
            apply_today_clears(
                target_state=args.state,
                target_date=getattr(args, 'target_date', None)
            )
        elif getattr(args, 'scan_email', False):
            scan_emails_for_responses(
                commit=getattr(args, 'commit', False),
                state_filter=args.state,
                days_back=getattr(args, 'days_back', 7)
            )
        elif args.all:
            asyncio.run(sync_all())
        elif args.imp_all:
            asyncio.run(sync_and_import_all())
        elif args.imp:
            asyncio.run(sync_and_import(args.state or "IN"))
        elif args.state == "IL":
            asyncio.run(sync_il())
        elif args.state == "WI":
            asyncio.run(sync_wi())
        elif args.state:
            asyncio.run(sync_state(args.state))
        else:
            parser.print_help()

    if use_lock:
        try:
            with ProcessLock():
                _run()
        except RuntimeError as e:
            log.error(str(e))
            sys.exit(1)
    else:
        _run()
