"""
verificar_ticket_antigo.py - Verifica/restaura tickets antigos renovados.

Uso:
    python verificar_ticket_antigo.py 20261906533              # so verifica
    python verificar_ticket_antigo.py 20261906533 --restore    # recria 1
    python verificar_ticket_antigo.py --all-missing --dry-run  # lista TODOS faltantes
    python verificar_ticket_antigo.py --all-missing            # restaura TODOS faltantes
    python verificar_ticket_antigo.py --all-missing --yes      # sem confirmacao
"""
import sys
import os
import json
import argparse
import urllib.request
import urllib.parse
import urllib.error


def load_env():
    env = {}
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env[k.strip()] = v.strip().strip('"').strip("'")
    return env


ENV = load_env()
SUPA_URL = ENV.get("SB_URL", "") or ENV.get("SUPABASE_URL", "")
SUPA_KEY = (ENV.get("SB_KEY", "") or ENV.get("SUPABASE_KEY", "")
            or ENV.get("SUPABASE_ANON_KEY", ""))

if not SUPA_URL or not SUPA_KEY:
    print("ERRO: SB_URL/SB_KEY nao encontrados no .env")
    sys.exit(1)


def sb_request(method, path, body=None):
    url = f"{SUPA_URL}/rest/v1/{path}"
    headers = {
        "apikey": SUPA_KEY,
        "Authorization": f"Bearer {SUPA_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"  HTTP {e.code}: {e.read().decode('utf-8')[:300]}")
        return None


def build_row(old_num, novo):
    """Constroi a row pra restaurar o ticket antigo a partir do snapshot."""
    expire_old = novo.get("expire_old") or ""
    status_old = novo.get("status_old") or "Closed"
    exp_clean = expire_old.split("Time:")[0].strip()
    return {
        "ticket": old_num,
        "state": novo.get("state") or "",
        "status": status_old or "Closed",
        "expire": exp_clean,
        "client": novo.get("client") or "",
        "prime": novo.get("prime") or "",
        "location": novo.get("location") or "",
        "address": novo.get("address") or "",
        "company": novo.get("company") or "One Drill",
        "project_id": novo.get("project_id"),
        "tipo": novo.get("tipo") or "",
        "job": novo.get("job") or "",
        "footage": novo.get("footage") or 0,
        "notes": f"[RESTORE] Restaurado do snapshot do ticket {novo.get('ticket')}",
        "old_ticket2": "",
        "status_old": "",
        "expire_old": "",
        "pending": "",
        "history": [{"ts": 0, "action": f"[RESTORE] Restaurado do snapshot do ticket {novo.get('ticket')}", "color": "#7c3aed"}],
        "attachments": [],
        "status_locked": False,
        "project_locked": False,
    }


def restore_one(old_num, novo, dry_run=False, ask=True):
    """Restaura um ticket. Retorna True se criou, False se pulou/erro."""
    expire_old = (novo.get("expire_old") or "").strip()
    if not expire_old:
        print(f"  [SKIP] {old_num}: expire_old vazio (novo={novo.get('ticket')})")
        return False
    row = build_row(old_num, novo)
    if dry_run:
        client = (row['client'] or '')[:25]
        print(f"  [DRY] {old_num} <- {novo.get('ticket')} | {row['state']} | exp {row['expire']} | {row['status']} | {client}")
        return True
    if ask:
        ans = input(f"  Criar {old_num}? [s/N]: ").strip().lower()
        if ans != "s":
            print(f"  [SKIP] {old_num}")
            return False
    created = sb_request("POST", "tickets", row)
    if created:
        print(f"  [OK] {old_num} criado (id={created[0].get('id')})")
        return True
    print(f"  [ERRO] {old_num}: insert falhou")
    return False


def cmd_all_missing(args):
    print("\n=== Detectando tickets antigos faltantes ===\n")
    renewed = sb_request(
        "GET",
        "tickets?old_ticket2=not.is.null&select=ticket,old_ticket2,state,status,client,prime,location,address,company,project_id,tipo,job,footage,status_old,expire_old&order=ticket"
    )
    if not renewed:
        print("Nenhum ticket renovado encontrado.")
        return
    # Mapeia old_num -> primeiro novo encontrado
    missing_map = {}
    for novo in renewed:
        chain = (novo.get("old_ticket2") or "").strip()
        if not chain:
            continue
        for old in chain.split(" → "):
            old = old.strip()
            if old and old not in missing_map:
                missing_map[old] = novo
    print(f"Total de tickets antigos referenciados: {len(missing_map)}")
    # Verifica quais existem no Supabase em batch
    all_old = list(missing_map.keys())
    existing = set()
    for i in range(0, len(all_old), 100):
        chunk = all_old[i:i + 100]
        in_list = ",".join(chunk)
        r = sb_request("GET", f"tickets?ticket=in.({urllib.parse.quote(in_list)})&select=ticket")
        if r:
            for row in r:
                existing.add(row["ticket"])
    missing = [n for n in all_old if n not in existing]
    print(f"Existem no Supabase: {len(existing)}")
    print(f"Faltando (vao restaurar): {len(missing)}\n")
    if not missing:
        print("Nada pra restaurar.")
        return
    candidates = [(n, missing_map[n]) for n in missing if (missing_map[n].get("expire_old") or "").strip()]
    sem_expire = len(missing) - len(candidates)
    if sem_expire:
        print(f"({sem_expire} sem expire_old -> pulados)\n")
    if args.dry_run:
        print(f"DRY RUN - mostrando primeiros 30 de {len(candidates)}:\n")
        for old_num, novo in candidates[:30]:
            restore_one(old_num, novo, dry_run=True)
        if len(candidates) > 30:
            print(f"\n... e mais {len(candidates) - 30}.")
        print("\nPra executar de verdade, rode SEM --dry-run.")
        return
    print(f"Vai criar {len(candidates)} rows no Supabase.")
    if not args.yes:
        ans = input("Confirma TUDO? [s/N]: ").strip().lower()
        if ans != "s":
            print("Abortado. Use --yes pra pular essa confirmacao.")
            return
    ok = 0
    err = 0
    for old_num, novo in candidates:
        if restore_one(old_num, novo, dry_run=False, ask=False):
            ok += 1
        else:
            err += 1
    print(f"\n=== DONE ===  Criados: {ok}  Erros: {err}")
    print("Recarregue a pagina (Ctrl+Shift+R) pra ver os tickets restaurados.")


def cmd_single(args):
    tnum = args.ticket.strip()
    print(f"\n=== Verificando ticket {tnum} no Supabase ===\n")
    res = sb_request("GET", f"tickets?ticket=eq.{tnum}&select=*")
    if res:
        r = res[0]
        print(f"[OK] {tnum} ENCONTRADO:")
        print(f"  id:       {r.get('id')}")
        print(f"  state:    {r.get('state')}")
        print(f"  status:   {r.get('status')}")
        print(f"  expire:   {r.get('expire')}")
        print(f"  client:   {r.get('client')}")
        print(f"  location: {r.get('location')}")
        if args.update_expire:
            print(f"\nAtualizando expire para {args.update_expire}...")
            upd = sb_request("PATCH", f"tickets?id=eq.{r['id']}", {"expire": args.update_expire})
            if upd:
                print("[OK] expire atualizado")
        return
    print(f"[AVISO] {tnum} NAO existe como row. Buscando como old_ticket2...\n")
    like_q = urllib.parse.quote(f"*{tnum}*")
    res2 = sb_request("GET", f"tickets?old_ticket2=like.{like_q}&select=*")
    if not res2:
        print(f"[ERRO] {tnum} nao encontrado em lugar nenhum.")
        sys.exit(2)
    print(f"[OK] {tnum} aparece em {len(res2)} ticket(s) como old_ticket2:")
    for nt in res2:
        print(f"  -> NOVO: {nt.get('ticket')}  state={nt.get('state')}  status={nt.get('status')}")
        print(f"     old_ticket2: {nt.get('old_ticket2')}")
        print(f"     status_old:  {nt.get('status_old')}")
        print(f"     expire_old:  {nt.get('expire_old')}")
        print(f"     client:      {nt.get('client')}")
        print()
    if not args.restore:
        print("Pra recriar como row separada:")
        print(f"  python verificar_ticket_antigo.py {tnum} --restore")
        print("Pra recriar TODOS os antigos faltantes de uma vez:")
        print("  python verificar_ticket_antigo.py --all-missing --dry-run")
        print("  python verificar_ticket_antigo.py --all-missing")
        return
    novo = res2[0]
    if not (novo.get("expire_old") or "").strip():
        print(f"[ERRO] expire_old vazio em {novo.get('ticket')} - nao da pra restaurar")
        sys.exit(3)
    row = build_row(tnum, novo)
    print("\n[RESTORE] Vou criar essa row:")
    for k in ("ticket", "state", "status", "expire", "client", "location"):
        print(f"  {k}: {row[k]}")
    ans = input("\nConfirma? [s/N]: ").strip().lower()
    if ans != "s":
        print("Abortado.")
        return
    created = sb_request("POST", "tickets", row)
    if created:
        print(f"[OK] Row criada - id={created[0].get('id')}")
        print(f"\nRecarregue a pagina pra ver {tnum}.")
    else:
        print("[ERRO] Insert falhou")
        sys.exit(4)


def main():
    ap = argparse.ArgumentParser(description="Verifica/restaura tickets antigos renovados")
    ap.add_argument("ticket", nargs="?", help="Numero do ticket antigo")
    ap.add_argument("--restore", action="store_true", help="Recria do snapshot")
    ap.add_argument("--update-expire", help="MM/DD/AAAA")
    ap.add_argument("--all-missing", action="store_true", help="Restaura TODOS faltantes")
    ap.add_argument("--dry-run", action="store_true", help="So mostra, nao escreve")
    ap.add_argument("--yes", action="store_true", help="Pula confirmacao geral")
    args = ap.parse_args()
    if args.all_missing:
        cmd_all_missing(args)
        return
    if not args.ticket:
        ap.print_help()
        sys.exit(1)
    cmd_single(args)


if __name__ == "__main__":
    main()
