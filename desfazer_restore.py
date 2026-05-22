"""
desfazer_restore.py - Apaga as rows criadas por verificar_ticket_antigo.py --all-missing

Identifica pelo prefixo [RESTORE] no campo notes que foi inserido pelo script.
Mostra primeiro o que vai apagar (dry-run), depois pergunta confirmacao.

Uso:
    python desfazer_restore.py              # mostra + pergunta confirmacao
    python desfazer_restore.py --dry-run    # mostra e sai (sem deletar)
    python desfazer_restore.py --force      # deleta sem perguntar (cuidado!)
"""
import sys
import os
import json
import urllib.request
import urllib.parse
import urllib.error
import argparse
from datetime import datetime


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


def save_pre_delete_backup(rows):
    """Salva os registros que serao deletados em JSON antes de deletar."""
    backup_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    path = os.path.join(backup_dir, f"desfazer_restore_{ts}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"deleted_at": ts, "count": len(rows), "rows": rows}, f, ensure_ascii=False, indent=2)
    return path


def main():
    parser = argparse.ArgumentParser(description="Desfaz restore: apaga tickets com [RESTORE] no notes")
    parser.add_argument("--dry-run", action="store_true", help="Mostra o que seria deletado, sem deletar")
    parser.add_argument("--force", action="store_true", help="Deleta sem pedir confirmacao (CUIDADO)")
    args = parser.parse_args()

    print("\n=== Buscando tickets com notes [RESTORE] ===\n")
    like_q = urllib.parse.quote("[RESTORE]*")
    res = sb_request("GET", f"tickets?notes=like.{like_q}&select=id,ticket,state,status,expire,client,notes")

    if not res:
        print("Nenhum ticket [RESTORE] encontrado. Nada a fazer.")
        return 0

    print(f"Encontrados: {len(res)} tickets criados pelo restore\n")
    for r in res[:30]:
        print(f"  {r['ticket']} | {r['state']} | {r['status']} | exp {r.get('expire', '')} | {(r.get('client') or '')[:30]}")
    if len(res) > 30:
        print(f"  ... e mais {len(res)-30}")

    if args.dry_run:
        print(f"\n[DRY-RUN] {len(res)} tickets seriam deletados. Nenhuma alteracao feita.")
        return 0

    print(f"\nVai DELETAR essas {len(res)} rows.")

    if not args.force:
        ans = input("Confirma? [s/N]: ").strip().lower()
        if ans != "s":
            print("Abortado.")
            return 0

    backup_path = save_pre_delete_backup(res)
    print(f"\nBackup salvo em: {backup_path}")

    ids = [str(r["id"]) for r in res]
    deleted = 0
    errors = 0
    for i in range(0, len(ids), 100):
        chunk = ids[i:i+100]
        in_list = ",".join(chunk)
        r = sb_request("DELETE", f"tickets?id=in.({urllib.parse.quote(in_list)})")
        if r is not None:
            deleted += len(chunk)
        else:
            errors += len(chunk)

    print(f"\n=== DONE === Deletados: {deleted}  Erros: {errors}")
    if errors:
        print(f"ATENCAO: {errors} rows nao foram deletadas. Verifique o backup em {backup_path}")
    print("Recarregue a pagina (Ctrl+Shift+R).")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
