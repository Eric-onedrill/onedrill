"""
desfazer_restore.py - Apaga as rows criadas por verificar_ticket_antigo.py --all-missing

Identifica pelo prefixo [RESTORE] no campo notes que foi inserido pelo script.
Mostra primeiro o que vai apagar (dry-run), depois pergunta confirmacao.
"""
import sys
import os
import json
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


print("\n=== Buscando tickets com notes [RESTORE] ===\n")
# PostgREST: notes LIKE '[RESTORE]%'
like_q = urllib.parse.quote("[RESTORE]*")
res = sb_request("GET", f"tickets?notes=like.{like_q}&select=id,ticket,state,status,expire,client,notes")

if not res:
    print("Nenhum ticket [RESTORE] encontrado. Nada a fazer.")
    sys.exit(0)

print(f"Encontrados: {len(res)} tickets criados pelo restore\n")
for r in res[:30]:
    print(f"  {r['ticket']} | {r['state']} | {r['status']} | exp {r['expire']} | {(r.get('client') or '')[:30]}")
if len(res) > 30:
    print(f"  ... e mais {len(res)-30}")

print(f"\nVai DELETAR essas {len(res)} rows.")
ans = input("Confirma? [s/N]: ").strip().lower()
if ans != "s":
    print("Abortado.")
    sys.exit(0)

ids = [str(r["id"]) for r in res]
# Delete em chunks de 100
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
print("Recarregue a pagina (Ctrl+Shift+R).")
