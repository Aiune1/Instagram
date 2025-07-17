import os
import requests
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

GRAPH_TOKEN     = os.getenv("GRAPH_TOKEN")
IG_USER_ID      = os.getenv("IG_USER_ID")
SUPABASE_URL    = os.getenv("SUPABASE_URL")
SUPABASE_KEY    = os.getenv("SUPABASE_KEY")

def buscar_registro_existente(post_id):
    try:
        res = requests.get(
            f"{SUPABASE_URL}/rest/v1/midias_organicas",
            params={"id_post": f"eq.{post_id}"},
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            }
        )
        if res.status_code == 200 and res.json():
            return res.json()[0]
        return None
    except Exception as e:
        print("⚠️ Erro ao buscar registro existente:", str(e))
        return None

def salvar(dados, houve_alteracao):
    if houve_alteracao:
        dados["atualizado_em"] = datetime.utcnow().isoformat()

    try:
        res = requests.post(
            f"{SUPABASE_URL}/rest/v1/midias_organicas",
            json=[dados],
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json",
                "Prefer": "resolution=merge-duplicates"
            }
        )
        if res.status_code in [200, 201]:
            print("✅ Salvo:", dados["id_post"])
        else:
            print("❌ Erro ao salvar:", res.text)
    except Exception as e:
        print("❌ Erro inesperado:", str(e))

def puxar_insights(post_id):
    try:
        res = requests.get(
            f"https://graph.facebook.com/v20.0/{post_id}/insights",
            params={
                "metric": "reach,likes,comments,saved",
                "access_token": GRAPH_TOKEN
            }
        )
        data = res.json().get("data", [])
        return {item["name"]: item["values"][0]["value"] for item in data}
    except Exception as e:
        print(f"⚠️ Sem insights para {post_id}: {str(e)}")
        return None

def puxar_posts():
    try:
        res = requests.get(
            f"https://graph.facebook.com/v20.0/{IG_USER_ID}/media",
            params={
                "fields": "id,media_type,timestamp,caption,permalink,thumbnail_url",
                "limit": 50,
                "access_token": GRAPH_TOKEN
            }
        )
        return res.json().get("data", [])
    except Exception as e:
        print("❌ Erro ao buscar posts:", str(e))
        return []

def executar():
    print("🔍 Buscando posts orgânicos...")
    posts = puxar_posts()
    print(f"📦 Total de posts: {len(posts)}")

    salvos = 0
    ignorados = 0

    for p in posts:
        insights = puxar_insights(p["id"])
        if not insights:
            continue

        dados = {
            "id_post":      p["id"],
            "nome":         p.get("caption", "")[:100],
            "tipo":         p.get("media_type", ""),
            "data":         p.get("timestamp"),
            "link":         p.get("permalink", ""),
            "thumbnail":    p.get("thumbnail_url", ""),
            "alcance":      insights.get("reach", 0),
            "curtidas":     insights.get("likes", 0),
            "comentarios":  insights.get("comments", 0),
            "salvamentos":  insights.get("saved", 0),
            "data_coleta":  datetime.utcnow().isoformat()
        }

        existente = buscar_registro_existente(p["id"])
        if existente:
            houve_alteracao = any([
                existente.get("alcance", 0) != dados["alcance"],
                existente.get("curtidas", 0) != dados["curtidas"],
                existente.get("comentarios", 0) != dados["comentarios"],
                existente.get("salvamentos", 0) != dados["salvamentos"]
            ])
        else:
            houve_alteracao = True

        salvar(dados, houve_alteracao)

        if houve_alteracao:
            salvos += 1
        else:
            ignorados += 1
            print("↪️ Sem mudança:", dados["id_post"])

    print(f"\n✅ Coleta finalizada. Salvos: {salvos} | Ignorados: {ignorados}\n")

if __name__ == "__main__":
    executar()
