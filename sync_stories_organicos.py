import requests
from datetime import datetime
from dotenv import load_dotenv
from supabase import create_client
import os

load_dotenv()

GRAPH_TOKEN = os.getenv("GRAPH_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_stories():
    url = (
        f"https://graph.facebook.com/v19.0/{IG_USER_ID}/stories"
        f"?fields=id,media_type,media_url,timestamp,permalink,thumbnail_url"
        f"&access_token={GRAPH_TOKEN}"
    )
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception(f"Erro na API: {res.text}")
    return res.json().get("data", [])

def get_metric(story_id, metric):
    url = f"https://graph.facebook.com/v19.0/{story_id}/insights?metric={metric}&access_token={GRAPH_TOKEN}"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"[falhou] Métrica {metric} para {story_id}: {res.json().get('error', {}).get('message')}")
        return None
    data = res.json().get("data", [])
    if data and "values" in data[0]:
        return int(data[0]["values"][0]["value"])
    return None

def story_existe(media_id):
    res = supabase.table("stories_organicos").select("media_id").eq("media_id", media_id).execute()
    return bool(res.data)

def salvar_story(story):
    media_id = story["id"]
    if story_existe(media_id):
        print(f"[ignorado] Já existe: {media_id}")
        return

    alcance = get_metric(media_id, "reach")
    impressoes = get_metric(media_id, "impressions")
    exits = get_metric(media_id, "exits")
    taps_forward = get_metric(media_id, "taps_forward")
    taps_back = get_metric(media_id, "taps_back")
    replies = get_metric(media_id, "replies")

    data = {
        "media_id": media_id,
        "tipo": story.get("media_type"),
        "data_publicacao": story.get("timestamp")[:10],
        "link": story.get("permalink"),
        "thumbnail": story.get("thumbnail_url") or story.get("media_url"),
        "alcance": alcance,
        "impressoes": impressoes,
        "exits": exits,
        "taps_forward": taps_forward,
        "taps_back": taps_back,
        "replies": replies,
        "tem_metricas": any([alcance, impressoes, exits, taps_forward, taps_back, replies]),
        "data_coleta": datetime.now().isoformat()
    }

    supabase.table("stories_organicos").insert(data).execute()
    print(f"[ok] Story salvo: {media_id}")

def main():
    print("🔗 Conectado à API. Buscando stories...")
    stories = get_stories()
    if not stories:
        print("Nenhum story encontrado.")
        return
    for story in stories:
        salvar_story(story)

if __name__ == "__main__":
    main()

