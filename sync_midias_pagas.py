import os
import requests
import datetime
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# 🔑 Variáveis de ambiente
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
GRAPH_TOKEN = os.getenv("GRAPH_TOKEN")
AD_ACCOUNT_ID = os.getenv("AD_ACCOUNT_ID")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def registro_igual_existente(id_ad, valor_total, data_fim):
    try:
        result = supabase.table("midias_pagas").select("valor_total, data_fim").eq("id_ad", id_ad).execute()
        if result.data:
            r = result.data[0]
            return float(r["valor_total"]) == float(valor_total) and r["data_fim"] == data_fim
    except Exception as e:
        print(f"❌ Erro ao verificar duplicidade: {e}")
    return False

def salvar(payload, houve_alteracao):
    if houve_alteracao:
        payload["atualizado_em"] = datetime.datetime.now().isoformat()
    try:
        supabase.table("midias_pagas").upsert(payload, on_conflict="id_ad").execute()
        return True
    except Exception as e:
        print(f"❌ Erro ao salvar {payload.get('id_ad')}: {e}")
        return False

print("\n🔍 Buscando criativos com entrega...")
res = requests.get(
    f"https://graph.facebook.com/v20.0/act_{AD_ACCOUNT_ID}/ads",
    params={
        "fields": "id,name,adcreatives{thumbnail_url,object_story_spec},insights{date_start,date_stop,impressions,reach,clicks,spend,cpc,actions}",
        "limit": 500,
        "date_preset": "last_30d",
        "access_token": GRAPH_TOKEN
    }
)

if res.status_code != 200:
    print("❌ Erro na API:", res.text)
    exit()

data = res.json().get("data", [])
print(f"\n📦 Total de criativos encontrados: {len(data)}\n")

salvos = 0
ignorados = 0

for ad in data:
    creative = ad.get("adcreatives", {}).get("data", [{}])[0]
    insight = ad.get("insights", {}).get("data", [{}])[0]
    actions = insight.get("actions", [])
    resultado = next((a for a in actions if a.get("action_type") in ["link_click", "post_engagement", "video_view"]), {})

    teve_entrega = int(insight.get("impressions", 0)) > 0 and float(insight.get("spend", 0)) > 0
    if not teve_entrega:
        print("⛔ Ignorado (sem entrega):", ad.get("id"))
        ignorados += 1
        continue

    payload = {
        "id_ad": ad.get("id"),
        "nome": ad.get("name"),
        "formato": 'STORY' if creative.get('object_story_spec', {}).get('instagram_story_id') else 'FEED',
        "id_post": creative.get('object_story_spec', {}).get('instagram_story_id'),
        "link": creative.get('object_story_spec', {}).get('link_data', {}).get('link'),
        "thumbnail": creative.get("thumbnail_url"),
        "resultado_tip": resultado.get("action_type"),
        "resultados": int(resultado.get("value", 0)),
        "alcance": int(insight.get("reach", 0)),
        "impressoes": int(insight.get("impressions", 0)),
        "custo_por_resultado": float(insight.get("cpc", 0)),
        "valor_total": float(insight.get("spend", 0)),
        "data_inicio": insight.get("date_start"),
        "data_fim": insight.get("date_stop"),
        "data_coleta": datetime.datetime.now().isoformat()
    }

    houve_alteracao = not registro_igual_existente(payload["id_ad"], payload["valor_total"], payload["data_fim"])
    
    if salvar(payload, houve_alteracao):
        if houve_alteracao:
            salvos += 1
            print(f"✅ Atualizado: {payload['id_ad']}")
        else:
            ignorados += 1
            print(f"↪️ Sem mudança: {payload['id_ad']}")

print(f"\n✅ Coleta e sincronização finalizada. Salvos: {salvos} | Ignorados: {ignorados}\n")
