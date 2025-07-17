import requests
from datetime import date, datetime
from supabase import create_client
from dotenv import load_dotenv
import os

load_dotenv()

# VARIÁVEIS DE AMBIENTE
GRAPH_TOKEN = os.getenv("GRAPH_TOKEN")
IG_USER_ID = os.getenv("IG_USER_ID")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# CLIENTE SUPABASE
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# CHAMADA À API DO INSTAGRAM
def get_total_seguidores():
    url = f"https://graph.facebook.com/v19.0/{IG_USER_ID}?fields=followers_count&access_token={GRAPH_TOKEN}"
    res = requests.get(url)
    if res.status_code != 200:
        raise Exception(f"Erro na API: {res.text}")
    return res.json().get("followers_count", 0)

# CONSULTA ÚLTIMO REGISTRO DE DIA ANTERIOR
def get_ontem_registro():
    hoje = date.today().isoformat()
    res = supabase.table("seguidores").select("*").lt("data", hoje).order("data", desc=True).limit(1).execute()
    if res.data:
        return res.data[0]
    return None

# MAIN
def salvar_total_seguidores():
    hoje = date.today()
    coletado_em = datetime.now()
    seguidores_atuais = get_total_seguidores()
    ontem = get_ontem_registro()

    variacao = None
    follows = None
    unfollows = None

    if ontem:
        variacao = seguidores_atuais - ontem["seguidores"]
        if variacao > 0:
            follows = variacao
            unfollows = 0
        elif variacao < 0:
            follows = 0
            unfollows = abs(variacao)
        else:
            follows = unfollows = 0

    dados = {
        "data": hoje.isoformat(),
        "seguidores": seguidores_atuais,
        "variacao": variacao,
        "follows": follows,
        "unfollows": unfollows,
        "coletado_em": coletado_em.isoformat(),
    }

    supabase.table("seguidores").upsert(dados, on_conflict=["data"]).execute()
    print(f"Seguidores atualizados para {seguidores_atuais} no dia {hoje}")

if __name__ == "__main__":
    salvar_total_seguidores()

