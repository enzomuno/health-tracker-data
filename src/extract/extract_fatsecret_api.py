import time
import uuid
import hmac
import hashlib
import base64
import urllib.parse as urlparse
import requests
from dotenv import load_dotenv
import os
from datetime import datetime, timezone


# Carregar as variáveis do .env
load_dotenv()
CONSUMER_KEY = os.getenv("FAT_SECRET_CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("FAT_SECRET_CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("FAT_SECRET_OAUTH_TOKEN")
ACCESS_SECRET = os.getenv("FAT_SECRET_OAUTH_SECRET")

print("🔍 [DEBUG] CHAVES LIDAS:")
print("FAT_SECRET_CONSUMER_KEY:", CONSUMER_KEY)
print("FAT_SECRET_CONSUMER_SECRET:", CONSUMER_SECRET)
print("FAT_SECRET_OAUTH_TOKEN:", ACCESS_TOKEN)
print("FAT_SECRET_OAUTH_SECRET:", ACCESS_SECRET)



# Definição da chamada a API
BASE_URL = "https://platform.fatsecret.com/rest/server.api"
HTTP_METHOD = "GET"
OAUTH_VERSION = "1.0"
SIGNATURE_METHOD = "HMAC-SHA1"


# Função para transformar a data de hoje para formato epoch
def get_fatsecret_epoch_days(date: datetime = None) -> int:
    """
    Retorna o número de dias desde 1º de janeiro de 1970 até a data especificada (em UTC).
    """
    if date is None:
        date = datetime.now(timezone.utc)

    epoch_start = datetime(1970, 1, 1, tzinfo=timezone.utc)
    delta = date - epoch_start
    return delta.days

# --- Geração de assinatura OAuth 1.0 ---
def generate_oauth_signature(params: dict, base_url: str) -> str:
    sorted_items = sorted(params.items())
    encoded_params = urlparse.urlencode(sorted_items, quote_via=urlparse.quote)

    base_string = "&".join([
        HTTP_METHOD,
        urlparse.quote(base_url, safe=''),
        urlparse.quote(encoded_params, safe='')
    ])

    signing_key = f"{urlparse.quote(CONSUMER_SECRET)}&{urlparse.quote(ACCESS_SECRET)}"

    return base64.b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
    ).decode()

# --- Geração dos parâmetros OAuth obrigatórios ---
def get_oauth_params() -> dict:
    return {
        "oauth_consumer_key": CONSUMER_KEY,
        "oauth_token": ACCESS_TOKEN,
        "oauth_nonce": uuid.uuid4().hex,
        "oauth_signature_method": SIGNATURE_METHOD,
        "oauth_timestamp": str(int(time.time())),
        "oauth_version": OAUTH_VERSION
    }

# --- Função genérica para consultar qualquer endpoint da FatSecret ---
def fatsecret_request(api_params: dict) -> dict:
    oauth_params = get_oauth_params()
    all_params = {**api_params, **oauth_params}
    oauth_signature = generate_oauth_signature(all_params, BASE_URL)

    final_params = {**api_params, **oauth_params, "oauth_signature": oauth_signature}
    response = requests.get(BASE_URL, params=final_params)

    try:
        return response.json()
    except Exception as e:
        return {"error": f"Erro ao parsear resposta: {e}", "raw_response": response.text}


def get_weight_entries():
    today_date = get_fatsecret_epoch_days()
    api_params = {
        'method':'weights.get_month.v2',
        'format':'json',
        'date':today_date
    }
    weight_entries = fatsecret_request(api_params)
    
    return weight_entries


def get_food_entries():
    today_date = get_fatsecret_epoch_days()
    api_params = {
        'method':'food_entries.get_month.v2',
        'format':'json',
        'date':today_date
    }
    food_entries = fatsecret_request(api_params)
    
    return food_entries


    