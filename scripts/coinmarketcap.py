# scripts/coinmarketcap.py
import requests
import pandas as pd
from .helper import save_to_csv_coinmarketcap
from config.settings import COINMARKETCAP_API_KEY

def fetch_crypto_ids():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
    
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return [str(coin["id"]) for coin in response.json()["data"]]
    else:
        print(f"Error fetching IDs: {response.status_code} - {response.text}")
        return None

def fetch_all_coins_info():
    coin_ids = fetch_crypto_ids()
    if not coin_ids:
        return

    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
    headers = {"X-CMC_PRO_API_KEY": COINMARKETCAP_API_KEY}
    batch_size = 100
    all_data = []

    for i in range(0, len(coin_ids), batch_size):
        response = requests.get(url, params={"id": ",".join(coin_ids[i:i+batch_size])}, headers=headers)
        if response.status_code == 200:
            all_data.extend(response.json()["data"].values())
        else:
            print(f"Error fetching batch {i}-{i+batch_size}: {response.status_code}")
    
    df = pd.DataFrame(all_data)
    save_to_csv_coinmarketcap(df, filename="coinmarketcap_all_coins_info")
