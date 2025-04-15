# scripts/coinapi.py
import sys
import os
import requests
import pandas as pd
from .helper import save_to_csv
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.settings import COINAPI_API_KEY

def fetch_coin_api_data():
    url = "https://rest.coinapi.io/v1/assets"
    headers = {"X-CoinAPI-Key": COINAPI_API_KEY}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        print(f"Hurray!!")
        df = pd.DataFrame(response.json())
        df = df[[
            "asset_id", "name", "type_is_crypto", "data_quote_start", "data_quote_end",
            "data_orderbook_start", "data_orderbook_end", "data_trade_start",
            "data_trade_end", "data_symbols_count", "volume_1hrs_usd", 
            "volume_1day_usd", "volume_1mth_usd", "price_usd", "data_start", "data_end"
        ]]
        save_to_csv(df, filename = "crypto_coinapi_data", S3_FILE_PATH = "coinapi_coins_desc.csv")
        print(f"Hurray!!")
    else:
        print(f"Error fetching data: {response.status_code}")