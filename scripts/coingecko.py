# scripts/coingecko.py
import requests
import pandas as pd
from .helper import save_to_csv
from config.settings import COINGECKO_API_URL, SNOWFLAKE_TABLE, S3_STAGE_NAME, S3_FILE_PATH


def fetch_coingecko_data():
    url = f"{COINGECKO_API_URL}/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 250,
        "page": 1,
        "sparkline": False
    }
    
    all_data = []
    for page in range(1, 5):
        params["page"] = page
        response = requests.get(url, params=params)
        if response.status_code == 200:
            all_data.extend(response.json())
        else:
            print(f"Error fetching page {page}: {response.status_code}")
            break

    df = pd.DataFrame(all_data)
    df = df[[
        "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
        "total_volume", "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "circulating_supply", "total_supply",
        "max_supply", "ath", "ath_change_percentage"
    ]]
    save_to_csv(df, filename="coingecko_market_data", S3_FILE_PATH = "coingecko_coins_list.csv")

def fetch_coingecko_coins_list():
    url = f"{COINGECKO_API_URL}/coins/list"
    response = requests.get(url)
    
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        save_to_csv(df, filename="coingecko_coins_list", S3_FILE_PATH = "coingecko.csv")
    else:
        print(f"Error fetching coins list: {response.status_code}")

# Function to create stage and table in Snowflake
CREATE_STAGE_SQL = f'''
    CREATE OR REPLACE STAGE {S3_STAGE_NAME}
    URL = 's3://coingeckolist' 
    STORAGE_INTEGRATION = snowflake_crypto_si;
'''

CREATE_TABLE_SQL = f'''
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        id STRING,
        symbol STRING,
        name STRING,
        row_update_date TIMESTAMP
    );
'''

# Function to load data from S3 into Snowflake
COPY_INTO_SQL = f'''
    COPY INTO {SNOWFLAKE_TABLE}
    FROM @{S3_STAGE_NAME}
    FILES = ('{S3_FILE_PATH}')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
    ON_ERROR = CONTINUE;
   '''
