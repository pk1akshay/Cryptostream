# scripts/coingecko.py
import requests
import pandas as pd
from .helper import save_to_csv
from datetime import datetime, timedelta, timezone
from config.settings import COINGECKO_API_URL, SNOWFLAKE_TABLE, S3_STAGE_NAME, S3_FILE_PATH1,S3_FILE_PATH2, S3_STAGE_NAME_COIN_GECKO, SNOWFLAKE_TABLE_COINGECKO


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
    pst_offset = timezone(timedelta(hours=-7))
    pst_time = datetime.now(pst_offset)
    df = df[[
        "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
        "total_volume", "high_24h", "low_24h", "price_change_24h",
        "price_change_percentage_24h", "circulating_supply", "total_supply",
        "max_supply", "ath", "ath_change_percentage"
    ]]
    df['row_insert_date'] = pst_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    save_to_csv(df, filename="coingecko_market_data", S3_FILE_PATH = "coingecko_coins_list.csv")

def fetch_coingecko_coins_list():
    url = f"{COINGECKO_API_URL}/coins/list"
    response = requests.get(url)
    
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        pst_offset = timezone(timedelta(hours=-7))
        pst_time = datetime.now(pst_offset)
        df['row_insert_date'] = pst_time.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        save_to_csv(df, filename="coingecko_coins_list", S3_FILE_PATH = "coingecko.csv")
    else:
        print(f"Error fetching coins list: {response.status_code}")

# Function to create stage and table in Snowflake
CREATE_STAGE_SQL = f'''
    CREATE OR REPLACE STAGE {S3_STAGE_NAME}
    URL = 's3://coingeckolist/coingecko.csv' 
    STORAGE_INTEGRATION = snowflake_crypto_si;
'''

CREATE_TABLE_SQL = f'''
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        id STRING,
        symbol STRING,
        name STRING,
        row_insert_date TIMESTAMP
    );
'''

# Function to load data from S3 into Snowflake
COPY_INTO_SQL = f'''
    COPY INTO {SNOWFLAKE_TABLE}
FROM @{S3_STAGE_NAME}
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';

   '''
# Function to create stage and table in Snowflake
CREATE_STAGE_SQL_COIN_GECKO = f'''
    CREATE OR REPLACE STAGE {S3_STAGE_NAME_COIN_GECKO}
    URL = 's3://coingeckolist/coingecko_coins_list.csv' 
    STORAGE_INTEGRATION = snowflake_crypto_si;
'''

CREATE_TABLE_SQL_COIN_GECKO = f'''
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE_COINGECKO} (
        id STRING,
        symbol STRING,
        name STRING,
        current_price FLOAT,
        market_cap FLOAT,
        market_cap_rank INTEGER,
        total_volume FLOAT,
        high_24h FLOAT,
        low_24h FLOAT,
        price_change_24h FLOAT,
        price_change_percentage_24h FLOAT,
        circulating_supply FLOAT,
        total_supply FLOAT,
        max_supply FLOAT,
        ath FLOAT,
        ath_change_percentage FLOAT,
        row_insert_date TIMESTAMP
    );
'''

# Function to load data from S3 into Snowflake
COPY_INTO_SQL_COIN_GECKO = f'''
    COPY INTO {SNOWFLAKE_TABLE_COINGECKO}
FROM @{S3_STAGE_NAME_COIN_GECKO}
FILE_FORMAT = (
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
)
ON_ERROR = 'CONTINUE';

   '''