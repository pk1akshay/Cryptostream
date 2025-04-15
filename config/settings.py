# config/settings.py
COINMARKETCAP_API_KEY = "04fcaff7-2831-4f9e-8a64-abcc060265da"
COINAPI_API_KEY = "0870F1ED-E9E9-45C2-9E4C-67348A1694C1"
COINGECKO_API_URL = "https://api.coingecko.com/api/v3"
DATA_DIR = "/opt/airflow/data/"
SNOWFLAKE_CONN_ID = "snowflake_conn"
DBT_CLOUD_CONN_ID = "dbt_conn"
DBT_JOB_ID = 70471823430549
S3_STAGE_NAME = "CRYPTO_DATA.PUBLIC.crypto_stage"
SNOWFLAKE_TABLE = "CRYPTO_DATA.PUBLIC.crypto_coins_list"
S3_FILE_PATH = "coingecko.csv" 