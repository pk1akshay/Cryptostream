import requests
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging


# Snowflake connection details (Airflow UI should have a connection named 'snowflake_conn')
SNOWFLAKE_CONN_ID = 'snowflake_conn'
TABLE_NAME = "crypto_data"

# Function to fetch data from CoinAPI
def fetch_coin_api_data(**kwargs):
    url = "https://rest.coinapi.io/v1/assets"
    api_key = "0870F1ED-E9E9-45C2-9E4C-67348A1694C1"
    headers = {"X-CoinAPI-Key": api_key}
    
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        # Push the data to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='coin_api_data', value=json.dumps(data))  
    else:
        raise Exception("API request failed")

# Function to insert data into Snowflake
def insert_data_into_snowflake(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_coin_api_data', key='coin_api_data')

    if not data:
        raise ValueError("No data received from CoinAPI")

    # Convert JSON data to Python list
    coin_data = json.loads(data)

    # Prepare data for insertion
    records = []
    for coin in coin_data:
        records.append((
            coin.get("asset_id"),
            coin.get("name"),
            coin.get("type_is_crypto"),
            coin.get("data_quote_start"),
            coin.get("data_quote_end"),
            coin.get("data_orderbook_start"),
            coin.get("data_orderbook_end"),
            coin.get("data_trade_start"),
            coin.get("data_trade_end"),
            int(coin.get("data_symbols_count", 0)),  # Default to 0 if None
            float(coin.get("volume_1hrs_usd", 0.0)),  # Default to 0.0 if None
            float(coin.get("volume_1day_usd", 0.0)),  # Default to 0.0 if None
            float(coin.get("volume_1mth_usd", 0.0)),  # Default to 0.0 if None
            float(coin.get("price_usd", 0.0)),  # Default to 0.0 if None
            coin.get("data_start"),
            coin.get("data_end"),
            datetime.utcnow().isoformat()  # Convert datetime to string using isoformat()
        ))

    # Log the prepared records for debugging
    logging.info("Prepared records for insertion: %s", json.dumps(records, indent=2))

    # Connect to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Insert data into Snowflake table
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        asset_id, name, type_is_crypto, data_quote_start, data_quote_end, 
        data_orderbook_start, data_orderbook_end, data_trade_start, data_trade_end, 
        data_symbols_count, volume_1hrs_usd, volume_1day_usd, volume_1mth_usd, 
        price_usd, data_start, data_end, row_update_date
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    # Ensure that records is passed correctly as a list of tuples
    for record in records:
        snowflake_hook.run(insert_query, parameters=record)

# Default arguments for DAG
default_args = {
    "owner": "Akshay",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    "crypto_data_etl_snowflake",
    default_args=default_args,
    description="ETL DAG for cryptocurrency data with Snowflake loading",
    schedule_interval="0 12 */2 * *",
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    fetch_coinapi_data = PythonOperator(
        task_id='fetch_coin_api_data',
        python_callable=fetch_coin_api_data,
        provide_context=True
    )

    create_table = SnowflakeOperator(
        task_id='create_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            asset_id STRING,
            name STRING,
            type_is_crypto BOOLEAN,
            data_quote_start TIMESTAMP,
            data_quote_end TIMESTAMP,
            data_orderbook_start TIMESTAMP,
            data_orderbook_end TIMESTAMP,
            data_trade_start TIMESTAMP,
            data_trade_end TIMESTAMP,
            data_symbols_count INT,
            volume_1hrs_usd FLOAT,
            volume_1day_usd FLOAT,
            volume_1mth_usd FLOAT,
            price_usd FLOAT,
            data_start TIMESTAMP,
            data_end TIMESTAMP,
            row_update_date TIMESTAMP
        )
        """
    )

    insert_data = PythonOperator(
        task_id='insert_data_into_snowflake',
        python_callable=insert_data_into_snowflake,
        provide_context=True
    )

    fetch_coinapi_data >> create_table >> insert_data
