import requests
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timezone
import logging


# Snowflake connection details (Airflow UI should have a connection named 'snowflake_conn')
SNOWFLAKE_CONN_ID = 'snowflake_conn'
TABLE_NAME = "dim_crypto_data"

# Function to fetch data from CoinAPI
def fetch_coingecko_coins_list(**kwargs):
    url = "https://api.coingecko.com/api/v3/coins/list"
    
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        # Push the data to XCom for downstream tasks
        kwargs['ti'].xcom_push(key='coin_gecko_list', value=json.dumps(data))  
    else:
        raise Exception("API request failed")

# Function to insert data into Snowflake
def insert_data_into_snowflake(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_coingecko_coins_list', key='coin_gecko_list')

    if not data:
        raise ValueError("No data received from CoinAPI")

    # Convert JSON data to Python list
    coin_data = json.loads(data)

    # Prepare data for insertion
    records = []
    for coin in coin_data:
        records.append((
            coin.get("id"),
            coin.get("symbol"),
            coin.get("name"),
            datetime.now(timezone.utc).isoformat() # Convert datetime to string using isoformat()
        ))

    # Log the prepared records for debugging
    logging.info("Prepared records for insertion: %s", json.dumps(records, indent=2))

    # Connect to Snowflake
    snowflake_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Insert data into Snowflake table
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        id, symbol, name, row_update_date
    ) VALUES (%s, %s, %s, %s);
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
    "crypto_coins_list",
    default_args=default_args,
    description="ETL DAG for cryptocurrency coins list loading to snowflake",
    schedule_interval="0 12 */2 * *",
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    fetch_coinapi_data = PythonOperator(
        task_id='fetch_coingecko_coins_list',
        python_callable=fetch_coingecko_coins_list,
        provide_context=True
    )

    create_table = SnowflakeOperator(
        task_id='create_table',
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id STRING,
            symbol STRING,
            name STRING,
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
