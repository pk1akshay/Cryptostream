import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow/scripts")


from scripts.coinapi import fetch_coin_api_data
from scripts.coingecko import fetch_coingecko_data, fetch_coingecko_coins_list
from scripts.coinmarketcap import fetch_all_coins_info

# Default DAG arguments
default_args = {
    "owner": "Akshay",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "crypto_data_etl",
    default_args=default_args,
    description="ETL DAG for cryptocurrency data",
    schedule_interval="0 12 */2 * *",
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    fetch_coinapi_task = PythonOperator(
        task_id='fetch_coin_api_data',
        python_callable=fetch_coin_api_data
    )

    fetch_coingecko_task = PythonOperator(
        task_id='fetch_coingecko_data',
        python_callable=fetch_coingecko_data
    )

    fetch_coindata_list_task = PythonOperator(
        task_id='fetch_coingecko_list',
        python_callable=fetch_coingecko_coins_list
    )

    fetch_all_coins_info_task = PythonOperator(
        task_id="fetch_all_coins_info",
        python_callable=fetch_all_coins_info
    )

    fetch_coinapi_task >> fetch_coingecko_task >> fetch_all_coins_info_task
