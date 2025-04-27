import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from scripts import fetch_coin_api_data
from scripts import fetch_coingecko_data, fetch_coingecko_coins_list, CREATE_STAGE_SQL, CREATE_TABLE_SQL, COPY_INTO_SQL, CREATE_STAGE_SQL_COIN_GECKO, CREATE_TABLE_SQL_COIN_GECKO, COPY_INTO_SQL_COIN_GECKO
#from scripts import fetch_all_coins_info

SNOWFLAKE_CONN_ID = "snowflake_conn"

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

    create_stage_table = SnowflakeOperator(
        task_id="create_snowflake_stage_table",
        sql = CREATE_STAGE_SQL,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    create_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql = CREATE_TABLE_SQL,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    load_data = SnowflakeOperator(
        task_id="load_data_into_snowflake",
        sql = COPY_INTO_SQL,
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
    )

    create_stage_table_cg = SnowflakeOperator(
        task_id="create_snowflake_stage_table_cg",
        sql = CREATE_STAGE_SQL_COIN_GECKO,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    create_table_cg = SnowflakeOperator(
        task_id="create_snowflake_table_cg",
        sql = CREATE_TABLE_SQL_COIN_GECKO,
        snowflake_conn_id= SNOWFLAKE_CONN_ID,
    )

    load_data_cg = SnowflakeOperator(
        task_id="load_data_into_snowflake_cg",
        sql = COPY_INTO_SQL_COIN_GECKO,
        snowflake_conn_id = SNOWFLAKE_CONN_ID,
    )   


    # fetch_all_coins_info_task = PythonOperator(
    #     task_id="fetch_all_coins_info",
    #     python_callable=fetch_all_coins_info
    # )

    fetch_coinapi_task >> fetch_coingecko_task >> fetch_coindata_list_task >> create_stage_table >> create_table >> load_data >> create_stage_table_cg >> create_table_cg >> load_data_cg 
    # >> fetch_all_coins_info_task
