import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta

# Snowflake Connection ID
SNOWFLAKE_CONN_ID = "snowflake_conn"
DBT_CLOUD_CONN_ID = "dbt_conn"
DBT_JOB_ID = 70471823430549  # Replace with your actual dbt Cloud job ID
S3_STAGE_NAME = "CRYPTO_DATA.PUBLIC.crypto_stage"
SNOWFLAKE_TABLE = "CRYPTO_DATA.PUBLIC.crypto_coins_list"
S3_FILE_PATH = "coingecko.csv"

# Function to create stage and table in Snowflake
CREATE_STAGE_TABLE_SQL = f'''
    CREATE OR REPLACE STAGE {S3_STAGE_NAME}
    URL = 's3://coingeckolist' 
    STORAGE_INTEGRATION = snowflake_crypto_si;

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

# Default DAG arguments
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
    "ingest_crypto_data_to_snowflake",
    default_args=default_args,
    description="Ingest crypto data from S3 into Snowflake",
    schedule_interval="0 14 */2 * *",
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    create_stage_table = SnowflakeOperator(
        task_id="create_snowflake_stage_table",
        sql=CREATE_STAGE_TABLE_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    load_data = SnowflakeOperator(
        task_id="load_data_into_snowflake",
        sql=COPY_INTO_SQL,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    run_dbt_models = DbtCloudRunJobOperator(
        task_id="run_dbt_models",
        dbt_cloud_conn_id=DBT_CLOUD_CONN_ID,
        job_id=DBT_JOB_ID,
        check_interval=30,  # Check status every 30 seconds
        timeout=600,  # Timeout after 10 minutes
    )


    create_stage_table >> load_data >> run_dbt_models