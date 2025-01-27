from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
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
    dag_id="test_snowflake_connection",
    default_args=default_args,
    description="Test connection between Airflow and Snowflake",
    schedule_interval=None,  # Trigger manually
    start_date=datetime(2025, 1, 22),
    catchup=False,
) as dag:

    # Task: Test Snowflake connection
    test_snowflake = SnowflakeOperator(
        task_id="test_snowflake",
        snowflake_conn_id="snowflake_conn",  # Make sure this matches your Airflow UI connection ID
        sql="SELECT CURRENT_VERSION();",
    )

    test_snowflake
