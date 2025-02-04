import requests
import json
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Use Airflow's S3Hook
import os
import logging

# Airflow Connection IDs
AWS_CONN_ID = "aws_conn"  # Use the connection configured in Airflow UI
S3_BUCKET_NAME = "coingeckolist"
S3_FILE_PATH = "crypto_coins_list.csv"

# Function to fetch CoinGecko data
def fetch_coingecko_coins_list(**kwargs):
    url = "https://api.coingecko.com/api/v3/coins/list"
    
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()

        # Convert data to JSON string
        json_data = json.dumps(data, indent=4)

        # Save JSON data to a temporary file
        temp_file = "/tmp/crypto_coins_list.csv"
        with open(temp_file, "w") as f:
            f.write(json_data)
        
        # Push file path to XCom
        kwargs['ti'].xcom_push(key='crypto_data_file', value=temp_file)
    
    else:
        raise Exception("API request failed")

# Function to upload the file to S3 using Airflow S3Hook
def upload_to_s3(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='fetch_coingecko_coins_list', key='crypto_data_file')

    if not file_path or not os.path.exists(file_path):
        raise ValueError("No valid file found to upload to S3")

    # Use Airflow's S3Hook to get AWS credentials
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # Upload file to S3
    s3_hook.load_file(filename=file_path, bucket_name=S3_BUCKET_NAME, key=S3_FILE_PATH, replace=True)
    logging.info(f"File uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")

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
    "crypto_coins_list_to_s3",
    default_args=default_args,
    description="ETL DAG for cryptocurrency coins list loading to S3",
    schedule_interval="0 12 */2 * *",
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

    fetch_coinapi_data = PythonOperator(
        task_id='fetch_coingecko_coins_list',
        python_callable=fetch_coingecko_coins_list,
        provide_context=True
    )

    upload_file_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True
    )

    fetch_coinapi_data >> upload_file_to_s3
