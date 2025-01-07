import requests
import csv
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Define the Python function to fetch and save cryptocurrency data
def fetch_crypto_data():
    url = "https://rest.coinapi.io/v1/assets"
    
    # Your CoinAPI key (replace with your actual API key)
    api_key = "0870F1ED-E9E9-45C2-9E4C-67348A1694C1"
    
    # Set the headers for authentication
    headers = {
        "X-CoinAPI-Key": api_key
    }
    
    # Send GET request to CoinAPI
    response = requests.get(url, headers=headers)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Create a timestamp to differentiate each file (optional)
        timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')

        df = pd.DataFrame(data)
        df = df[[
        "asset_id", "name", "type_is_crypto", "data_quote_start", "data_quote_end", "data_orderbook_start", "data_orderbook_end",
                   "data_trade_start", "data_trade_end", "data_symbols_count", "volume_1hrs_usd", "volume_1day_usd", "volume_1mth_usd",
                   "price_usd", "data_start", "data_end"
    ]]
        df['row_update_date'] = timestamp
        
        # Specify the path where you want to save the file
        directory_path = f"/opt/airflow/data/crypto_data_{timestamp}.csv"
        df.to_csv(directory_path, index=False)
        print("Data saved to the directory path")
        print(f"Saving file to: {directory_path}")
    else:
        print("Something is fishy")
        
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
    "crypto_data_etl",
    default_args=default_args,
    description="ETL DAG for cryptocurrency data",
    schedule_interval="0 12 */2 * *",  # At Noon Every 2 Days
    start_date=datetime(2025, 1, 4),
    catchup=False,
) as dag:

# Define the task to fetch and save cryptocurrency data to CSV
    extract_task = PythonOperator(
        task_id='fetch_crypto_data', 
        python_callable=fetch_crypto_data
    )

    # The DAG will only have this single task, but you can add more if needed
    extract_task
