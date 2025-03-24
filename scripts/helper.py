# scripts/helper.py
# import pandas as pd
# import os
# from datetime import datetime

# def save_to_csv(df, filename):
#     """Save DataFrame to a CSV file with a timestamp."""
#     timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
#     filepath = os.path.join("/opt/airflow/data", f"{filename}_{timestamp}.csv")
#     df.to_csv(filepath, index=False)
#     print(f"Data saved to: {filepath}")

import requests
import csv
import boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Use Airflow's S3Hook
import os
import logging

# Airflow Connection IDs

# Use the connection configured in Airflow UI
# AWS_CONN_ID = "aws_conn"  
# S3_BUCKET_NAME = "coingeckolist"
# S3_FILE_PATH = "crypto_coins_list.csv"

# # Function to fetch CoinGecko data and store it as CSV
# def fetch_coingecko_coins_list(**kwargs):
#     url = "https://api.coingecko.com/api/v3/coins/list"
#     response = requests.get(url)
    
#     if response.status_code == 200:
#         data = response.json()
#         temp_file = "/tmp/crypto_coins_list.csv"
        
#         # Writing to CSV
#         with open(temp_file, "w", newline='') as f:
#             writer = csv.writer(f)
#             writer.writerow(["id", "symbol", "name", "row_update_date"])  # Writing header
#             for coin in data:
#                 formatted_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Formats with milliseconds
#                 writer.writerow([coin["id"], coin["symbol"], coin["name"], formatted_time])
        
#         # Push file path to XCom
#         kwargs['ti'].xcom_push(key='crypto_data_file', value=temp_file)
#     else:
#         raise Exception("API request failed")

# Function to upload the file to S3 using Airflow S3Hook

def save_to_csv_coinapi(df, filename):

    AWS_CONN_ID = "aws_conn"  
    S3_BUCKET_NAME = "coingeckolist"
    S3_FILE_PATH = "coinapi_coins_desc.csv"

    # Define the local file path
    local_file_path = f"/tmp/{filename}.csv"

    # Save the DataFrame as a CSV file
    df.to_csv(local_file_path, index=False)
    print(f"CSV file saved locally at {local_file_path}")

    # Use Airflow's S3Hook to upload to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=local_file_path,  # Local file path
        bucket_name=S3_BUCKET_NAME,  # S3 bucket name
        key=S3_FILE_PATH,  # S3 file path
        replace=True  # Replace if file already exists
    )

    print(f"File successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")

def save_to_csv_coingecko(df, filename):

    AWS_CONN_ID = "aws_conn"  
    S3_BUCKET_NAME = "coingeckolist"
    S3_FILE_PATH = "coingecko.csv"

    # Define the local file path
    local_file_path = f"/tmp/{filename}.csv"

    # Save the DataFrame as a CSV file
    df.to_csv(local_file_path, index=False)
    print(f"CSV file saved locally at {local_file_path}")

    # Use Airflow's S3Hook to upload to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=local_file_path,  # Local file path
        bucket_name=S3_BUCKET_NAME,  # S3 bucket name
        key=S3_FILE_PATH,  # S3 file path
        replace=True  # Replace if file already exists
    )

    print(f"File successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")

def save_to_csv_coingecko_list(df, filename):

    AWS_CONN_ID = "aws_conn"  
    S3_BUCKET_NAME = "coingeckolist"
    S3_FILE_PATH = "coingecko_coins_list.csv"

    # Define the local file path
    local_file_path = f"/tmp/{filename}.csv"

    # Save the DataFrame as a CSV file
    df.to_csv(local_file_path, index=False)
    print(f"CSV file saved locally at {local_file_path}")

    # Use Airflow's S3Hook to upload to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=local_file_path,  # Local file path
        bucket_name=S3_BUCKET_NAME,  # S3 bucket name
        key=S3_FILE_PATH,  # S3 file path
        replace=True  # Replace if file already exists
    )

    print(f"File successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")

def save_to_csv_coinmarketcap(df, filename):

    AWS_CONN_ID = "aws_conn"  
    S3_BUCKET_NAME = "coingeckolist"
    S3_FILE_PATH = "coinmarketcap.csv"

    # Define the local file path
    local_file_path = f"/tmp/{filename}.csv"

    # Save the DataFrame as a CSV file
    df.to_csv(local_file_path, index=False)
    print(f"CSV file saved locally at {local_file_path}")

    # Use Airflow's S3Hook to upload to S3
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    s3_hook.load_file(
        filename=local_file_path,  # Local file path
        bucket_name=S3_BUCKET_NAME,  # S3 bucket name
        key=S3_FILE_PATH,  # S3 file path
        replace=True  # Replace if file already exists
    )

    print(f"File successfully uploaded to S3: s3://{S3_BUCKET_NAME}/{S3_FILE_PATH}")