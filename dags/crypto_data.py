import requests
import csv
import os
import time
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



# Function to fetch all cryptocurrency IDs
def fetch_crypto_ids():
    url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
    API_KEY = "04fcaff7-2831-4f9e-8a64-abcc060265da"
    headers = {"X-CMC_PRO_API_KEY": API_KEY}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        coin_ids = [str(coin["id"]) for coin in data["data"]]  # Extract IDs
        return coin_ids
    else:
        print(f"Error fetching IDs: {response.status_code} - {response.text}")
        return None

# Function to fetch cryptocurrency info in batches with rate limiting
def fetch_all_coins_info():
    coin_ids = fetch_crypto_ids()
    if not coin_ids:
        print("No coin IDs retrieved, stopping execution.")
        return

    url = "https://pro-api.coinmarketcap.com/v2/cryptocurrency/info"
    API_KEY = "04fcaff7-2831-4f9e-8a64-abcc060265da"
    headers = {"X-CMC_PRO_API_KEY": API_KEY}
    
    batch_size = 100  # API request batch size
    all_data = []

    for i in range(0, len(coin_ids), batch_size):
        batch = ",".join(coin_ids[i:i+batch_size])  # Create batch of IDs
        params = {"id": batch}

        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()
            all_data.extend(data["data"].values())  # Store results
        elif response.status_code == 429:
            print(f"Rate limit hit at batch {i}-{i+batch_size}. Waiting for 60 seconds...")
            time.sleep(60)  # Wait for 1 minute and retry the request
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                all_data.extend(data["data"].values())
            else:
                print(f"Failed to fetch batch {i}-{i+batch_size} even after retrying: {response.status_code} - {response.text}")
        else:
            print(f"Error fetching batch {i}-{i+batch_size}: {response.status_code} - {response.text}")

        # Optional: Add a small delay between requests to prevent hitting the rate limit too quickly
        time.sleep(3)  # Wait for 3 seconds before making the next request

    # Convert the collected data into a DataFrame
    df = pd.DataFrame(all_data)

    # Add timestamp
    timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    df['row_update_date'] = timestamp

    # Save to CSV
    directory_path = f"/opt/airflow/data/coinmarketcap_all_coins_info_{timestamp}.csv"
    df.to_csv(directory_path, index=False)
    print(f"Data saved to: {directory_path}")

# Define the Python function to fetch and save cryptocurrency data
def fetch_coin_api_data():
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

def fetch_coingecko_data():
    # CoinGecko API URL
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",  # Market data in USD
        "order": "market_cap_desc",  # Order by market cap
        "per_page": 250,  # Max number of coins per request
        "page": 1,  # Page number for pagination
        "sparkline": False  # Include sparkline data or not
    }
    
    all_data = []  # To store combined results

    # Loop through pages to fetch up to 1000 records (4 pages of 250 records each)
    for page in range(1, 5):  # 4 pages (250 records per page)
        params["page"] = page
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if not data:  # Stop the loop if no more data is returned
                break
            all_data.extend(data)  # Add the new data to the list
        else:
            print(f"Failed to fetch data for page {page}. Status Code: {response.status_code}")
            break

    # Convert to a Pandas DataFrame for processing
    df = pd.DataFrame(all_data)
    
    # Select relevant fields from the API response
    df = df[[
        "id", "symbol", "name", "current_price", "market_cap", "market_cap_rank",
        "total_volume", "high_24h", "low_24h", "price_change_24h", "price_change_percentage_24h",
        "circulating_supply", "total_supply", "max_supply", "ath", "ath_change_percentage"
    ]]
    
    # Add a timestamp column for when the data was fetched
    timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    df['row_update_date'] = timestamp
    
    # Save the DataFrame to a CSV file
    directory_path = f"/opt/airflow/data/coingecko_data_{timestamp}.csv"
    df.to_csv(directory_path, index=False)
    print(f"CoinGecko data saved to: {directory_path}")

def fetch_coingecko_coins_list():
    # CoinGecko API URL
    url = "https://api.coingecko.com/api/v3/coins/list"
    
    # Send the GET request to CoinGecko API
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        
        # Convert to a Pandas DataFrame for processing
        df = pd.DataFrame(data)
        
        # Add a timestamp column for when the data was fetched
        timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
        df['row_update_date'] = timestamp
        
        # Save the DataFrame to a CSV file
        directory_path = f"/opt/airflow/data/coingecko_coins_list_{timestamp}.csv"
        df.to_csv(directory_path, index=False)
        print(f"CoinGecko coins list data saved to: {directory_path}")
    else:
        print("Failed to fetch data from CoinGecko API.")
        print(f"Response Status Code: {response.status_code}, Message: {response.text}")
        
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
    fetch_coinapi_data = PythonOperator(
        task_id='fetch_coin_api_data', 
        python_callable=fetch_coin_api_data
    )

    fetch_coingecko_task = PythonOperator(
        task_id='fetch_coingecko_data',
        python_callable=fetch_coingecko_data
    )

    fetch_coindata_list = PythonOperator(
        task_id='fetch_coingecko_list',
        python_callable=fetch_coingecko_coins_list
    )

    fetch_all_coins_info_task = PythonOperator(
        task_id="fetch_all_coins_info",
        python_callable=fetch_all_coins_info
    )
    # The DAG will only have this single task, but you can add more if needed
    fetch_coinapi_data >> fetch_coingecko_task >> fetch_coindata_list >> fetch_all_coins_info_task
    