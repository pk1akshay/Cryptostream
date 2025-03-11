# scripts/helper.py
import pandas as pd
import os
from datetime import datetime

def save_to_csv(df, filename):
    """Save DataFrame to a CSV file with a timestamp."""
    timestamp = datetime.now().strftime('%d_%m_%Y_%H_%M_%S')
    filepath = os.path.join("/opt/airflow/data", f"{filename}_{timestamp}.csv")
    df.to_csv(filepath, index=False)
    print(f"Data saved to: {filepath}")
