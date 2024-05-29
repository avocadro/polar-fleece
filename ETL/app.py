import requests
import pandas as pd
import snowflake.connector
import json
import os

# Configuration
API_URL = "https://api.example.com/data"  # Replace with the actual API URL
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
SNOWFLAKE_TABLE = os.getenv('SNOWFLAKE_TABLE')

def fetch_secrets():
    if os.getenv('ENVIRONMENT') == 'local':
        SNOWFLAKE_PASSWORD = os.getenv("LOCALDEV_SNOWFLAKE_PASSWORD")
        API_KEY = os.getenv("LOCALDEV_ALPHAVANTAGE_API_KEY")
    elif os.getenv('ENVIRONMENT') == 'AKSPRD':
        SNOWFLAKE_PASSWORD = os.getenv("AKSPRD_SNOWFLAKE_PASSWORD")
        API_KEY = os.getenv("AKSPRD_ALPHAVANTAGE_API_KEY")

def fetch_data(api_url):
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def load_data_to_snowflake(data):
    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )

    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Write data to Snowflake
    success, nchunks, nrows, _ = df.to_sql(
        SNOWFLAKE_TABLE,
        conn,
        if_exists='append',  # Change to 'replace' if you want to overwrite the table
        index=False
    )

    print(f"Inserted {nrows} rows into {SNOWFLAKE_TABLE}")

    # Close the connection
    conn.close()

def main():
    data = fetch_data(API_URL)
    load_data_to_snowflake(data)

if __name__ == "__main__":
    main()
