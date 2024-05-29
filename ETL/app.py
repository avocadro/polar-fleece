import requests
import snowflake.connector
import os

# Configuration

ENVIRONMENT = os.getenv('ENVIRONMENT')
if ENVIRONMENT not in ['LOCAL_DEV', 'AKS_PRD']:
    raise ValueError(f"Environment '{ENVIRONMENT}' is not valid. Expected 'LOCAL_DEV' or 'AKS_PRD'.")
else:
    SNOWFLAKE_USER = 'APP_PYTHON_AKS'
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_ACCOUNT = 'AQMTEBD.JX70701'
    SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
    ALPHAVANTAGE_API_KEY = os.getenv('ALPHAVANTAGE_API_KEY')

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE
    )

def get_and_put_data(conn):
    cursor = conn.cursor()
    select_query =  ''' 
        with ticker_latest as (
            select 
                ticker_symbol,
                max_by(company_name, insert_datetime_ntz) as company_name,
                max_by(stock_exchange, insert_datetime_ntz) as stock_exchange,
                max_by(is_active, insert_datetime_ntz) as is_active_latest
            from polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim
            group by ticker_symbol
            )
            select ticker_symbol
            from ticker_latest 
            where is_active_latest = true 
    '''
    cursor.execute(select_query)
    results = cursor.fetchall()
    cursor.close()
    for result in results:
        # tickers are just in the first column (0)
        ticker_symbol = result[0]
        api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker_symbol}&outputsize=compact&apikey={ALPHAVANTAGE_API_KEY}&datatype=json" 
        # Make an API call for each value
        response = requests.get(api_url)
        api_data = response.json()
        # Insert each row of the API call result JSON into the Snowflake table
        insert_data(conn, api_data)

def insert_data(conn, data):
    insert_query = f"insert into polar_fleece_apac_prd.finance_ods.av_daily_fact VALUES (?, ?)"  # Example query - needs to be adjusted
    cursor = conn.cursor()
    for row in data:
        cursor.execute(insert_query, row)  # Assuming row is a tuple of values matching the table schema
    cursor.close()
    conn.commit()

def main():
    get_and_put_data(conn)

if __name__ == "__main__":
    main()
