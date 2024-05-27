-- API-integration
-- Enables the querying of Alpha Vantage APIs so that further analytics on NYSE data can be developed.
-- Note that the current free-tier API key is limited to 25 calls per day.
-- Adapted from https://medium.com/snowflake/pulling-data-from-an-external-api-into-snowflake-with-python-dcc1ba6ecc69
-- Refined with the assistance of ChatGPT.

-- Session context:
USE ROLE ROLE_FINANCE_OWNER_PRD;

-- Create a network rule
CREATE OR REPLACE NETWORK RULE POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_api_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('www.alphavantage.co');

-- Create a once-off secret then delete the string so that later steps do not refer to plaintext
CREATE OR REPLACE SECRET POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_api_key
  TYPE = GENERIC_STRING
  SECRET_STRING = 'placeholder'; -- DO NOT STORE SECRETS IN PLAINTEXT

-- Create an external access integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_access_integration
  ALLOWED_NETWORK_RULES = (alphavantage_api_rule)
  ALLOWED_AUTHENTICATION_SECRETS = (alphavantage_api_key)
  ENABLED = true;

-- Create the function for querying the Alpha Vantage API
CREATE OR REPLACE FUNCTION get_daily_adjusted_time_series(symbol VARCHAR) 
RETURNS TABLE (
    timestamp STRING, 
    open FLOAT, 
    high FLOAT, 
    low FLOAT, 
    close FLOAT, 
    adjusted_close FLOAT, 
    volume BIGINT, 
    dividend_amount FLOAT, 
    split_coefficient FLOAT)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'ApiData'
EXTERNAL_ACCESS_INTEGRATIONS = (alphavantage_access_integration)
SECRETS = ('api_key' = alphavantage_api_key)
PACKAGES = ('requests')
AS
$$
import requests

class ApiData:
    def process(self, symbol):
        # Retrieve the API key securely
        api_key = _snowflake.get_secret('alphavantage_api_key')
        
        # Call the API
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey={api_key}"
        response = requests.get(url)
        data = response.json()
        
        time_series = data.get("Time Series (Daily)", {})
        
        for timestamp, values in time_series.items():
            yield (
                timestamp,
                float(values["1. open"]),
                float(values["2. high"]),
                float(values["3. low"]),
                float(values["4. close"]),
                float(values["5. adjusted close"]),
                int(values["6. volume"]),
                float(values["7. dividend amount"]),
                float(values["8. split coefficient"])
            )
$$;

-- This step uses the existing function to query the API for each symbol in the table
WITH symbol_data AS (
    SELECT symbol FROM symbols
),
api_results AS (
    SELECT symbol, t.*
    FROM symbol_data,
    LATERAL TABLE(get_daily_adjusted_time_series(symbol)) AS t
)
SELECT * FROM api_results;