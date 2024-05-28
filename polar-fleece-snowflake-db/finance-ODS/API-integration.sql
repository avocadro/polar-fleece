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

-- Create a once-off secret then delete the string so that later steps do not refer to plaintext (cut because UDF secrets not working for free tier)
-- CREATE OR REPLACE SECRET POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_api_key
--   TYPE = GENERIC_STRING
--   SECRET_STRING = ''; -- DO NOT STORE SECRETS IN PLAINTEXT

-- Create an external access integration (cut because it is unavailable for free-tier)
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_access_integration
--   ALLOWED_NETWORK_RULES = (alphavantage_api_rule)
--   ALLOWED_AUTHENTICATION_SECRETS = (alphavantage_api_key)
--   ENABLED = true;

-- Create the function for querying the Alpha Vantage API
CREATE OR REPLACE FUNCTION POLAR_FLEECE_APAC_PRD.FINANCE_ODS.get_daily_time_series(symbol text) 
RETURNS TABLE (
    timestamp date, 
    open number, 
    high number, 
    low number, 
    close number, 
    volume number)
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'ApiData'
--EXTERNAL_ACCESS_INTEGRATIONS = (alphavantage_access_integration)
--SECRETS = ('api_key' = POLAR_FLEECE_APAC_PRD.FINANCE_ODS.alphavantage_api_key)
PACKAGES = ('requests')
AS
$$
import requests
import _snowflake

class ApiData:
    def process(self, symbol):
        # Retrieve the API key securely
        api_key = 'BW0FQAPLGU1TVBWP' #_snowflake.get_generic_secret_string('api_key')
        
        # Call the API
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=compact&apikey={api_key}&datatype=json"
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
                int(values["5. volume"])
            )
$$;