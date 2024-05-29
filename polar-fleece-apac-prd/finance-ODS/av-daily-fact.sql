

create or replace table polar_fleece_apac_prd.finance_ods.av_daily_adjusted_fact (
    daily_adjusted_id number autoincrement not null unique comment 'Autoincrementing numeric primary key.',
) comment = 'Fact table containing transactional daily adjusted stock data from Alpha Vantage API. Populated by a daily task.';

with ticker_latest as (
    select 
        ticker_symbol,
        max_by(company_name, insert_datetime_ntz) as company_name,
        max_by(stock_exchange, insert_datetime_ntz) as stock_exchange,
        max_by(is_active, insert_datetime_ntz) as is_active_latest
    from polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim
    group by ticker_symbol
),
relevant_tickers as (
    select stock_exchange || '.' || ticker_symbol as symbol
    from ticker_latest 
    where is_active_latest = true
)
SELECT 
    s.symbol,
    t.timestamp, 
    t.open, 
    t.high, 
    t.low, 
    t.close, 
    t.volume
FROM relevant_tickers s,
TABLE(POLAR_FLEECE_APAC_PRD.FINANCE_ODS.get_daily_time_series(s.symbol)) AS t;
