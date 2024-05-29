-- av-tickers-sc-dim
---- basic constrained and commented slowly changing type 2 dimension table for use with the AlphaVantage API integration.

-- create the table
create or replace table polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim (
    ticker_id number autoincrement not null unique comment 'Autoincrementing numeric primary key.',
    company_name text not null comment 'Name of the company associated with the ticker.',
    ticker_symbol text not null comment 'Ticker symbol used on the given stock exchange.',
    stock_exchange text not null comment 'Exchange where the stock is listed.',
    is_active boolean not null default false comment 'Boolean used to activate/deactivate monitoring.',
    insert_datetime_ntz timestamp_ntz default sysdate() comment 'UTC datetime when the row was created.',
    constraint pk_ticker_id primary key (ticker_id)
)
comment = 'Slowly changing type 2 dimension table. Provides a list of stock tickers to monitor via the AlphaVantage API.';

-- insert the first set of values for the table
insert into polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim (
    ticker_symbol,
    company_name,
    stock_exchange,
    is_active
) values 
    ('RMD', 'ResMed Inc.', 'NYSE', true),
    ('NVO', 'Novo Nordisk A/S', 'NYSE', true),
    ('TMO', 'Thermo Fisher Scientific Inc.', 'NYSE', true),
    ('AMD', 'Advanced Micro Devices, Inc', 'NASDAQ', true),
    ('TSM', 'Taiwan Semiconductor Manufacturing Company Limited', 'NYSE', true),
    ('FSLR', 'First Solar, Inc.', 'NASDAQ', true),
    ('RWL', 'Rubicon Water Ltd', 'ASX', true);

-- make changes to the table
insert into polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim (
    ticker_symbol,
    company_name,
    stock_exchange,
    is_active
) values 
    ('NVO', 'Novo Nordisk A/S', 'NYSE', false),
    ('AMD', 'Advanced Micro Devices, Inc', 'NASDAQ', false),
    ('ISRG', 'Intuitive Surgical, Inc.', 'NASDAQ', true);

-- sample the table
select * from polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim sample (3 rows);

-- query the table, but only fetch tickers if the latest is_active is true
---- does not return NVO or AMD, as expected
---- this will be the foundation for the av-daily-adjusted-fact table
with ticker_latest as (
    select 
        ticker_symbol,
        max_by(company_name, insert_datetime_ntz) as company_name,
        max_by(stock_exchange, insert_datetime_ntz) as stock_exchange,
        max_by(is_active, insert_datetime_ntz) as is_active_latest
    from polar_fleece_apac_prd.finance_ods.av_tickers_sc_dim
    group by ticker_symbol
)
select 
    ticker_symbol,
    company_name,
    stock_exchange
from ticker_latest 
where is_active_latest = true;