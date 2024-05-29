-- Database and schema creation statements plus access management

-- Create split Finance schema
---- separate the data from the logic as per the data mesh philosophy
create schema polar_fleece_apac_prd.finance_ods 
    with managed access
    comment = 'Operational data store layer for finance data that is ingested into the Polar Fleece database. This serves as a source of transactional data prior to analysis.';
create schema polar_fleece_apac_prd.finance_analytics 
    with managed access
    comment = 'Analytics layer for finance data that has been transformed from finance_ODS. This layer is for developing views for users and reporting tools.';

-- Create a domain owner role and grant it to a human user
---- restricted to ownership privileges of finance domain objects
create role if not exists role_finance_owner_prd;
grant usage on database polar_fleece_apac_prd to role role_finance_owner_prd;
grant usage on warehouse compute_wh to role role_finance_owner_prd;
---- Grant the Finance owner role full access privileges over the Finance domain
grant ownership on schema polar_fleece_apac_prd.finance_ods to role role_finance_owner_prd revoke current grants;
grant ownership on schema polar_fleece_apac_prd.finance_analytics to role role_finance_owner_prd revoke current grants;
---- Create a user and grant them finance domain ownership
create user user_jordan_prd;
alter user user_jordan_prd set default_warehouse = compute_wh; --password set in web ui
grant role role_finance_owner_prd to user user_jordan_prd;

-- Create ETL and Viz roles and grant them to a bot user
---- Create a role for handling the ETL of the alphavantage API
---- The role will need to be able to select from finance-ODS objects
---- The role will need to be able to insert into finance-ODS objects
create role if not exists role_finance_etl_prd;
grant usage on database polar_fleece_apac_prd to role role_finance_etl_prd;
grant usage on warehouse compute_wh to role role_finance_etl_prd;
grant select,insert on all tables in schema polar_fleece_apac_prd.finance_ods to role role_finance_etl_prd;
---- Create a role for visualising Snowflake data
---- The role will need to be able to select from finance-ODS and finance-analytics objects
create role if not exists role_finance_viz_prd;
grant usage on database polar_fleece_apac_prd to role role_finance_viz_prd;
grant usage on warehouse compute_wh to role role_finance_viz_prd;
grant select on all tables in database polar_fleece_apac_prd to role role_finance_viz_prd;
grant select on all views in database polar_fleece_apac_prd to role role_finance_viz_prd;
---- Create the bot user
create user app_python_aks;
alter user app_python_aks set default_warehouse = compute_wh; --password set in web ui
grant role role_finance_viz_prd to user app_python_aks;
grant role role_finance_etl_prd to user app_python_aks;