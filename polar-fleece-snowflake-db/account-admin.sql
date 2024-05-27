-- Some administrative commands for setting up core components of the Polar Fleece database

-- Create a domain owner role, which is restricted to ownership privileges of finance domain objects
create role if not exists ROLE_FINANCE_OWNER_PRD;
grant usage on database polar_fleece_apac_prd to role ROLE_FINANCE_OWNER_PRD;

-- Create split Finance schema, to separate the data from the logic as per the data mesh philosophy
create schema polar_fleece_apac_prd.finance_ods 
    with managed access
    comment = 'Operational data store layer for finance data that is ingested into the Polar Fleece database. This serves as a source of transactional data prior to analysis.';
create schema polar_fleece_apac_prd.finance_analytics 
    with managed access
    comment = 'Analytics layer for finance data that has been transformed from finance_ODS. This layer is for developing views for users and reporting tools.';

-- Grant the Finance owner role full access privileges over the Finance domain
grant ownership on schema polar_fleece_apac_prd.finance_ods to role role_finance_owner_prd revoke current grants;
grant ownership on schema polar_fleece_apac_prd.finance_analytics to role role_finance_owner_prd revoke current grants;

-- Create a user and grant them finance domain ownership
create user user_jordan_prd;
grant role role_finance_owner_prd to user user_jordan_prd;