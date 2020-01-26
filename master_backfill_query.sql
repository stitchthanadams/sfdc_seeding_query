with
clients as (
    select id as client_id, name, created_at::date as signup_date from internal_analytics.blessed_stitch_clients
),
emails as (
    select client_id, email from (
    select *, row_number() over(partition by client_id order by user_created_at) r from internal_analytics.client_all_users
    where email not ilike '%stitchdata.com'
    ) where r = 1
),
domains as (
    select distinct  stitch_client_id as client_id, email as email, trim(regexp_substr(email, '@.*'), '@') as email_domain, company_domain from internal_analytics.clearbit_users_enriched
),
trial_starts as (
  select
    min(event_date)::date as trial_start_date,
    client_id
  from internal_analytics.blessed_subscription_events
  where event = 'trial_start'
  group by 2
),
rowage as (
  select client_id, sum(total_rows_daily) as rows_last_30_days 
  from internal_analytics.blessed_account_activity
  where day::timestamp_ntz >= date_trunc('DAY', convert_timezone('EST', current_timestamp)::timestamp_ntz) - interval '30 days'
  group by 1
),
plans as (
    select client_id, plan, plan_start_date::date as plan_start_date, case when plan in(
          'enterprise'
        , 'standard'
        , 'premier'
        , 'basic'
        , 'starter'
    ) then plan_start_date end as paid_plan_start_date
    from "STITCHCDW"."RECKONER"."ACCOUNTS_LATEST_VIEW"
    --where plan not in('deactivated', 'expired')
),

customer_tier_detail as (
select 
    client_id 
  , case when limit_million_rows > 0 then limit_million_rows end as Stitch_Customer_Tier_Detail
  , case when limit_million_rows > 0 then coalesce(term, 'monthly') end as Stitch_Customer_Term
from "STITCHCDW"."RECKONER"."ACCOUNTS_LATEST_VIEW"  
),

joined as (
  select * from clients
  left join emails using(client_id) --cl_cid = em_cid
  left join domains using(email)
  left join trial_starts using(client_id)
  left join rowage using(client_id)
  left join plans using(client_id)
  left join customer_tier_detail using(client_id)
),
clean as (
  select
      client_id as Stitch_Customer_ID__c
//    , email
//    , email_domain
    , trial_start_date as Stitch_Free_Trial_Customer_Start_Date__c
    , rows_last_30_days as Stitch_Row_Usage_Last_30_Days__c
    , plan as Stitch_Customer_Tier__c
    , case when plan = 'free' then 'free'
        when plan = 'enterprise' then 'enterprise'
        when plan in ('standard', 'premier', 'basic', 'starer') then 'self-service'
        end as Stitch_Customer_Type
--    , case when plan in('expired', 'deactivated', 'pre_trial') then null else plan end as Stitch_Customer_Tier__c
    , paid_plan_start_date as Stitch_Paying_Customer_Date__c
    , Stitch_Customer_Tier_Detail
    , Stitch_Customer_Term
    , signup_date
    , case when Stitch_customer_Tier__C in ('expired', 'deactivated','pre_trial') then signup_date end as dead_account_signup_date
  from joined
), 

clean_base as (
    select * from clean
), 

------------------------------------------
source_and_destination as (
with all_clients as (
select 
  id as client_id, 
  name as company_name,
  created_at as client_created_at
from "STITCHCDW"."INTERNAL_ANALYTICS"."BLESSED_STITCH_CLIENTS"
),

active_connections_agg as (
select 
  client_id, 
  listagg(distinct type,',') as source_types,
  count(distinct (case when regexp_substr(type, 'close|desk|hubspot|intercom|jira|marketo|mongo|netsuite|pardot|quickbook|salesforce|zendesk|zuora|xero|responsys|db2|oracle|amazon-rds-oracle|ftp-sftp-ftps') is not null then regexp_substr(type, 'close|desk|hubspot|intercom|jira|marketo|mongo|netsuite|pardot|quickbook|salesforce|zendesk|zuora|xero|responsys|db2|oracle|amazon-rds-oracle|ftp-sftp-ftps') end)) as num_paid_sources,
  count(distinct type) as num_sources,
  count(distinct connection_id) as num_connections
from internal_analytics.active_connections 
group by 1
),

destinations as (
select 
  client_id, 
  count(distinct type) as num_destinations,
  listagg(distinct type,',') as dest_types
from STITCHCDW.CONNECTION_SERVICE.CONNECTIONS
where namespace = 'remote-data-warehouses' and deleted_at is null
group by 1
),

zuora_mrr as (
select 
  account_number, 
  mrr, 
  mrr*12 as arr
  from internal_analytics.zuora_accounts 
),

sources_joined as (
select * from all_clients 
left join active_connections_agg using(client_id)
left join destinations using(client_id)
left join zuora_mrr on try_to_numeric(account_number) = all_clients.client_id
-- left join prepaid on try_to_numeric(account_number) = active_clients.client_id
)

select 
  client_id, 
  company_name,
  client_created_at, 
  source_types, 
  num_paid_sources, 
  num_sources, 
  num_connections, 
  dest_types, 
  num_destinations,
  mrr, 
  arr
from sources_joined
),
------------------------------------------

first_join as (
    select * from clean_base
    full outer join source_and_destination 
    on Stitch_Customer_ID__c = client_id
), 

billing_address as (
  with zuora_accounts as (
    select * from internal_analytics.zuora_accounts
  ), 
    
  payment_method as (
    select * from internal_analytics.zuora_paymentmethod
  ),
    
  combined as (
    select 
        try_to_numeric(a.account_number) as client_id, 
        a.balance, 
        a.last_invoice_date, 
        b.credit_card_address1, 
        b.credit_card_address2, 
        b.credit_card_city, 
        b.credit_card_state, 
        b.credit_card_country, 
        b.last_transaction_date, 
        b.last_transaction_status
    from zuora_accounts a
    join payment_method b on payment_id = default_payment_method_id
    -- where balance > 0 or mrr > 0 or last_transaction_date is not null

    order by mrr
  )
    select * from combined
), 

second_join as (
select * from first_join 
  left join billing_address using(client_id)
),

industry as (
with first_user as (

with filtered_users as (
select * from  platform.rjm_users users
where email not ilike '%stitchdata.com' and email not ilike '%talend.com'
)

select client_id, client_name, user_id, email, user_created_at from (
  select
    clients.id as client_id,
    clients.name as client_name,
    users.email,
    users.uid as user_id,
    users.joindate as user_created_at,
    row_number() over (partition by clients.id order by rights.date_granted) as user_number
  from internal_analytics.blessed_stitch_clients clients
  left join platform.rjm_user_rights rights
    on rights.cid = clients.id
  join filtered_users users
    on users.uid = rights.uid
  
) a
where user_number = 1
)

select distinct stitch_client_id as client_id, company_category_industry FROM "STITCHCDW"."INTERNAL_ANALYTICS"."CLEARBIT_USERS_ENRICHED" 
where company_category_industry is not null
and stitch_user_id in (select user_id from first_user)
), 

add_industry as (
select * from second_join 
  left join industry using(client_id)
),


cbit_country as (
with first_user as (

with filtered_users as (
select * from  platform.rjm_users users
where email not ilike '%stitchdata.com' and email not ilike '%talend.com'
)

select client_id, client_name, user_id, email, user_created_at from (
  select
    clients.id as client_id,
    clients.name as client_name,
    users.email,
    users.uid as user_id,
    users.joindate as user_created_at,
    row_number() over (partition by clients.id order by rights.date_granted) as user_number
  from internal_analytics.blessed_stitch_clients clients
  left join platform.rjm_user_rights rights
    on rights.cid = clients.id
  join filtered_users users
    on users.uid = rights.uid
  
) a
where user_number = 1
)

select distinct stitch_client_id as client_id, company_geo_country FROM "STITCHCDW"."INTERNAL_ANALYTICS"."CLEARBIT_USERS_ENRICHED" 
where company_geo_country is not null
and stitch_user_id in (select user_id from first_user)
),


add_cbit_country as (
select * from add_industry 
  left join cbit_country using(client_id)
),

cbit_state as (
with first_user as (

with filtered_users as (
select * from  platform.rjm_users users
where email not ilike '%stitchdata.com' and email not ilike '%talend.com'
)

select client_id, client_name, user_id, email, user_created_at from (
  select
    clients.id as client_id,
    clients.name as client_name,
    users.email,
    users.uid as user_id,
    users.joindate as user_created_at,
    row_number() over (partition by clients.id order by rights.date_granted) as user_number
  from internal_analytics.blessed_stitch_clients clients
  left join platform.rjm_user_rights rights
    on rights.cid = clients.id
  join filtered_users users
    on users.uid = rights.uid
  
) a
where user_number = 1
)

select distinct stitch_client_id as client_id, company_geo_state FROM "STITCHCDW"."INTERNAL_ANALYTICS"."CLEARBIT_USERS_ENRICHED" 
where company_geo_country is not null
and stitch_user_id in (select user_id from first_user)
),

add_cbit_state as (
select * from add_cbit_country 
  left join cbit_state using(client_id)
),


users as (
  select 
    uid as user_id, 
    cid as client_id, 
    first_name, 
    last_name, 
    name, 
    email, 
    joindate as user_join_date
  from platform.rjm_users
  join platform.rjm_user_rights using(uid)
), 

clearbit_data as (
select 
  stitch_user_id as user_id, 
  person_employment_role as job_role, 
  person_employment_seniority as job_seniority,
  person_employment_title as job_title
from "STITCHCDW"."INTERNAL_ANALYTICS"."CLEARBIT_USERS_ENRICHED"
),

add_users as (
select * from add_cbit_state 
  full outer join users using(client_id)
), 

add_clearbit_job_details as (
select * from add_users 
  full outer join clearbit_data using(user_id)
), 

all_data as (
select * from add_clearbit_job_details
),

first_pass as (
select 
  company_name, 
  first_name, 
  last_name, 
  name, 
  email, 
  CLIENT_CREATED_AT,
  nullif(credit_card_country,'') as credit_card_country__c, 
  company_geo_country as cbit_country, 
  coalesce(credit_card_country__c, cbit_country) as combined_country, 
  company_category_industry as cbit_industry, 
  nullif(credit_card_state, '') as credit_card_state__c,
  company_geo_state as cbit_state,
  coalesce(credit_card_state__c, cbit_state) as combined_state, 
  job_role, 
  job_seniority, 
  job_title, 
  client_id, 
  stitch_customer_tier__c, 
  stitch_customer_type,
  stitch_paying_customer_date__c, 
  stitch_free_trial_customer_start_date__c,
  signup_date as stitch_signup_date, 
  source_types as integrations, 
  num_sources as num_integrations,
  dest_types as destinations, 
  num_destinations, 
  case when stitch_customer_type = 'enterprise' then 'enterprise' else concat(stitch_customer_tier__c, ' ', stitch_customer_term, ' ', stitch_customer_tier_detail) end as stitch_customer_tier_detail, 
  Stitch_Customer_Term as stitch_customer_term,
  mrr as stitch_plan_cost
from all_data
  where dead_account_signup_date >= dateadd('year', -2, current_date) or dead_account_signup_date is null
), 

remove_nulls as (
select * from first_pass 
  where client_id is not null 
  and email is not null
), 

blessed_current_clients as (
select client_id, plan as current_plan from internal_analytics.blessed_current_clients 
), 

add_current_status as (
select a.*, b.current_plan from remove_nulls a
left join  blessed_current_clients b using(client_id)
),

final as (
select 
  company_name, 
  first_name, 
  last_name, 
  email, 
  CLIENT_CREATED_AT,
  combined_country as country, 
  cbit_industry as industry,
  combined_state as state, 
  job_role, 
  job_seniority, 
  job_title, 
  client_id, 
  stitch_customer_tier__c, 
  stitch_customer_type, 
  stitch_paying_customer_date__c, 
  stitch_free_trial_customer_start_date__c, 
  stitch_signup_date, 
  integrations, 
  num_integrations, 
  destinations, 
  num_destinations, 
  stitch_customer_tier_detail, 
  stitch_customer_term,
  stitch_plan_cost, 
  current_plan
  from add_current_status
  where CLIENT_CREATED_AT is not null
),

cleanest as (
  select 
      COMPANY_NAME
    , FIRST_NAME
    , LAST_NAME
    , EMAIL
    , CLIENT_CREATED_AT as stitch_client_creation_date
    , COUNTRY
    , INDUSTRY
    , STATE
    , JOB_TITLE
    , CLIENT_ID
    , case when STITCH_CUSTOMER_TIER__C is null then 'pre_trial' else STITCH_CUSTOMER_TIER__C end as STITCH_CUSTOMER_TIER__C
    , STITCH_CUSTOMER_TYPE
    , STITCH_PAYING_CUSTOMER_DATE__C
    , STITCH_FREE_TRIAL_CUSTOMER_START_DATE__C
    , INTEGRATIONS
    , DESTINATIONS
    , STITCH_CUSTOMER_TIER_DETAIL
    , case 
        when STITCH_CUSTOMER_TIER__C = 'enterprise' and stitch_customer_term is null
        then 'annual'
      else stitch_customer_term
      end as stitch_customer_term
    , case 
        when STITCH_CUSTOMER_TIER__C = 'standard' and stitch_customer_term = 'annual' 
        then STITCH_PLAN_COST * 12  
        
        when STITCH_CUSTOMER_TIER__C = 'enterprise' and stitch_customer_term is null
        then STITCH_PLAN_COST * 12  
  
      else STITCH_PLAN_COST
      end as stitch_plan_cost
  from final
)

select STITCH_CUSTOMER_TIER__C, count(*) as counts from cleanest

-->SELF SERVE ONLY
//where STITCH_CUSTOMER_TIER__C = 'standard'

-->ALL ENTERPRISE
//where STITCH_CUSTOMER_TIER__C = 'enterprise' and stitch_plan_cost > 0

-->ALL FREE
//where STITCH_CUSTOMER_TIER__C = 'free'

-->ALL TRIAL
//where STITCH_CUSTOMER_TIER__C = 'trial'

-->ALL PRE TRIAL
//where STITCH_CUSTOMER_TIER__C = 'pre_trial'

-->ALL EXPIRED
//where STITCH_CUSTOMER_TIER__C = 'expired'

--order by client_id

group by 1 order by 2 desc

