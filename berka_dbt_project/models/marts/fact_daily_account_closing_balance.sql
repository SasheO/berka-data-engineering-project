-- this could be changed to incremental materialization that compares with account_id an accounting_date as the size of data grows and table materialization becomes too slow
{{
  config(
    materialized = 'incremental',
    unique_key = ['account_id', 'accounting_date'],
  )
}} 
WITH 
last_transaction_of_day AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    transaction_id,
    account_balance_after_transaction
  FROM {{ ref('fact_transaction') }}
  where transaction_id_immediately_after_on_same_day = 0 
  -- WOULD DO: add is_incremental block here to only get data within most recent accounting period
),
aggregated_closing_balances AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    district_id,
    count(transaction_id) as number_of_transactions,
    round(sum(transaction_amount), 2) as absolute_transaction_value,
    round(SUM(CASE WHEN transaction_type = 'withdrawal' THEN -transaction_amount ELSE transaction_amount END), 2) as net_transaction_value
  FROM {{ ref('fact_transaction') }}
  GROUP BY account_id, accounting_date, district_id
),
base_dates as (
    {{ dbt.date_spine(
        datepart="day",
        start_date="(select min(transaction_date) from " ~ ref('fact_transaction') ~ ")",
        end_date="(select dateadd(day, 1, max(transaction_date)) from " ~ ref('fact_transaction') ~ ")"
    ) }}
),
unique_account_ids as (
    select distinct 
        account_id
    from {{ ref('dim_account') }}   -- Replace with your actual staging or dim model
    where account_id is not null
),
account_date_grid as (
    select
        cast(d.date_day as date) as accounting_date,
        a.account_id as account_id
    from base_dates d
    cross join unique_account_ids a
),
final_ as 
(
  SELECT 
    a.account_id as account_id,
    a.accounting_date as accounting_date,
    a.district_id as district_id,
    a.number_of_transactions as number_of_transactions,
    r.account_balance_after_transaction as closing_balance,
    a.absolute_transaction_value as absolute_transaction_value,
    a.net_transaction_value as net_transaction_value
  FROM aggregated_closing_balances a
  LEFT JOIN last_transaction_of_day r 
  using (account_id, accounting_date )
  
  
  union all 

  select
    account_id,
    accounting_date,
    0 as district_id,
    0 as number_of_transactions,
    0 as closing_balance,
    0 as absolute_transaction_value,
    0 as net_transaction_value
  from account_date_grid
)
SELECT 
    account_id,
    accounting_date,
    max(district_id) as district_id,
    sum(number_of_transactions) as number_of_transactions,
    sum(closing_balance) as closing_balance,
    sum(absolute_transaction_value) AS absolute_transaction_value,
    sum(net_transaction_value) as net_transaction_value
from final_
group by account_id, accounting_date
order by accounting_date, account_id