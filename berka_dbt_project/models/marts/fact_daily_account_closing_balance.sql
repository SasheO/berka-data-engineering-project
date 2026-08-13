-- this could be changed to incremental materialization that compares with account_id an accounting_date as the size of data grows and table materialization becomes too slow
-- TODO: add accounting date spine where daily account closing balance is the previous day's if there are no records on that day itself
{{
  config(
    materialized = 'incremental',
    unique_key = ['account_id', 'accounting_date'],
  )
}} -- TODO: add is incremental block somewhere
WITH last_transaction_of_day AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    transaction_id,
    account_balance_after_transaction
  FROM {{ ref('fact_transaction') }}
  where transaction_id_immediately_after_on_same_day = 0
),
aggregated AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    district_id,
    count(transaction_id) as number_of_transactions,
    round(sum(transaction_amount), 2) as absolute_transaction_value,
    round(SUM(CASE WHEN transaction_type = 'withdrawal' THEN -transaction_amount ELSE transaction_amount END), 2) as net_transaction_value
  FROM {{ ref('fact_transaction') }}
  GROUP BY account_id, accounting_date, district_id
)
SELECT 
  a.account_id as account_id,
  a.accounting_date as accounting_date,
  a.district_id as district_id,
  a.number_of_transactions as number_of_transactions,
  r.account_balance_after_transaction as closing_balance,
  a.absolute_transaction_value as absolute_transaction_value,
  a.net_transaction_value as net_transaction_value
FROM aggregated a
LEFT JOIN last_transaction_of_day r 
using (account_id, accounting_date )