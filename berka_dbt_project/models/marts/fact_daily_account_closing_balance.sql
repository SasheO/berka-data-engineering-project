-- TODO: add incremental materialization
-- TODO: implement bankers rounding in summed values
-- TODO: change implementation of getting closing balance as highest transaction id balance after to latest transaction of the day
WITH ranked_transactions AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    transaction_id,
    transaction_amount,
    transaction_type,
    account_balance_after_transaction,
    ROW_NUMBER() OVER (
      PARTITION BY account_id, transaction_date 
      ORDER BY transaction_id DESC
    ) as rn
  FROM {{ ref('fact_transaction') }}
),
aggregated AS (
  SELECT 
    account_id,
    transaction_date as accounting_date,
    district_id,
    count(transaction_id) as number_of_transactions,
    sum(transaction_amount) as absolute_transaction_value,
    SUM(CASE WHEN transaction_type = 'withdrawal' THEN -transaction_amount ELSE transaction_amount END) as net_transaction_value
  FROM {{ ref('fact_transaction') }}
  GROUP BY account_id, accounting_date, district_id
)
SELECT 
  a.account_id,
  a.accounting_date,
  a.district_id,
  a.number_of_transactions,
  r.account_balance_after_transaction as closing_balance,
  a.absolute_transaction_value,
  a.net_transaction_value
FROM aggregated a
LEFT JOIN ranked_transactions r 
using (account_id, accounting_date )
where r.rn = 1