SELECT 
  account_id,
  transaction_date as accounting_date,
  district_id,
  count(transaction_id) as number_of_transactions,
  max(account_balance_after_transaction) as closing_balance, -- TODO: implement closing balance as account_balance_after_transaction where transaction_id is max
  sum(transaction_amount) as absolute_transaction_value,
  SUM(CASE WHEN transaction_type = 'withdrawal' THEN -transaction_amount ELSE transaction_amount END)  as net_transaction_value
from {{ ref('fact_transaction') }}
group by account_id, transaction_date, district_id


-- Table fact_daily_account_closing_balance {
--   Note: "One row per account per day"
--   accounting_date date [primary key]
--   account_id integer [primary key, note: ""]
--   district_id integer [not null, note: ""]
--   closing_balance integer [not null, note: ""]
--   number_of_transactions integer [not null, note: ""]
--   absolute_transaction_value integer [not null, note: ""]
--   net_transaction_value integer [not null, note: ""]
-- }