


{% snapshot fact_loan_event %}
  {{
    config(
      unique_key='loan_id',
      strategy='check',
      check_cols=['loan_status_update']
    )
  }}
  with clients as (
   select 
      client_id,
      account_id
   from {{ ref('dim_disposition') }}
   where "type" = 'OWNER'
  ),
  districts as (
   select
      district_id,
      client_id
   from {{ ref('dim_client') }}
  ),
  loans as (
   select 
      loan_id,
      date_granted,
      account_id,
      amount_granted,
      loan_duration_months,
      monthly_payments_amount,
      loan_status_update
   from {{ ref('stg_berka_raw__loans') }}
  )
select 
   loan_id,
   date_granted,
   loans.account_id,
   amount_granted,
   loan_duration_months,
   monthly_payments_amount,
   loan_status_update,
   account_id,
   clients.client_id as primary_client_id,
   district_id
from loans 
left join clients using (account_id)
left join districts using (client_id)
  
{% endsnapshot %}

-- Table fact_loan_event {
--   Note: "One row per loan per account"
--   loan_event_id integer [primary key]
--   loan_id integer [not null]
--   account_id integer [not null, note: "identification of the account"]
--   amount_granted integer [not null, note: ""]
--   date_granted date [not null, note: ""]
--   loan_duration_months integer [not null, note: ""]
--   monthly_payments_amount integer [not null, note: ""]
--   district_id integer [not null, note: "The district of the client when the loan was issued. SCD 0, doesn't change"]
--   primary_client_id integer [not null, note: "owner of the account tied to the loan"]
--   loan_status_update string [note: "SCD 2. Status repaying the loan. Can be one of four options showing whether contract is running or not and client is defaulting on payments or not."]
--   loan_status_effective_from date [note: ""]
--   loan_status_effective_to date [note: ""]
  
-- }