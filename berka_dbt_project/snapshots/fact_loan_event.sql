


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
   loans.account_id as account_id,
   amount_granted,
   loan_duration_months,
   monthly_payments_amount,
   loan_status_update,
   clients.client_id as primary_client_id,
   district_id
from loans 
left join clients using (account_id)
left join districts using (client_id)
  
{% endsnapshot %}
