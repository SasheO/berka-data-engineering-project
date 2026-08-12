{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
  )
}}
with transactions as (
    select 
        transaction_id,
        account_id,
        transaction_date,
        transaction_type,
        transaction_operation,
        transaction_amount,
        account_balance_after_transaction,
        account_balance_before_transaction,
        transaction_characterisation,
        bank_code,
        partner_account_id,
        transaction_id_immediately_after_on_same_day
    from {{ ref('stg_berka_raw__transactions') }}
    {% if is_incremental() %}
        where transaction_date >= dateadd(day, -31, current_date) -- only load current accounting period's transactions
    {% endif %}
),
primary_clients as (
    select
        client_id,
        account_id
    from {{ ref('dim_disposition') }}
    where "type" = 'OWNER'

),
districts as (
    select
        client_id,
        district_id
    from  {{ ref('dim_client') }}
)
select 
    transaction_id,
    account_id,
    transaction_date,
    transaction_type,
    transaction_operation,
    transaction_amount,
    account_balance_after_transaction,
    account_balance_before_transaction,
    transaction_characterisation,
    bank_code,
    partner_account_id,
    transaction_id_immediately_after_on_same_day,
    district_id,
    primary_clients.client_id as primary_client_id
from transactions
left join primary_clients using (account_id)
left join districts using (client_id)