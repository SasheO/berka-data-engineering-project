select 
    account_id,
    district_id,
    statement_issue_frequency,
    date_created
from {{ ref('stg_berka_raw__accounts') }}