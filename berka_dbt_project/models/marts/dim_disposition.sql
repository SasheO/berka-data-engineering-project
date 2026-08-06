select 
    disposition_id,
    client_id,
    account_id,
    "type"
from {{ ref('stg_berka_raw__dispositions') }}
