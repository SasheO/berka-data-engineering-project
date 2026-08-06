select 
    credit_card_id,
    credit_card_type,
    date_of_issue,
    DATE '9999-12-31' as date_of_expiry, -- should be coalesce date_of_expiry with this, but input dataset has no date of expiry
    disposition_id
from {{ ref('stg_berka_raw__credit_cards') }}
