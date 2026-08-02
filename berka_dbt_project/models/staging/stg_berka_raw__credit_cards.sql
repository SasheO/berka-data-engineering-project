select
        card_id,
        disp_id as disposition_id,
        "type" as credit_card_type,
        CAST(parseDateTimeBestEffort(concat('19', issued)) AS Date) AS date_of_issue
from {{ source('berka_raw', 'src_cards') }}
