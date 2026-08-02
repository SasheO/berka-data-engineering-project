select
        disp_id as disposition_id,
        client_id,
        account_id,
        "type"
from {{ source('berka_raw', 'src_disposition') }}
