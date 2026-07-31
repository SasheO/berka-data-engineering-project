select
        account_id,
        district_id,
        frequency,
        toDate("date", '%y%m%d')) AS converted_date

from {{ source('berka_raw', 'src_accounts') }}