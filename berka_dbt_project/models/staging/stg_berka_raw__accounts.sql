select
        account_id,
        district_id,
        frequency,
        CAST(parseDateTime("date", '%y%m%d') AS Date) AS converted_date

from {{ source('berka_raw', 'src_accounts') }}