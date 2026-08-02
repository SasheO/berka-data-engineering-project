select
        account_id,
        district_id,
        CASE frequency
            WHEN 'POPLATEK MESICNE' THEN 'monthly issuance'
            WHEN 'POPLATEK TYDNE' THEN 'weekly issuance'
            WHEN 'POPLATEK PO OBRATU' THEN 'issuance after transaction'
            ELSE 'unknown'
        END AS statement_issue_frequency,
        CAST(parseDateTimeBestEffort(concat('19', "date")) AS Date) AS date_created

from {{ source('berka_raw', 'src_accounts') }}