select
        trans_id as transaction_id,
        CAST(parseDateTimeBestEffort(concat('19', "date")) AS Date) AS transaction_date,
        CASE "type"
                WHEN 'PRIJEM' THEN 'credit'
                when 'VYDAJ' THEN 'withdrawal'
                ELSE 'unknown'
        END AS transaction_type,
        CASE operation
                WHEN 'VYBER KARTOU' THEN 'credit card withdrawal'
                when 'VKLAD' THEN 'credit in cash'
                WHEN 'PREVOD Z UCTU' THEN 'collection from another bank'
                WHEN 'VYBER' THEN 'withdrawal in cash'
                WHEN 'PREVOD NA UCET' THEN 'remittance to another bank'
                ELSE 'unknown'
        END AS transaction_operation,
        amount as transaction_amount,
        balance as account_balance_after_transaction,
        CASE k_symbol
                WHEN 'POJISTNE' THEN 'insurance payment'
                WHEN 'SLUZBY' THEN 'payment for statement'
                WHEN 'UROK' THEN 'interest credited'
                WHEN 'SANKC. UROK' THEN 'sanction interest if negative balance'
                WHEN 'SIPO' THEN 'household'
                WHEN 'DUCHOD' THEN 'old­age pension'
                WHEN 'UVER' THEN 'loan payment'
                ELSE ''
        END AS transaction_characterisation,
        bank as bank_id,
        account as partner_account_id
from {{ source('berka_raw', 'src_transactions') }}