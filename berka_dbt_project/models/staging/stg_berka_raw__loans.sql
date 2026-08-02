select
        loan_id,
        CAST(parseDateTimeBestEffort(concat('19', "date")) AS Date) AS date_granted,
        account_id,
        amount as amount_granted,
        duration as loan_duration_months,
        payments as monthly_payments_amount,
        CASE "status"
                WHEN 'A' THEN 'contract finished, no problems'
                WHEN 'B' THEN 'contract finished, loan not payed'
                WHEN 'C' THEN 'running contract, OK so far'
                WHEN 'D' THEN 'running contract, client in debt'
        END AS loan_status_update
from {{ source('berka_raw', 'src_loans') }}