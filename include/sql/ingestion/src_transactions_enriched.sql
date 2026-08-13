INSERT INTO {{ params.db_schema }}.src_transactions_enriched
(
    trans_id,
    account_id,
    `date`,
    `type`,
    operation,
    amount,
    balance_after,
    k_symbol,
    bank,
    account,
    balance_before,
    subsequent_trans_id
)
with initial as (
    select
        trans_id,
        account_id,
        `date`,
        `type`,
        operation,
        amount,
        balance as balance_after,
        k_symbol,
        bank,
        account
FROM {{params.db_schema}}.src_transactions
),
balances_before_and_after as (
    SELECT 
        trans_id,
        account_id,
        `date`,
        `type`,
        operation,
        amount,
        balance_after,
        k_symbol,
        bank,
        account,
        if(`type`='PRIJEM', balance_after-amount, balance_after+amount) as balance_before -- subtract if PRIJEM type (meaning a credit transaction) to get what value was before, add otherwise i.e. a withdrawal
    from initial
), 
subsequent_trans_ids as (
    select 
        t1.trans_id as trans_id,
        t1.account_id as account_id,
        t1.date as date,
        t1.type,
        t1.operation,
        t1.amount,
        t1.balance_after,
        t1.k_symbol,
        t1.bank,
        t1.account,
        t1.balance_before,
        t2.trans_id as subsequent_trans_id_
    from balances_before_and_after t1
    left join balances_before_and_after t2
    on t1.date = t2.date and t1.account_id = t2.account_id and abs(t1.balance_after-t2.balance_before) < 0.1
)
select 
    trans_id,
    account_id,
    `date`,
    `type`,
    operation,
    amount,
    balance_after,
    k_symbol,
    bank,
    account,
    balance_before,
    subsequent_trans_id_ as subsequent_trans_id
from subsequent_trans_ids
