-- table to store transaction order via subsequent_transaction_id since source from kaggle does not have this data
CREATE OR REPLACE TABLE {{ params.db_schema }}.src_transactions_enriched (
    trans_id UInt64,
    account_id String,
    `date` String,
    `type` LowCardinality(String),
    operation LowCardinality(String),
    amount Float32,
    balance_after Float32,
    k_symbol LowCardinality(String),
    bank String,
    account String,
    balance_before Float32,
    subsequent_trans_id UInt64 default 0 -- defaults to , the real transaction ID is set when transaction_oder is figured out and the last transaction of the day has this as 0
) ENGINE = ReplacingMergeTree()
ORDER BY (trans_id);