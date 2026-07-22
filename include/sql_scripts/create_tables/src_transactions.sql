-- "trans_id" int;"account_id" string;"date" date yymmdd;"type" low cardinality string;"operation" low cardinality string;"amount" float;"balance" float;"k_symbol" low cardinality string;"bank" string;"account" string
CREATE TABLE IF NOT EXISTS src_transactions (
    trans_id UInt64,
    account_id String,
    date Date,
    type LowCardinality(String),
    operation LowCardinality(String),
    amount Float32,
    balance Float32,
    k_symbol LowCardinality(String),
    bank String,
    account String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, date, trans_id);