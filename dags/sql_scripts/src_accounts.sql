-- "account_id" string;"district_id"int;"frequency" low cardinality string;"date" date yymmdd
CREATE TABLE IF NOT EXISTS src_accounts (
    account_id String,
    district_id Int32,
    frequency LowCardinality(String),
    date Date
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, date);