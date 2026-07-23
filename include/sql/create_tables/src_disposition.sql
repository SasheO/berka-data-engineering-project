-- "disp_id" int;"client_id" int;"account_id" string;"type" low cardinality string
CREATE TABLE IF NOT EXISTS {{ params.db_schema }}.src_disposition (
    disp_id UInt64,
    client_id UInt64,
    account_id String,
    type LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (disp_id);