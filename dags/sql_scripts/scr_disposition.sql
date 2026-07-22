-- "disp_id" int;"client_id" int;"account_id" string;"type" low cardinality string
CREATE TABLE IF NOT EXISTS src_disposition (
    disp_id Int32,
    client_id Int32,
    account_id String,
    type LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (disp_id);