-- "order_id" int;"account_id" string;"bank_to" string;"account_to" string;"amount" float;"k_symbol" low cardinality string
CREATE TABLE IF NOT EXISTS src_permanent_orders (
    order_id Int32,
    account_id String,
    bank_to String,
    account_to String,
    amount Float32,
    k_symbol LowCardinality(String)
) ENGINE = MergeTree()
ORDER BY (order_id);