-- "loan_id" int;"account_id" string;"date" date yymmdd;"amount" float;"duration" int;"payments" float;"status" low cardinality string/single char
CREATE TABLE IF NOT EXISTS src_loans (
    loan_id Int32,
    account_id String,
    date Date,
    amount Float32,
    duration Int32,
    payments Float32,
    status LowCardinality(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (account_id, date, loan_id);