-- "client_id" int;"birth_number" string;"district_id" int
-- birth number: the number is in the form YYMMDD for men,
-- the number is in the form YYMM+50DD for
-- women,
-- where YYMMDD is the date of birth
CREATE TABLE IF NOT EXISTS src_clients (
    client_id UInt64,
    birth_number String,
    district_id UInt64
) ENGINE = MergeTree()
ORDER BY (client_id);