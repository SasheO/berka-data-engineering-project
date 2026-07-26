-- A1 (district_id/code) int;  A2 (district name) string;  A3 (region) String; 
--A4 (no. of inhabitants) int; A5 (no. of municipalities with inhabitants < 499) int; 
-- A6 (no. of municipalities with inhabitants 500­1999) int; A7 (no. of municipalities with inhabitants 2000­9999) int; 
-- A8 (no. of municipalities with inhabitants >10000) int; A9 (no. of cities) int; 
-- A10 (ratio of urban inhabitants) float; A11 (average_salary) float; A12 (unemploymant rate '95) float; 
-- A13 (unemploymant rate '96) float; 
-- A14 (no. of enterpreneurs per 1000 inhabitants) int; A15 (no. of commited crimes '95) int; 
-- A16 (no. of commited crimes '96) int
CREATE TABLE IF NOT EXISTS {{ params.db_schema }}.src_demographic_district (
    district_id UInt64,
    district_name String,
    region String,
    num_inhabitants UInt64,
    num_municipalities_less_499 UInt64,
    num_municipalities_500_1999 UInt64,
    num_municipalities_2000_9999 UInt64,
    num_municipalities_gt_10000 UInt64,
    num_cities UInt64,
    urban_ratio Float32,
    average_salary Float32,
    unemployment_rate_95 Float32,
    unemployment_rate_96 Float32,
    entrepreneurs_per_1000 UInt64,
    num_crimes_95 UInt64,
    num_crimes_96 UInt64
) ENGINE = MergeTree()
ORDER BY (district_id);