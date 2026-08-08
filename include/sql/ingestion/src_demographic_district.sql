INSERT INTO {{ params.db_schema }}.{{ params.table_name }} 
(
    district_id,
    district_name,
    region,
    num_inhabitants,
    num_municipalities_less_499,
    num_municipalities_500_1999,
    num_municipalities_2000_9999,
    num_municipalities_gt_10000,
    num_cities,
    urban_ratio,
    average_salary,
    unemployment_rate_95,
    unemployment_rate_96,
    entrepreneurs_per_1000,
    num_crimes_95,
    num_crimes_96
)
SELECT 
    A1, -- district_id UInt64,
    A2, -- district_name String,
    A3, -- region String,
    A4,
    A5,
    A6,
    A7,
    A8,
    A9,
    A10,
    A11,
    if(A12 = '?', -1, toFloat32(A12)),  -- deal with malformed records with ?
    A13,
    A14,
    if(A15 = '?', -1, toInt64(A15)),  -- deal with malformed records with ?
    A16
FROM s3(
    '{{ var.value.minio_endpoint }}/{{ params.minio_bucket_name }}/{{  params.file_name  }}',
    '{{ var.value.minio_username }}', 
    '{{ var.value.minio_password }}', 
    'CSVWithNames'
)
SETTINGS input_format_null_as_default = 1, -- set malformed records to their default
format_csv_delimiter = ';'