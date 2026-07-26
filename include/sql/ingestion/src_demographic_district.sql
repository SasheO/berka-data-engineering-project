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
SELECT A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16
FROM s3(
    '{{ params.minio_endpoint }}/{{ params.minio_bucket_name }}/{{  params.file_name  }}',
    '{{ params.minio_username }}', 
    '{{ params.minio_password }}', 
    'CSVWithNames'
)
SETTINGS input_format_allow_errors_ratio = 0.05, -- ignore up to 5% malformed records (in case there are trailing rows in the CSV file or something like that)
format_csv_delimiter = ';'