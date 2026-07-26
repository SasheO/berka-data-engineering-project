INSERT INTO {{ params.db_schema }}.{{ params.table_name }} 
SELECT * 
FROM s3(
    '{{ params.minio_endpoint }}/{{ params.minio_bucket_name }}/{{  params.file_name  }}',
    '{{ params.minio_username }}', 
    '{{ params.minio_password }}', 
    'CSVWithNames'
)
SETTINGS input_format_allow_errors_ratio = 0.05, -- ignore up to 5% malformed records (in case there are trailing rows in the CSV file or something like that)
format_csv_delimiter = ';'