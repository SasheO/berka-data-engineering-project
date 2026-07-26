INSERT INTO {{ params.db_schema }}.{{ params.table_name }} 
SELECT * 
FROM s3(
    '{{ params.minio_endpoint }}/{{ params.minio_bucket_name }}/{{  params.file_name  }}',
    '{{ params.minio_username }}', 
    '{{ params.minio_password }}', 
    'CSVWithNames'
)