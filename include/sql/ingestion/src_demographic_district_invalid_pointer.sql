INSERT INTO {{ params.db_schema }}.src_demographic_district 
(
district_id,
district_name,
region
)
SELECT 0, 'INVALID', 'INVALID'
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ params.db_schema }}.src_demographic_district
    WHERE district_id = 0
);