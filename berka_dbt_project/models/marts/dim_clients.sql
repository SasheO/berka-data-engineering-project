with client as (
    select
        client_id,
        date_of_birth,
        sex,
        district_id
    from {{ ref('stg_berka_raw__clients') }}
    ),
district as (
    select  
        district_id,
        -- district_valid_from date [not null, note: ""] -- TODO: implement client district historical SCD 4
        district_name
    from {{ ref('stg_berka_raw__demographic_districts') }}
)
SELECT 
    client.client_id,
    client.date_of_birth,
    client.sex,
    client.district_id,
    -- district.district_valid_from,
    district.district_name
FROM client
left join district using (district_id)