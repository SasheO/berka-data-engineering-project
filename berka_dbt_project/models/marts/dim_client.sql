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
        district_name
    from {{ ref('stg_berka_raw__demographic_districts') }}
)
SELECT 
    client.client_id,
    client.date_of_birth,
    client.sex,
    client.district_id,
    district.district_name
FROM client
left join district using (district_id)