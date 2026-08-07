
select 
    district_id,
    1995 as "year",
    unemployment_rate_in_95 as unemployment_rate,
    number_of_committed_crimes_in_95 as number_of_committed_crimes
from {{ ref('stg_berka_raw__demographic_districts') }}

union all

select 
    district_id,
    1996 as "year",
    unemployment_rate_in_96 as unemployment_rate,
    number_of_committed_crimes_in_96 as number_of_committed_crimes
from {{ ref('stg_berka_raw__demographic_districts') }}