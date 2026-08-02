select
    district_id,
    district_name,
    region,
    num_inhabitants as number_of_inhabitants,
    num_municipalities_less_499 as number_of_municipalities_fewer_than_499_inhabitants,
    num_municipalities_500_1999 as number_of_municipalities_between_500_and_1999_inhabitants,
    num_municipalities_2000_9999 as number_of_municipalities_between_2000_and_9999_inhabitants,
    num_municipalities_gt_10000 as number_of_municipalities_greater_than_10000_inhabitants,
    num_cities as number_of_cities,
    urban_ratio,
    average_salary,
    unemployment_rate_95 as unemployment_rate_in_95,
    unemployment_rate_96 as unemployment_rate_in_96,
    (entrepreneurs_per_1000/1000) as ratio_of_enterpreneurs,
    num_crimes_95 as number_of_committed_crimes_in_95,
    num_crimes_96 as number_of_committed_crimes_in_96
from {{ source('berka_raw', 'src_demographic_district') }}
