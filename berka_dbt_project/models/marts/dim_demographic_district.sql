select 
    district_id,
    district_name,
    region,
    number_of_inhabitants,
    number_of_municipalities_fewer_than_499_inhabitants,
    number_of_municipalities_between_500_and_1999_inhabitants,
    number_of_municipalities_between_2000_and_9999_inhabitants,
    number_of_municipalities_greater_than_10000_inhabitants,
    (
        number_of_municipalities_fewer_than_499_inhabitants +
        number_of_municipalities_between_500_and_1999_inhabitants +
        number_of_municipalities_between_2000_and_9999_inhabitants +
        number_of_municipalities_greater_than_10000_inhabitants
    ) as total_number_of_municipalities,
    number_of_cities,
    urban_ratio,
    average_salary,
    ratio_of_enterpreneurs
from {{ ref('stg_berka_raw__demographic_districts') }}