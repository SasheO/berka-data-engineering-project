with clients_with_gender_and_dob as (
    select
        client_id,
        district_id,
        if(CAST(SUBSTRING(birth_number, 3, 2) AS INT) > 50, 'F', 'M') AS gender,
        CASE
            -- If month > 50, subtract 50 for females
            WHEN CAST(SUBSTRING(birth_number, 3, 2) AS INT) > 50 THEN
                concat(
                    SUBSTRING(birth_number, 1, 2),
                    lpad(toString(CAST(SUBSTRING(birth_number, 3, 2) AS INT) - 50), 2, '0'), -- ensure this is 2 characters wide for mm using lpad
                    SUBSTRING(birth_number, 5, 2)
                )
            -- Otherwise keep as is
            ELSE birth_number
        END AS dob
        from {{ source('berka_raw', 'src_clients') }}
    )
select
    client_id,
    district_id,
    gender,
    makeDate32(
        toInt32(concat('19', substring(dob, 1, 2))),
        toInt32(substring(dob, 3, 2)),
        toInt32(substring(dob, 5, 2))
    ) AS date_of_birth -- needs to be Date32 because all clients are born before in 1900s, and some are before 1970
from clients_with_gender_and_dob