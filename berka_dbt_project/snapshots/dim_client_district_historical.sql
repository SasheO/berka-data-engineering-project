{% snapshot dim_client_district_historical %}
  {{
    config(
      unique_key='client_id',
      strategy='check',
      check_cols=['district_id']
    )
  }}
  SELECT 
      client_id,
      district_id,
      district_name
  from {{ ref('dim_client') }}
{% endsnapshot %}