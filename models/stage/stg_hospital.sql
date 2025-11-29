{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    hospital_id,
    hospital_name,
    region_code,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_hospital') }}
