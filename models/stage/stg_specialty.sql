{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    specialty_code,
    specialty_name,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_specialty') }}
