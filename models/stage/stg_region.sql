{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    region_code,
    region_name,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_region') }}
