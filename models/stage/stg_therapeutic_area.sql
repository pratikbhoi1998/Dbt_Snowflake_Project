{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}
select
    therapeutic_area_code,
    therapeutic_area_name,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_therapeutic_area') }}
