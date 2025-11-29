{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    rep_id,
    rep_name,
    region_code,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_rep') }}
