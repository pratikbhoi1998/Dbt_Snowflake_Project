{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    channel_code,
    channel_name,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_channel_codes') }}
