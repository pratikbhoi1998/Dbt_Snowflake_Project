{{ config(materialized='view') }}

select
    channel_code,
    channel_name
from {{ source('healthcare', 'stg_channel_codes') }}
