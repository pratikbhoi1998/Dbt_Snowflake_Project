{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    campaign_id,
    drug_id,
    channel_code,
    try_to_date(start_date) as start_date,
    try_to_date(end_date) as end_date,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_campaign') }}
