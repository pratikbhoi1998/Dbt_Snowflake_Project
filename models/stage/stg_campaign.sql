{{ config(materialized='view') }}

select
    campaign_id,
    drug_id,
    channel_code,
    try_to_date(start_date) as start_date,
    try_to_date(end_date) as end_date
from {{ source('healthcare', 'stg_campaign') }}
