{{ config(
    materialized='incremental',
    unique_key='campaign_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        c.campaign_id,
        c.drug_id,
        c.channel_code,
        c.start_date,
        c.end_date,
        c.ingested_at
    from {{ ref('stg_campaign') }} c
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
