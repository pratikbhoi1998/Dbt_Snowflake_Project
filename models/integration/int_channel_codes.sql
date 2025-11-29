{{ config(
    materialized='incremental',
    unique_key='channel_code',
    incremental_strategy='merge'
) }}

with base as (
    select
        c.channel_code,
        c.channel_name,
        c.ingested_at
    from {{ ref('stg_channel_codes') }} c
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
