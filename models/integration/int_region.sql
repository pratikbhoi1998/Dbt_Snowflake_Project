{{ config(
    materialized='incremental',
    unique_key='region_code',
    incremental_strategy='merge'
) }}

with base as (
    select
        r.region_code,
        r.region_name,
        r.ingested_at
    from {{ ref('stg_region') }} r
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
