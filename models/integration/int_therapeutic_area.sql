{{ config(
    materialized='incremental',
    unique_key='therapeutic_area_code',
    incremental_strategy='merge'
) }}

with base as (
    select
        t.therapeutic_area_code,
        t.therapeutic_area_name,
        t.ingested_at
    from {{ ref('stg_therapeutic_area') }} t
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
