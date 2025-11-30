{{ config(
    materialized='incremental',
    unique_key='rep_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        r.rep_id,
        r.rep_name,
        r.region_code,
        r.ingested_at
    from {{ ref('stg_rep') }} r
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
