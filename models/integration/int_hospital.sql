{{ config(
    materialized='incremental',
    unique_key='hospital_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        h.hospital_id,
        h.hospital_name,
        h.region_code,
        h.ingested_at
    from {{ ref('stg_hospital') }} h
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
