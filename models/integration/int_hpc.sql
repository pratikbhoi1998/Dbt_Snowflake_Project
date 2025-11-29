{{ config(
    materialized='incremental',
    unique_key='hcp_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        h.hcp_id,
        h.hcp_name,
        h.specialty_code,
        h.hospital_id,
        h.region_code,
        ingested_at
    from {{ ref('stg_hcp') }} h
)

select
*
from base b

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
