{{ config(
    materialized='incremental',
    unique_key='specialty_code',
    incremental_strategy='merge'
) }}

with base as (
    select
        s.specialty_code,
        s.specialty_name,
        s.ingested_at
    from {{ ref('stg_specialty') }} s
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
