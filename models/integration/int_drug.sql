{{ config(
    materialized='incremental',
    unique_key='drug_id',
    incremental_strategy='merge'
) }}

with base as (
    select
        d.drug_id,
        d.drug_name,
        d.therapeutic_area_code,
        d.product_line_code,
        d.ingested_at
    from {{ ref('stg_drug') }} d
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
