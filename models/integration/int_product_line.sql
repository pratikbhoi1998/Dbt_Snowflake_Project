{{ config(
    materialized='incremental',
    unique_key='product_line_code',
    incremental_strategy='merge'
) }}

with base as (
    select
        p.product_line_code,
        p.product_line_name,
        p.ingested_at
    from {{ ref('stg_product_line') }} p
)

select
    *
from base

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
