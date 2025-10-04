{{ config(materialized='view') }}

select
    drug_id,
    drug_name,
    therapeutic_area_code,
    product_line_code
from {{ source('healthcare', 'stg_drug') }}
