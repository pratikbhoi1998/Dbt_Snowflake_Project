{{ config(materialized='view') }}

select
    product_line_code,
    product_line_name
from {{ source('healthcare', 'stg_product_line') }}
