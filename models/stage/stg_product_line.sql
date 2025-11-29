{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    product_line_code,
    product_line_name,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_product_line') }}
