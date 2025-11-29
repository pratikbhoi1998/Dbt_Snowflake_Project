{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    drug_id,
    drug_name,
    therapeutic_area_code,
    product_line_code,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_drug') }}
