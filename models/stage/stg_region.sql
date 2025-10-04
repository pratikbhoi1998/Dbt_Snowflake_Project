{{ config(materialized='view') }}

select
    region_code,
    region_name
from {{ source('healthcare', 'stg_region') }}
