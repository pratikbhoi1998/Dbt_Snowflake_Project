{{ config(materialized='view') }}

select
    therapeutic_area_code,
    therapeutic_area_name
from {{ source('healthcare', 'stg_therapeutic_area') }}
