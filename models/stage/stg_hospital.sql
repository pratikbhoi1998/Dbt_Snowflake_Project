{{ config(materialized='view') }}

select
    hospital_id,
    hospital_name,
    region_code
from {{ source('healthcare', 'stg_hospital') }}
