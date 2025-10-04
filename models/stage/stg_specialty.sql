{{ config(materialized='view') }}

select
    specialty_code,
    specialty_name
from {{ source('healthcare', 'stg_specialty') }}
