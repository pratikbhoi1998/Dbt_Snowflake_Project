{{ config(materialized='view') }}

select
    hcp_id,
    hcp_name,
    specialty_code,
    hospital_id,
    region_code
from {{ source('healthcare', 'stg_hcp') }}
