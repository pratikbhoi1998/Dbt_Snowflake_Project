{{ config(materialized="table") }}

with base as (
    select
        trim(hcp_id) as hcp_id,
        hcp_name,
        nullif(trim(specialty_code), '') as specialty_code,
        nullif(trim(hospital_id), '') as hospital_id,
        region_code
    from {{ ref('stg_hcp') }}
    where coalesce(trim(hcp_id), '') <> ''
)

select
    hcp_id,
    hcp_name,
    specialty_code,
    hospital_id,
    region_code
from base
