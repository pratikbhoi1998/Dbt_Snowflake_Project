{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

select
    hcp_id,
    hcp_name,
    specialty_code,
    hospital_id,
    region_code,
    to_timestamp(ingested_at) as ingested_at
from {{ source('healthcare', 'stg_hcp') }}
