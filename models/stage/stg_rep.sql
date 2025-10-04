{{ config(materialized='view') }}

select
    rep_id,
    rep_name,
    region_code
from {{ source('healthcare', 'stg_rep') }}
