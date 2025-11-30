-- models/transformation/dim_hcp.sql
{{ config(
    materialized='incremental',
    unique_key='hcp_id',
    incremental_strategy='merge'
) }}

with base as (
  select
        h.hcp_id,
        h.hcp_name,
        h.specialty_code,
        h.hospital_id,
        h.region_code,
    s.specialty_code,
    hs.hospital_id,
    current_timestamp() as valid_from
  from {{ ref('int_hpc') }} h
  left join {{ ref('int_specialty') }} s on h.specialty_code=s.specialty_code
  left join {{ ref('int_hospital') }} hs on h.hospital_id=hs.hospital_id
)

select
  {{ dbt_utils.generate_surrogate_key(['hcp_id','hcp_name','specialty_code','hospital_id','region_code','hospital_id']) }} as hcp_key,
  *
  , to_timestamp_ntz('9999-12-31') as valid_to
  , true as is_current
from base

{% if is_incremental() %}
-- For brevity: rely on dbt-scd-type-2 pattern or snapshot; otherwise manage merges + close-out prior versions.
{% endif %}
