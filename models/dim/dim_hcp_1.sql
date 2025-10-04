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
        current_timestamp() as valid_from
    from {{ ref('int_hcp') }} h
)

select
    {{ dbt_utils.generate_surrogate_key(['hcp_id','hcp_name','specialty_code','hospital_id','region_code']) }} as hcp_key,
    b.hcp_id,
    b.hcp_name,
    b.specialty_code,
    b.hospital_id,
    b.region_code,
    b.valid_from,
    to_timestamp_ntz('9999-12-31') as valid_to,
    true as is_current
from base b

{% if is_incremental() %}
-- For incremental runs we rely on merge semantics provided by "incremental_strategy='merge'.
-- If your adapter does not support merge you can implement a custom MERGE statement in a post-hook or use dbt snapshots to manage SCD2.
{% endif %}
