{% snapshot hcp_snapshot %}

{{ config(
    target_schema='SNAPSHOTS',
    unique_key = 'hcp_id',
    strategy='check',
    check_cols=['hcp_id','hcp_name','specialty_code','hospital_id','region_code','ingested_at']
) }}

select
hcp_id,
hcp_name,
specialty_code,
hospital_id,
region_code,
ingested_at
from {{ ref('int_hpc') }}

{% endsnapshot %}
