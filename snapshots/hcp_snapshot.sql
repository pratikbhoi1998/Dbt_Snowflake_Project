{% snapshot hcp_snapshot %}

{{ config(
    target_schema='SNAPSHOTS',
    unique_key = 'hcp_id',
    strategy='check',
    check_cols=['hcp_id','hcp_name','specialty_code','hospital_id','region_code']
) }}

select
hcp_id,
hcp_name,
specialty_code,
hospital_id,
region_code
from {{ ref('int_hcp') }}

{% endsnapshot %}
