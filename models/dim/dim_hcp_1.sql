{{ config(materialized="view") }}

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "hcp_id",
                "hcp_name",
                "specialty_code",
                "hospital_id",
                "region_code"
            ]
        )
    }} as hcp_key,
    hcp_id,
    hcp_name,
    specialty_code,
    hospital_id,
    region_code,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref("hcp_snapshot_1") }}
