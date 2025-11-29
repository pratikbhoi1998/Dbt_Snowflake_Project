{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

with phone as (
    select
        call_id as engagement_id,
        phone.campaign_id,
        phone.hcp_id,
        phone.drug_id,
        'PHONE' as channel_code,
        try_to_timestamp(call_date) as engaged_ts,
        phone.status,
        0 as opened_flag,
        0 as sent_flag,
        0 as clicked_flag,
        case when phone.status = 'Connected' then 1 else 0 end as connected_flag,
        0 as attended_flag,
        try_cast(duration_sec as number) as call_duration_sec,
    to_timestamp(ingested_at) as ingested_at
    from {{ source('healthcare', 'stg_campaign_phone') }} phone
)

select * from phone
