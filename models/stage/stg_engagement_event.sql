{{ 
    config(
        materialized = 'table',
        pre_hook = "TRUNCATE TABLE IF EXISTS {{ this }}"
    ) 
}}

with event as (
    select
        event_id as engagement_id,
        event.campaign_id,
        event.hcp_id,
        event.drug_id,
        'EVENT' as channel_code,
        try_to_timestamp(event_date) as engaged_ts,
        event.status,
        0 as opened_flag,
        0 as sent_flag,
        0 as clicked_flag,
        0 as connected_flag,
        case when event.status = 'Attended' then 1 else 0 end as attended_flag,
        null::number as call_duration_sec,
    to_timestamp(ingested_at) as ingested_at
    from {{ source('healthcare', 'stg_campaign_event') }} event
)

select * from event
