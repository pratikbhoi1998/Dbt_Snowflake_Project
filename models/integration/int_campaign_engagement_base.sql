{{ config(
    materialized='incremental',
    unique_key='hcp_id',
    incremental_strategy='merge'
) }}

with email as (
    select * from {{ ref('stg_campaign_email') }}
),
phone as (
    select * from {{ ref('stg_campaign_phone') }}
),
event as (
    select * from {{ ref('stg_engagement_event') }}
),
final_cte as (
select * from email
union all
select * from phone
union all
select * from event
)
select * from final_cte
{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}