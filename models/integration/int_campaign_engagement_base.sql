{{ config(materialized='table') }}

with email as (
    select * from {{ ref('stg_campaign_email') }}
),
phone as (
    select * from {{ ref('stg_campaign_phone') }}
),
event as (
    select * from {{ ref('stg_engagement_event') }}
)

select * from email
union all
select * from phone
union all
select * from event
