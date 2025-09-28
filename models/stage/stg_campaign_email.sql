select
    email_id as engagement_id,
    email.campaign_id,
    email.hcp_id,
    email.drug_id,
    'EMAIL' as channel_code,
    SEND_DATE as engaged_ts,
    email.status,
case  when email.status = 'Opened' then 1 else 0 end as opened_flag,
case  when email.status = 'Sent' then 1 else 0 end as sent_flag,
case  when email.status = 'Clicked' then 1 else 0 end as clicked_flag,
    0 as connected_flag,
    0 as attended_flag,
    null::number as call_duration_sec
from healthcare_db.stage.stg_campaign_email email