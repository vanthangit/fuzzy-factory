with source as (
    select * from {{ source('raw', 'website_sessions') }}
)

select
    website_session_id,
    created_at,
    user_id,
    is_repeat_session::boolean as is_repeat_session,
    lower(trim(utm_source))   as utm_source,
    lower(trim(utm_campaign)) as utm_campaign,
    lower(trim(utm_content))  as utm_content,
    lower(trim(device_type))  as device_type,
    http_referer
from source
