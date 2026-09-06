with source as (
    select * from {{ source('raw', 'website_pageviews') }}
)

select
    website_pageview_id,
    created_at,
    website_session_id,
    lower(trim(pageview_url)) as pageview_url
from source
