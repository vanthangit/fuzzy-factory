with sessions as (
    select * from {{ ref('stg_fuzzyfactory__website_sessions') }}
),

landing_pages as (
    select website_session_id, pageview_url as landing_page
    from {{ ref('int_session_pageviews_sequenced') }}
    where is_landing_page
),

exit_pages as (
    select website_session_id, pageview_url as exit_page
    from {{ ref('int_session_pageviews_sequenced') }}
    where is_exit_page
),

pageview_counts as (
    select website_session_id, max(session_pageview_count) as pageview_count
    from {{ ref('int_session_pageviews_sequenced') }}
    group by 1
),

orders as (
    select website_session_id, order_id, price_usd
    from {{ ref('stg_fuzzyfactory__orders') }}
)

select
    s.website_session_id,
    s.created_at as session_started_at,
    s.user_id,
    s.is_repeat_session,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type,
    s.http_referer,
    lp.landing_page,
    ep.exit_page,
    coalesce(pv.pageview_count, 0) as pageview_count,
    o.order_id,
    o.price_usd as revenue_usd,
    o.order_id is not null as is_converted_session
from sessions s
left join landing_pages lp on lp.website_session_id = s.website_session_id
left join exit_pages ep on ep.website_session_id = s.website_session_id
left join pageview_counts pv on pv.website_session_id = s.website_session_id
left join orders o on o.website_session_id = s.website_session_id
