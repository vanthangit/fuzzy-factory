with pageviews as (
    select * from {{ ref('stg_fuzzyfactory__website_pageviews') }}
),

sequenced as (
    select
        website_pageview_id,
        website_session_id,
        created_at,
        pageview_url,
        row_number() over (
            partition by website_session_id order by created_at, website_pageview_id
        ) as pageview_number,
        count(*) over (partition by website_session_id) as session_pageview_count,
        lead(pageview_url) over (
            partition by website_session_id order by created_at, website_pageview_id
        ) as next_pageview_url
    from pageviews
)

select
    *,
    pageview_number = 1 as is_landing_page,
    pageview_number = session_pageview_count as is_exit_page
from sequenced
