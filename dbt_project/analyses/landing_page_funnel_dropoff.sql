-- Business question: buoc nao trong funnel mua hang lam ro ri nhieu
-- session nhat, va co hoi doanh thu neu sua duoc buoc do la bao nhieu?
-- Ky thuat: CTE stack + window function (int_session_pageviews_sequenced
-- da tinh san thu tu pageview trong session), join voi seed
-- `funnel_steps` (bang tra cuu tinh - xem seeds/funnel_steps.csv) de map
-- pageview_url sang buoc funnel thay vi hardcode CASE WHEN trong SQL.

with pageviews as (
    select * from {{ ref('int_session_pageviews_sequenced') }}
),

funnel_step_lookup as (
    select * from {{ ref('funnel_steps') }}
),

pageview_funnel_step as (
    select
        pv.website_pageview_id,
        pv.website_session_id,
        max(fsl.step_number) as funnel_step
    from pageviews pv
    join funnel_step_lookup fsl
        on pv.pageview_url like fsl.url_pattern || '%'
    group by pv.website_pageview_id, pv.website_session_id
),

session_progress as (
    select
        website_session_id,
        max(funnel_step) as max_funnel_step
    from pageview_funnel_step
    group by website_session_id
),

max_step_histogram as (
    select
        max_funnel_step,
        count(distinct website_session_id) as sessions_at_max_step
    from session_progress
    group by 1
),

step_names as (
    select distinct step_number, step_name from funnel_step_lookup
),

step_reach_counts as (
    select
        sn.step_number,
        sn.step_name,
        sum(msh.sessions_at_max_step) as sessions_reaching_step
    from step_names sn
    join max_step_histogram msh on msh.max_funnel_step >= sn.step_number
    group by sn.step_number, sn.step_name
)

select
    step_number,
    step_name,
    sessions_reaching_step,
    lag(sessions_reaching_step) over (order by step_number) as sessions_prior_step,
    round(
        sessions_reaching_step::decimal
        / nullif(lag(sessions_reaching_step) over (order by step_number), 0)
        * 100,
        2
    ) as pct_retained_from_prior_step
from step_reach_counts
order by step_number

-- Doc ket qua: buoc co pct_retained_from_prior_step thap nhat la diem ro
-- ri lon nhat. Lay so session mat o buoc do nhan voi conversion rate
-- trung binh toan site va AOV de uoc luong doanh thu thu hoi duoc neu cai
-- thien buoc nay.
