-- Business question: kenh marketing nao dang convert tot nhung bi
-- underinvest, va thu hang do doi the nao qua tung thang?
-- Ky thuat: CTE + RANK() OVER (PARTITION BY month) - xep hang moi kenh
-- trong pham vi cung 1 thang, khong so voi toan bo lich su.

with monthly_channel_sessions as (
    select
        date_trunc('month', session_started_at) as session_month,
        utm_source,
        utm_campaign,
        count(*) as sessions,
        sum(is_converted_session::int) as orders,
        sum(revenue_usd) as revenue_usd
    from {{ ref('fct_sessions') }}
    where utm_source is not null
    group by 1, 2, 3
),

monthly_channel_performance as (
    select
        session_month,
        utm_source,
        utm_campaign,
        sessions,
        orders,
        revenue_usd,
        round(orders::decimal / nullif(sessions, 0) * 100, 2) as conversion_rate_pct,
        round(revenue_usd / nullif(sessions, 0), 2) as revenue_per_session_usd
    from monthly_channel_sessions
)

select
    *,
    rank() over (
        partition by session_month order by conversion_rate_pct desc
    ) as conversion_rank_in_month,
    rank() over (
        partition by session_month order by sessions desc
    ) as volume_rank_in_month
from monthly_channel_performance
order by session_month, conversion_rank_in_month

-- Doc ket qua: kenh co conversion_rank_in_month tot (top 2-3) nhung
-- volume_rank_in_month kem la tin hieu "underinvested" - convert tot nhung
-- dang it duoc rot ngan sach/traffic. Ghep voi revenue_per_session_usd de
-- uoc luong quy mo co hoi neu dau tu them cho kenh do.
