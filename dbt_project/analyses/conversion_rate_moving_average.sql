-- Business question: conversion tren website dang tang hay giam, tru
-- nhieu ngau nhien ngay-qua-ngay? Conversion rate cua 1 ngay don le qua
-- nhieu nhieu de hanh dong duoc.
-- Ky thuat: window frame ROWS BETWEEN de tinh moving average 7 ngay tren
-- CTE conversion theo tung ngay.

with daily_sessions as (
    select
        date_trunc('day', session_started_at) as session_date,
        count(*) as sessions,
        sum(is_converted_session::int) as orders
    from {{ ref('fct_sessions') }}
    group by 1
),

daily_conversion as (
    select
        session_date,
        sessions,
        orders,
        round(orders::decimal / nullif(sessions, 0) * 100, 2) as conversion_rate_pct
    from daily_sessions
)

select
    session_date,
    sessions,
    orders,
    conversion_rate_pct,
    round(
        avg(conversion_rate_pct) over (
            order by session_date
            rows between 6 preceding and current row
        ),
        2
    ) as conversion_rate_7d_moving_avg
from daily_conversion
order by session_date

-- Doc ket qua: so sanh conversion_rate_7d_moving_avg hien tai voi gia tri
-- cua ~30/60/90 ngay truoc. 1 khoang cach ben vung giua rate hang ngay va
-- moving average quanh 1 moc thoi gian cu the la tin hieu 1 thay doi
-- (lander moi, thay doi gia, dich chuyen kenh) da tac dong that.
