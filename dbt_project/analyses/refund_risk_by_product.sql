-- Business question: san pham nao co ty le hoan tien bat thuong cao, va
-- dang "am tham" an mon bao nhieu margin?
-- Ky thuat: join order_items voi order_item_refunds cung grain
-- (fct_order_items vs fct_refunds deu o muc order_item_id), roi so sanh ty
-- le hoan tien tung san pham voi trung binh toan cong ty bang window
-- function - de "bat thuong" duoc dinh nghia theo chinh du lieu, khong
-- phai 1 nguong cung hard-code.

with item_refund_status as (
    select
        oi.order_item_id,
        oi.product_id,
        oi.price_usd,
        oi.margin_usd,
        r.order_item_id is not null as is_refunded,
        coalesce(r.refund_amount_usd, 0) as refund_amount_usd
    from {{ ref('fct_order_items') }} oi
    left join {{ ref('fct_refunds') }} r on r.order_item_id = oi.order_item_id
),

product_refund_stats as (
    select
        product_id,
        count(*) as items_sold,
        sum(is_refunded::int) as items_refunded,
        sum(refund_amount_usd) as total_refunded_usd,
        sum(margin_usd) as total_margin_usd,
        round(sum(is_refunded::int)::decimal / nullif(count(*), 0) * 100, 2) as refund_rate_pct
    from item_refund_status
    group by 1
)

select
    p.product_name,
    prs.items_sold,
    prs.items_refunded,
    prs.refund_rate_pct,
    prs.total_refunded_usd,
    prs.total_margin_usd,
    round(avg(prs.refund_rate_pct) over (), 2) as company_avg_refund_rate_pct,
    round(prs.refund_rate_pct - avg(prs.refund_rate_pct) over (), 2) as pct_points_above_avg
from product_refund_stats prs
join {{ ref('dim_products') }} p on p.product_id = prs.product_id
order by pct_points_above_avg desc

-- Doc ket qua: san pham co pct_points_above_avg duong lon la outlier
-- refund-risk. total_refunded_usd o dong do la chi phi truc tiep; nhan
-- toc do tang truong cua items_sold voi refund_rate_pct de du bao margin
-- se mat neu san pham scale ma khong sua (QA, mo ta/size ro rang, dong goi).
