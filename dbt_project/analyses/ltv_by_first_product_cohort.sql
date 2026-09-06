-- Business question: san pham mua dau tien nao du bao LTV cao nhat, va
-- khach nhom do quay lai mua tiep nhanh co nao?
-- Ky thuat: cohort theo san pham mua dau tien (int_user_orders_sequenced
-- da danh so thu tu don hang va tinh next_order_at bang LEAD), roi tong
-- hop doanh thu tren tung user va cuon len theo cohort.

with first_orders as (
    select
        user_id,
        primary_product_id as first_product_id,
        ordered_at as first_order_at,
        next_order_at as second_order_at,
        datediff('day', ordered_at, next_order_at) as days_to_second_order
    from {{ ref('int_user_orders_sequenced') }}
    where user_order_sequence = 1
),

customer_ltv as (
    select
        user_id,
        sum(price_usd) as lifetime_revenue_usd,
        sum(cogs_usd) as lifetime_cogs_usd,
        count(*) as lifetime_orders
    from {{ ref('int_user_orders_sequenced') }}
    group by 1
)

select
    p.product_name as first_product_purchased,
    count(distinct fo.user_id) as customers,
    round(avg(cl.lifetime_revenue_usd), 2) as avg_ltv_revenue_usd,
    round(avg(cl.lifetime_revenue_usd - cl.lifetime_cogs_usd), 2) as avg_ltv_margin_usd,
    round(avg(cl.lifetime_orders), 2) as avg_orders_per_customer,
    round(avg(fo.days_to_second_order), 1) as avg_days_to_second_order
from first_orders fo
join customer_ltv cl on cl.user_id = fo.user_id
join {{ ref('dim_products') }} p on p.product_id = fo.first_product_id
group by 1
order by avg_ltv_revenue_usd desc

-- Doc ket qua: dong dau tien la san pham lam "cua ngo" tot nhat cho khach
-- LTV cao. Neu san pham do khong dang duoc uu tien quang cao/vi tri hien
-- thi nhat, do la co hoi tai phan bo - do bang avg_ltv_revenue_usd (cohort
-- cao nhat) tru avg_ltv_revenue_usd (cohort thap nhat), nhan voi so khach
-- co the dich chuyen sang cohort top moi quy.
