select
    o.order_id,
    o.price_usd as order_total,
    oi.items_total,
    o.price_usd - oi.items_total as diff
from {{ ref('fct_orders') }} o
join (
    select order_id, sum(price_usd) as items_total
    from {{ ref('fct_order_items') }}
    group by 1
) oi on oi.order_id = o.order_id
where abs(o.price_usd - oi.items_total) > 0.01
