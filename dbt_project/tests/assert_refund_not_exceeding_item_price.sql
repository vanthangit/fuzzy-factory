select
    r.order_item_refund_id,
    r.order_item_id,
    r.refund_amount_usd,
    oi.price_usd as original_price_usd
from {{ ref('fct_refunds') }} r
join {{ ref('fct_order_items') }} oi on oi.order_item_id = r.order_item_id
where r.refund_amount_usd > oi.price_usd + 0.01
