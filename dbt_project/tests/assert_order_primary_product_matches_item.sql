select
    o.order_id,
    o.primary_product_id as order_primary_product_id,
    oi.product_id as item_primary_product_id
from {{ ref('fct_orders') }} o
join {{ ref('fct_order_items') }} oi
    on oi.order_id = o.order_id
    and oi.is_primary_item
where oi.product_id != o.primary_product_id
