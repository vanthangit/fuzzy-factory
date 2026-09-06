with source as (
    select * from {{ source('raw', 'order_item_refunds') }}
)

select
    order_item_refund_id,
    created_at,
    order_item_id,
    order_id,
    refund_amount_usd
from source
