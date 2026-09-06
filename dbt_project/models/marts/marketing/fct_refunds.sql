select
    order_item_refund_id,
    created_at as refunded_at,
    order_item_id,
    order_id,
    refund_amount_usd
from {{ ref('stg_fuzzyfactory__order_item_refunds') }}
