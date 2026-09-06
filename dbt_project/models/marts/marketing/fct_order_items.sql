select
    order_item_id,
    created_at as ordered_at,
    order_id,
    product_id,
    is_primary_item,
    price_usd,
    cogs_usd,
    price_usd - cogs_usd as margin_usd
from {{ ref('stg_fuzzyfactory__order_items') }}
