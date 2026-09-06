with source as (
    select * from {{ source('raw', 'orders') }}
)

select
    order_id,
    created_at,
    website_session_id,
    user_id,
    primary_product_id,
    items_purchased,
    price_usd,
    cogs_usd
from source
