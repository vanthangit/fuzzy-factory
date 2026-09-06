with orders as (
    select * from {{ ref('stg_fuzzyfactory__orders') }}
)

select
    order_id,
    user_id,
    created_at as ordered_at,
    primary_product_id,
    price_usd,
    cogs_usd,
    row_number() over (
        partition by user_id order by created_at, order_id
    ) as user_order_sequence,
    lead(created_at) over (
        partition by user_id order by created_at, order_id
    ) as next_order_at
from orders
