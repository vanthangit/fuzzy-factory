with orders as (
    select * from {{ ref('stg_fuzzyfactory__orders') }}
),

sessions as (
    select
        website_session_id,
        utm_source,
        utm_campaign,
        utm_content,
        device_type
    from {{ ref('stg_fuzzyfactory__website_sessions') }}
),

products as (
    select product_id, product_name from {{ ref('stg_fuzzyfactory__products') }}
)

select
    o.order_id,
    o.created_at as ordered_at,
    o.website_session_id,
    o.user_id,
    o.primary_product_id,
    p.product_name as primary_product_name,
    o.items_purchased,
    o.price_usd,
    o.cogs_usd,
    o.price_usd - o.cogs_usd as margin_usd,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type
from orders o
left join sessions s on s.website_session_id = o.website_session_id
left join products p on p.product_id = o.primary_product_id
