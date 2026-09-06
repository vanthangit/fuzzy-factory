select
    product_id,
    product_name,
    created_at as product_created_at
from {{ ref('stg_fuzzyfactory__products') }}
