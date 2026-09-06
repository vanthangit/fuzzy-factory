with source as (
    select * from {{ source('raw', 'products') }}
)

select
    product_id,
    created_at,
    trim(product_name) as product_name
from source
