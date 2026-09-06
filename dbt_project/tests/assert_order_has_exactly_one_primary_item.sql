select
    order_id,
    count(*) as primary_item_count
from {{ ref('fct_order_items') }}
where is_primary_item
group by order_id
having count(*) != 1
