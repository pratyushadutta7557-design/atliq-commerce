
{{ config(materialized='table') }}

with deduplicated_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        item_price,
        created_at,
        row_number() over (
            partition by order_id, product_id, quantity, item_price, created_at
            order by order_item_id
        ) as row_num
    from {{ ref('stg_order_items') }}
),

valid_items as (
    select
        order_item_id,
        order_id,
        product_id,
        quantity,
        item_price,
        created_at
    from deduplicated_items
    where row_num = 1
),

sales_orders as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status
    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_customers') }} c
        on o.customer_id = c.customer_id
    where coalesce(c.is_test, false) = false
)

select
    i.order_item_id,
    i.order_id,
    o.customer_id,
    i.product_id,
    o.order_date,
    o.status,
    i.quantity,
    i.item_price,
    cast(i.quantity * i.item_price as decimal(18,2)) as gross_revenue
from valid_items i
inner join sales_orders o
    on i.order_id = o.order_id
