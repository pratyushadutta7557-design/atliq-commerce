
{{ config(materialized='table') }}

select
    session_id,
    min(event_ts) as session_start,
    cast(min(event_ts) as date) as session_date,
    max(customer_id) as customer_id,
    max(device) as device,
    count(*) as events,
    max(case when event_type = 'product_view' then 1 else 0 end)
        as reached_product_view,
    max(case when event_type = 'add_to_cart' then 1 else 0 end)
        as reached_add_to_cart,
    max(case when event_type = 'checkout' then 1 else 0 end)
        as reached_checkout,
    max(case when event_type = 'purchase' then 1 else 0 end)
        as reached_purchase
from {{ ref('stg_web_events') }}
where session_id is not null
group by session_id
