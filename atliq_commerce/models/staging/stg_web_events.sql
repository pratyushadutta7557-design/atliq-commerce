
{{ config(materialized='view') }}

select
    session_id,
    event_id,
    customer_id,
    event_type,
    event_ts,
    product_id,
    order_id,
    device,
    user_agent,
    utm_source
from {{ source('silver', 'web_events') }}
