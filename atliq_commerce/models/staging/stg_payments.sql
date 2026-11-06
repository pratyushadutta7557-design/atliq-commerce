{{ config(materialized='view') }}

SELECT
    payment_id,
    order_id,
    amount,
    method,
    paid_at,
    updated_at
FROM {{ source('silver', 'payments') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY paid_at, payment_id
) = 1