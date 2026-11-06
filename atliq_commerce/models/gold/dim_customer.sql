
{{ config(materialized='table') }}

with customers as (
    select
        customer_id,
        customer_name,
        city,
        signup_date
    from {{ ref('stg_customers') }}
    where coalesce(is_test, false) = false
),

first_orders as (
    select
        customer_id,
        min(order_date) as first_order_date
    from {{ ref('stg_orders') }}
    group by customer_id
)

select
    c.customer_id,
    c.customer_name,
    c.city,
    c.signup_date,
    date_trunc('MONTH', c.signup_date) as signup_cohort,
    f.first_order_date
from customers c
left join first_orders f
    on c.customer_id = f.customer_id
