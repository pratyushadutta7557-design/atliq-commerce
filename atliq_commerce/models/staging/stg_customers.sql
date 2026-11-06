{{ config(materialized='view') }}

SELECT
    customer_id,
    customer_name,
    email,
    city,
    signup_date,
    updated_at,
    CASE
        WHEN LOWER(TRIM(email)) LIKE '%@atliq-test.com' THEN TRUE
        ELSE FALSE
    END AS is_test
FROM {{ source('silver', 'customers') }}