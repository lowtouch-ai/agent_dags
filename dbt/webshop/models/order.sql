{{ config(
    materialized='table'
) }}

WITH max_time AS (
    SELECT MAX(ordertimestamp) AS max_timestamp FROM {{ ref('order_seed') }}
),

day_diff AS (
    SELECT DATE_PART('day', NOW() - max_timestamp) AS day_difference FROM max_time
)

SELECT 
    id,
    customerid,
    ordertimestamp + INTERVAL '1 day' * (SELECT day_difference FROM day_diff) AS ordertimestamp,
    shippingaddressid,
    total,
    shippingcost,
    created,
    updated
FROM {{ ref('order_seed') }}

