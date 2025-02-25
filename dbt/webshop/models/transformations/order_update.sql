{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT
    id,
    customerid,
    ordertimestamp + (CURRENT_DATE - MAX(ordertimestamp) OVER()) AS ordertimestamp,
    shippingaddressid,
    total,
    shippingcost,
    created,
    updated
FROM webshop.order

{% if is_incremental() %}
WHERE ordertimestamp > (SELECT MAX(ordertimestamp) FROM {{ this }})
{% endif %}
