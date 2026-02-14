SELECT *
FROM {{ ref('bronze_orders') }}
WHERE order_date > CURRENT_DATE