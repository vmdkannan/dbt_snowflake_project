SELECT f.*
FROM {{ ref('silver_fact_orders') }} f
LEFT JOIN {{ ref('silver_dim_customers') }} d
    ON f.customer_key = d.customer_key
WHERE d.customer_key IS NULL