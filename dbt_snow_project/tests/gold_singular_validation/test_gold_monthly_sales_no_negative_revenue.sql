SELECT *
FROM {{ ref('gold_monthly_sales') }}
WHERE total_revenue < 0