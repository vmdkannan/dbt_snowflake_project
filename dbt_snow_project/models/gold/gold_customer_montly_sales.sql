with gold_customer_monthly_sales as (

    select
        c.customer_id,
        c.full_name,
        date_trunc('month', f.order_date) as order_month,
        sum(f.total_amount) as monthly_spent,
        count(distinct f.order_id) as total_orders
    from {{ ref('silver_fact_orders') }} f
    join {{ ref('silver_dim_customers') }} c
        on f.customer_key = c.customer_key
    group by
        c.customer_id,
        c.full_name,
        date_trunc('month', f.order_date)
)

select *
from gold_customer_monthly_sales
order by order_month desc, monthly_spent desc