with gold_customer_sales as (

    select
        c.customer_id,
        c.full_name,
        c.created_at,
        sum(f.total_amount) as total_spent,
        count(distinct f.order_id) as total_orders
    from {{ ref('silver_fact_orders') }} f
    join {{ ref('silver_dim_customers') }} c
        on f.customer_key = c.customer_key
    group by
        c.customer_id,
        c.full_name,
        created_at
)

select *
from gold_customer_sales
order by total_spent desc