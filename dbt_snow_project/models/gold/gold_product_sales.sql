with gold_product_sales as (

    select
        p.product_name,
        p.category,
        sum(f.total_quantity) as total_quantity_sold,
        sum(f.total_amount) as total_revenue,
        sum(f.total_profit) as total_profit
    from {{ ref('silver_fact_orders_by_products') }} f
    join {{ ref('silver_dim_products') }} p
        on f.product_key = p.product_key
    group by
        p.product_name,
        p.category

)

select *
from gold_product_sales
order by total_revenue desc
