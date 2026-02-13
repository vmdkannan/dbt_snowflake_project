with silver_fact_orders_by_products as (

    select
        row_number() over (order by oi.order_item_id) as sales_key,   -- surrogate key
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        c.customer_key,
        dp.product_key,
        dp.product_id,
        dp.product_name,
        dp.category,
        o.order_date,
        oi.quantity,
        oi.unit_price,
        oi.total_price,
        (oi.quantity * (oi.unit_price - dp.cost)) as profit
    from {{ ref('bronze_order_items') }} oi
    join {{ ref('bronze_orders') }} o
        on oi.order_id = o.order_id
    join {{ ref('silver_dim_customers') }} c
        on o.customer_id = c.customer_id
    join {{ ref('silver_dim_products') }} dp
        on dp.product_id = oi.product_id

)

select *
from silver_fact_orders_by_products
