with orders_clean as (
    select *
    from {{ ref('bronze_orders') }}
    where order_id is not null
),

fact_orders as (
    select
        row_number() over (order by o.order_id) as order_key,  -- surrogate key
        o.order_id,
        c.customer_key,
        o.order_date,
        o.order_status,
        sum(oi.quantity) as total_quantity,
        sum(oi.total_price) as total_amount,
        sum(oi.quantity * (oi.unit_price - p.cost)) as total_profit
    from orders_clean o
    join {{ ref('silver_dim_customers') }} c
        on o.customer_id = c.customer_id
    join {{ ref('bronze_order_items') }} oi
        on o.order_id = oi.order_id
    join {{ ref('silver_dim_products') }} p
        on oi.product_id = p.product_id
    group by
        o.order_id,
        c.customer_key,
        o.order_date,
        o.order_status
)

select *
from fact_orders
