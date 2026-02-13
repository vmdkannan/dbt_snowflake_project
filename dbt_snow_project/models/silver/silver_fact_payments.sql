with payments_clean as (

    select *
    from {{ ref('bronze_payments') }}
    where payment_id is not null

),

silver_fact_payments as (

    select
        row_number() over (order by p.payment_id) as payment_key,  -- surrogate key
        p.payment_id,
        o.order_id,
        c.customer_key,
        p.payment_method,
        p.payment_status,
        p.payment_amount,
        p.payment_date
    from payments_clean p
    join {{ ref('bronze_orders') }} o
        on p.order_id = o.order_id
    join {{ ref('silver_dim_customers') }} c
        on o.customer_id = c.customer_id

)

select *
from silver_fact_payments
