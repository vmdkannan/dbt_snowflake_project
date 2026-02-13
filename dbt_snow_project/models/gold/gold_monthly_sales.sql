with gold_monthly_sales as (

    select
        d.year,
        d.month,
        d.month_name,
        sum(f.total_amount) as total_revenue,
        sum(f.total_quantity) as total_quantity,
        sum(f.total_profit) as total_profit,
        count(distinct f.order_id) as total_orders
    from {{ ref('silver_fact_orders') }} f
    join {{ ref('silver_dim_date') }} d
        on f.order_date = d.full_date
    group by
        d.year,
        d.month,
        d.month_name

)

select *
from gold_monthly_sales
order by year, month
