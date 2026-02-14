select f.customer_id
from {{ ref('silver_fact_orders') }} f
left join {{ ref('silver_dim_customers') }} d
    on f.customer_id = d.customer_id
where d.customer_id is null