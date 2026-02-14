select dc.first_name, fo.order_item_id, dp.product_name, fo.total_price, (fo.total_price*(10/100)) as discount, (fo.total_price-discount) as Net_Amount from {{ ref('silver_dim_customers') }} dc 
join {{ ref('silver_fact_orders_by_products') }} fo
on dc.customer_key = fo.customer_key
join {{ ref('silver_dim_products') }} dp
on dp.product_id = fo.product_id