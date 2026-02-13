with bronze_products_clean as (
    select *
    from {{ ref('bronze_products') }}
    where product_id is not null
),

deduped_products as (
    select
        *,
        row_number() over (
            partition by product_id
            order by created_at desc
        ) as rn
    from bronze_products_clean
),

silver_dim_products as (
    select
        row_number() over (order by product_id) as product_key,  -- surrogate key
        product_id,
        initcap(product_name) as product_name,
        initcap(category) as category,
        initcap(brand) as brand,
        price,
        cost,
        created_at
    from deduped_products
    where rn = 1

)

select *
from silver_dim_products
