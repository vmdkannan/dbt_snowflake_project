with bronze_customers_clean as (

    select *
    from {{ ref('bronze_customers') }}
    where customer_id is not null

),

deduped_customers as (

    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc
        ) as rn
    from bronze_customers_clean

),

silver_dim_customers as (

    select
        row_number() over (order by customer_id) as customer_key,  -- surrogate key
        customer_id,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        initcap(trim(first_name)) || ' ' || initcap(trim(last_name)) as full_name,
        lower(trim(email)) as email,
        trim(phone) as phone,
        initcap(trim(city)) as city,
        upper(trim(state)) as state,
        upper(trim(country)) as country,
        signup_date as created_at,
        updated_at
    from deduped_customers
    where rn = 1

)

select *
from silver_dim_customers
