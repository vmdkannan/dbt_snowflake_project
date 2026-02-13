with date_spine as (

    select
        dateadd(day, seq4(), '2024-07-01') as date_day
    from table(generator(rowcount => 90))   -- 90 days for practice

),

silver_dim_date as (

    select
        row_number() over (order by date_day) as date_key,  -- surrogate key
        date_day as full_date,
        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        monthname(date_day) as month_name,
        day(date_day) as day,
        dayofweek(date_day) as day_of_week,
        dayname(date_day) as day_name,
        weekofyear(date_day) as week_of_year,
        case 
            when dayofweek(date_day) in (0,6) then true 
            else false 
        end as is_weekend
    from date_spine

)

select *
from silver_dim_date