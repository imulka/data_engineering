with transactions as (

    select *
    from {{ ref('stg_building_materials_transactions') }}

),

monthly_product_revenue as (

    select
        year,
        month,
        product_category,

        count(*) as transaction_count,
        sum(units) as total_units,
        round(sum(revenue), 2) as total_revenue,
        round(avg(unit_price), 2) as avg_unit_price

    from transactions

    group by
        year,
        month,
        product_category

)

select *
from monthly_product_revenue
order by
    year,
    month,
    total_revenue desc