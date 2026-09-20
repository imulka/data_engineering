with transactions as (

    select *
    from {{ ref('stg_building_materials_transactions') }}

),

aggregated as (

    select
        region,
        count(*) as transaction_count,
        sum(units) as total_units,
        round(sum(revenue), 2) as total_revenue,
        round(avg(unit_price), 2) as avg_unit_price,
        round(avg(mortgage_rate), 2) as avg_mortgage_rate

    from transactions

    group by region

)

select *
from aggregated
order by total_revenue desc