with source as (

    select *
    from {{ ref('building_materials_transactions') }}

),

cleaned as (

    select
        date as transaction_date,
        year,
        month,
        week_of_year,

        lower(trim(region)) as region,
        lower(trim(product_category)) as product_category,
        upper(trim(sku)) as sku,
        lower(trim(channel)) as channel,
        lower(trim(customer_type)) as customer_type,

        units,
        round(unit_price, 2) as unit_price,
        round(revenue, 2) as revenue,

        housing_starts_index,
        lumber_price_index,
        mortgage_rate

    from source

)

select *
from cleaned