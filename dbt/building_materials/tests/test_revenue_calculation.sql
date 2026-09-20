select *
from {{ ref('stg_building_materials_transactions') }}
where abs(revenue - (units * unit_price)) > 0.01