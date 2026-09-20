select *
from {{ ref('stg_building_materials_transactions') }}
where unit_price <= 0