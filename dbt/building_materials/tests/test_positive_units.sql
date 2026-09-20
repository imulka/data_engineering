select *
from {{ ref('stg_building_materials_transactions') }}
where units <= 0