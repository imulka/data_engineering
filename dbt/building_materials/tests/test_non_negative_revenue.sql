select *
from {{ ref('stg_building_materials_transactions') }}
where revenue < 0