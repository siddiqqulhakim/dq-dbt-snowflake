select
    entity_id,
    entity_name,
    status,
    region,
    country,
    created_at,
    updated_at
from {{ ref('BRZ__HR__ENTITY_DIM') }}
where entity_id is not null
qualify row_number() over (partition by entity_id order by updated_at desc) = 1
