select
    fact_id,
    entity_id,
    location_id,
    event_ts,
    status,
    region,
    country,
    channel,
    cast(quantity as numeric(12,2)) as quantity,
    cast(price    as numeric(18,2)) as price,
    cast(amount   as numeric(18,2)) as amount
from {{ ref('BRZ__HR__ACTIVITY_FACT') }}
where fact_id is not null
