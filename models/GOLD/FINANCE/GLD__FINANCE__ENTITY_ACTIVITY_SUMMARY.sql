select
    e.entity_id,
    e.entity_name,
    e.region,
    e.country,
    e.status,
    count(distinct a.fact_id)   as total_activities,
    sum(a.amount)               as total_activity_amount,
    sum(a.quantity)             as total_quantity,
    min(a.event_ts)             as first_activity_ts,
    max(a.event_ts)             as last_activity_ts
from {{ ref('SLV__FINANCE__ENTITY_DIM') }} e
left join {{ ref('SLV__FINANCE__ACTIVITY_FACT') }} a
    on e.entity_id = a.entity_id
group by 1, 2, 3, 4, 5
