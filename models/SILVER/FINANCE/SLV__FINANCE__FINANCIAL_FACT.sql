select
    fact_id,
    entity_id,
    transaction_ts,
    status,
    region,
    country,
    cast(amount  as numeric(18,2)) as amount,
    cast(cost    as numeric(18,2)) as cost,
    cast(revenue as numeric(18,2)) as revenue
from {{ ref('BRZ__FINANCE__FINANCIAL_FACT') }}
where fact_id is not null
