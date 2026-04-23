select
    'FINANCE'    as domain,
    region,
    country,
    status,
    date_trunc('month', transaction_ts) as transaction_month,
    count(fact_id)                       as transaction_count,
    sum(amount)                          as total_amount,
    sum(revenue)                         as total_revenue
from {{ ref('SLV__FINANCE__FINANCIAL_FACT') }}
group by 1, 2, 3, 4, 5

union all

select
    'OPERATIONS' as domain,
    region,
    country,
    status,
    date_trunc('month', transaction_ts) as transaction_month,
    count(fact_id)                       as transaction_count,
    sum(amount)                          as total_amount,
    sum(revenue)                         as total_revenue
from {{ ref('SLV__OPERATIONS__FINANCIAL_FACT') }}
group by 1, 2, 3, 4, 5

union all

select
    'HR'         as domain,
    region,
    country,
    status,
    date_trunc('month', transaction_ts) as transaction_month,
    count(fact_id)                       as transaction_count,
    sum(amount)                          as total_amount,
    sum(revenue)                         as total_revenue
from {{ ref('SLV__HR__FINANCIAL_FACT') }}
group by 1, 2, 3, 4, 5
