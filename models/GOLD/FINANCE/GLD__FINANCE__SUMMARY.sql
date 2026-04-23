select
    f.region,
    f.country,
    f.status,
    date_trunc('month', f.transaction_ts)   as transaction_month,
    count(f.fact_id)                         as transaction_count,
    sum(f.amount)                            as total_amount,
    sum(f.cost)                              as total_cost,
    sum(f.revenue)                           as total_revenue,
    avg(f.amount)                            as avg_amount
from {{ ref('SLV__FINANCE__FINANCIAL_FACT') }} f
group by 1, 2, 3, 4
