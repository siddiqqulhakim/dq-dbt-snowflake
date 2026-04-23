select * from {{ source('hr', 'financial_fact') }}
