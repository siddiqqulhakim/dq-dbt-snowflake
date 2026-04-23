select * from {{ source('operations', 'financial_fact') }}
