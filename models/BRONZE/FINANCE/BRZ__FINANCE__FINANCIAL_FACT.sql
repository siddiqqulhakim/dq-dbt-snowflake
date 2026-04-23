select * from {{ source('finance', 'financial_fact') }}
