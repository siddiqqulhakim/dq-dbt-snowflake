select * from {{ source('finance', 'activity_fact') }}
