select * from {{ source('hr', 'activity_fact') }}
