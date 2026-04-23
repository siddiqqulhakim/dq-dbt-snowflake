select * from {{ source('operations', 'activity_fact') }}
