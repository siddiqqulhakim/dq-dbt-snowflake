select * from {{ source('hr', 'entity_dim') }}
