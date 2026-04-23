select * from {{ source('operations', 'entity_dim') }}
