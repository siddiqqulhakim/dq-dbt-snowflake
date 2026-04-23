select * from {{ source('finance', 'entity_dim') }}
