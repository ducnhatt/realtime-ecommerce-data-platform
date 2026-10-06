select *
from {{ source('staging', 'ecommerce_events_conflicts') }}
