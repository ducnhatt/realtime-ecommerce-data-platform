{{ config(order_by=['reviewer_id']) }}

select
    reviewer_id,
    tupleElement(
        argMax(
            tuple(reviewer_name),
            tuple(kafka_timestamp, kafka_partition, kafka_offset)
        ),
        1
    ) as reviewer_name
from {{ ref('stg_ecommerce_events_current') }}
group by reviewer_id
