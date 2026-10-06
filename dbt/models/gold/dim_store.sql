{{ config(order_by=['store_id']) }}

select
    store_id,
    argMax(store_name, tuple(kafka_timestamp, kafka_partition, kafka_offset)) as store_name,
    cast(
        argMax(city, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as city,
    cast(
        argMax(region, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as region,
    cast(
        argMax(store_type, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as store_type
from {{ ref('stg_ecommerce_events_current') }}
group by store_id
