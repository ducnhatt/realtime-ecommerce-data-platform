select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    count() as copies
from {{ ref('fact_ecommerce_event') }}
group by kafka_topic, kafka_partition, kafka_offset
having copies != 1
