{{ config(order_by=['asin']) }}

select
    asin,
    argMax(product_name, tuple(kafka_timestamp, kafka_partition, kafka_offset)) as product_name,
    cast(
        argMax(category, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as category,
    cast(
        argMax(sub_category, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as sub_category,
    cast(
        argMax(brand, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
        'LowCardinality(String)'
    ) as brand
from {{ ref('stg_ecommerce_events_current') }}
group by asin
