SELECT throwIf(
    count() != 5,
    'Gold must contain exactly the five v1 star-schema tables'
)
FROM system.tables
WHERE database = 'gold';

SELECT throwIf(
    countIf(engine != 'MergeTree') != 0,
    'All Gold v1 tables must use MergeTree'
)
FROM system.tables
WHERE database = 'gold';

SELECT throwIf(
    arraySort(groupArray(tuple(name, type))) != arraySort([
        tuple('asin', 'String'),
        tuple('product_name', 'String'),
        tuple('category', 'LowCardinality(String)'),
        tuple('sub_category', 'LowCardinality(String)'),
        tuple('brand', 'LowCardinality(String)')
    ]),
    'gold.dim_product schema mismatch'
)
FROM system.columns
WHERE database = 'gold' AND table = 'dim_product';

SELECT throwIf(
    arraySort(groupArray(tuple(name, type))) != arraySort([
        tuple('store_id', 'String'),
        tuple('store_name', 'String'),
        tuple('city', 'LowCardinality(String)'),
        tuple('region', 'LowCardinality(String)'),
        tuple('store_type', 'LowCardinality(String)')
    ]),
    'gold.dim_store schema mismatch'
)
FROM system.columns
WHERE database = 'gold' AND table = 'dim_store';

SELECT throwIf(
    arraySort(groupArray(tuple(name, type))) != arraySort([
        tuple('reviewer_id', 'String'),
        tuple('reviewer_name', 'Nullable(String)')
    ]),
    'gold.dim_reviewer schema mismatch'
)
FROM system.columns
WHERE database = 'gold' AND table = 'dim_reviewer';

SELECT throwIf(
    arraySort(groupArray(tuple(name, type))) != arraySort([
        tuple('date', 'Date'),
        tuple('year', 'UInt16'),
        tuple('quarter', 'UInt8'),
        tuple('month', 'UInt8'),
        tuple('week_of_year', 'UInt8'),
        tuple('day_of_week', 'UInt8'),
        tuple('is_weekend', 'Bool')
    ]),
    'gold.dim_date schema mismatch'
)
FROM system.columns
WHERE database = 'gold' AND table = 'dim_date';

SELECT throwIf(
    arraySort(groupArray(tuple(name, type))) != arraySort([
        tuple('kafka_topic', 'LowCardinality(String)'),
        tuple('kafka_partition', 'UInt16'),
        tuple('kafka_offset', 'Int64'),
        tuple('asin', 'String'),
        tuple('store_id', 'String'),
        tuple('reviewer_id', 'String'),
        tuple('review_date', 'Date'),
        tuple('review_timestamp', 'DateTime64(3, \'UTC\')'),
        tuple('order_id', 'String'),
        tuple('payment_id', 'String'),
        tuple('shipping_id', 'String'),
        tuple('unit_price', 'Decimal(18, 2)'),
        tuple('quantity', 'UInt32'),
        tuple('total_amount', 'Decimal(18, 2)'),
        tuple('overall_rating', 'UInt8'),
        tuple('helpful_yes', 'UInt32'),
        tuple('total_vote', 'UInt32'),
        tuple('payment_method', 'LowCardinality(String)'),
        tuple('payment_status', 'LowCardinality(String)'),
        tuple('shipping_method', 'LowCardinality(String)'),
        tuple('carrier_name', 'LowCardinality(String)'),
        tuple('shipping_status', 'LowCardinality(String)'),
        tuple('kafka_timestamp', 'DateTime64(3, \'UTC\')'),
        tuple('ingested_at', 'DateTime64(3, \'UTC\')'),
        tuple('ingest_date', 'Date'),
        tuple('validation_status', 'Enum8(\'VALID\' = 1, \'WARNING\' = 2)')
    ]),
    'gold.fact_ecommerce_event schema mismatch'
)
FROM system.columns
WHERE database = 'gold' AND table = 'fact_ecommerce_event';

SELECT throwIf(
    any(partition_key) != 'toYYYYMM(ingest_date)',
    'unexpected Gold fact partition key'
)
FROM system.tables
WHERE database = 'gold' AND name = 'fact_ecommerce_event';

SELECT throwIf(
    any(primary_key) != 'review_date, asin, store_id',
    'unexpected Gold fact primary key'
)
FROM system.tables
WHERE database = 'gold' AND name = 'fact_ecommerce_event';

SELECT throwIf(
    any(sorting_key) != 'review_date, asin, store_id, kafka_topic, kafka_partition, kafka_offset',
    'unexpected Gold fact sorting key'
)
FROM system.tables
WHERE database = 'gold' AND name = 'fact_ecommerce_event';

SELECT throwIf(
    countIf(
        (name = 'dim_product' AND sorting_key != 'asin')
        OR (name = 'dim_store' AND sorting_key != 'store_id')
        OR (name = 'dim_reviewer' AND sorting_key != 'reviewer_id')
        OR (name = 'dim_date' AND sorting_key != 'date')
    ) != 0,
    'unexpected Gold dimension sorting key'
)
FROM system.tables
WHERE database = 'gold'
  AND name IN ('dim_product', 'dim_store', 'dim_reviewer', 'dim_date');

SELECT
    'CLICKHOUSE_GOLD_SCHEMA_CONTRACT_OK' AS marker,
    (SELECT count() FROM gold.fact_ecommerce_event) AS fact_rows,
    (SELECT count() FROM gold.dim_product) AS product_rows,
    (SELECT count() FROM gold.dim_store) AS store_rows,
    (SELECT count() FROM gold.dim_reviewer) AS reviewer_rows,
    (SELECT count() FROM gold.dim_date) AS date_rows;
