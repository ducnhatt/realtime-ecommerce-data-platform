CREATE TABLE IF NOT EXISTS staging.ecommerce_events_raw
(
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset Int64,

    reviewer_id String,
    reviewer_name Nullable(String),

    asin String,
    product_name String,
    category LowCardinality(String),
    sub_category LowCardinality(String),
    brand LowCardinality(String),

    store_id String,
    store_name String,
    city LowCardinality(String),
    region LowCardinality(String),
    store_type LowCardinality(String),

    review_timestamp DateTime64(3, 'UTC'),
    review_date Date,

    order_id String,
    unit_price Decimal(18, 2),
    quantity UInt32,
    total_amount Decimal(18, 2),

    overall_rating UInt8,
    helpful_yes UInt32,
    total_vote UInt32,

    payment_id String,
    payment_method LowCardinality(String),
    payment_status LowCardinality(String),

    shipping_id String,
    shipping_method LowCardinality(String),
    carrier_name LowCardinality(String),
    shipping_status LowCardinality(String),

    kafka_timestamp DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    ingest_date Date,
    validation_status Enum8('VALID' = 1, 'WARNING' = 2),
    validation_warnings Array(String),
    source_contract_version LowCardinality(String),
    silver_schema_version LowCardinality(String),

    silver_row_hash FixedString(64),
    load_batch_id String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(ingest_date)
ORDER BY
(
    kafka_topic,
    kafka_partition,
    kafka_offset,
    silver_row_hash
);
