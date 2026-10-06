CREATE TABLE IF NOT EXISTS gold.dim_product
(
    asin String,
    product_name String,
    category LowCardinality(String),
    sub_category LowCardinality(String),
    brand LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY asin;

CREATE TABLE IF NOT EXISTS gold.dim_store
(
    store_id String,
    store_name String,
    city LowCardinality(String),
    region LowCardinality(String),
    store_type LowCardinality(String)
)
ENGINE = MergeTree()
ORDER BY store_id;

CREATE TABLE IF NOT EXISTS gold.dim_reviewer
(
    reviewer_id String,
    reviewer_name Nullable(String)
)
ENGINE = MergeTree()
ORDER BY reviewer_id;

CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date Date,
    year UInt16,
    quarter UInt8,
    month UInt8,
    week_of_year UInt8,
    day_of_week UInt8,
    is_weekend Bool
)
ENGINE = MergeTree()
ORDER BY date;

CREATE TABLE IF NOT EXISTS gold.fact_ecommerce_event
(
    kafka_topic LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset Int64,

    asin String,
    store_id String,
    reviewer_id String,
    review_date Date,
    review_timestamp DateTime64(3, 'UTC'),

    order_id String,
    payment_id String,
    shipping_id String,

    unit_price Decimal(18, 2),
    quantity UInt32,
    total_amount Decimal(18, 2),
    overall_rating UInt8,
    helpful_yes UInt32,
    total_vote UInt32,

    payment_method LowCardinality(String),
    payment_status LowCardinality(String),
    shipping_method LowCardinality(String),
    carrier_name LowCardinality(String),
    shipping_status LowCardinality(String),

    kafka_timestamp DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    ingest_date Date,
    validation_status Enum8('VALID' = 1, 'WARNING' = 2)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(ingest_date)
PRIMARY KEY (review_date, asin, store_id)
ORDER BY
(
    review_date,
    asin,
    store_id,
    kafka_topic,
    kafka_partition,
    kafka_offset
);
