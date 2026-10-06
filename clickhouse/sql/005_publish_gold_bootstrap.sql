SELECT throwIf(
    count() = 0,
    'Cannot publish Gold from an empty staging current view'
)
FROM staging.ecommerce_events_current;

SELECT throwIf(
    (SELECT count() FROM staging.ecommerce_events_conflicts) != 0,
    'Cannot publish Gold while staging lineage conflicts exist'
);

DROP TABLE IF EXISTS gold.dim_product_next;
DROP TABLE IF EXISTS gold.dim_store_next;
DROP TABLE IF EXISTS gold.dim_reviewer_next;
DROP TABLE IF EXISTS gold.dim_date_next;
DROP TABLE IF EXISTS gold.fact_ecommerce_event_next;

CREATE TABLE gold.dim_product_next AS gold.dim_product;
CREATE TABLE gold.dim_store_next AS gold.dim_store;
CREATE TABLE gold.dim_reviewer_next AS gold.dim_reviewer;
CREATE TABLE gold.dim_date_next AS gold.dim_date;
CREATE TABLE gold.fact_ecommerce_event_next AS gold.fact_ecommerce_event;

INSERT INTO gold.dim_product_next
SELECT
    asin,
    argMax(product_name, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(category, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(sub_category, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(brand, tuple(kafka_timestamp, kafka_partition, kafka_offset))
FROM staging.ecommerce_events_current
GROUP BY asin;

INSERT INTO gold.dim_store_next
SELECT
    store_id,
    argMax(store_name, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(city, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(region, tuple(kafka_timestamp, kafka_partition, kafka_offset)),
    argMax(store_type, tuple(kafka_timestamp, kafka_partition, kafka_offset))
FROM staging.ecommerce_events_current
GROUP BY store_id;

INSERT INTO gold.dim_reviewer_next
SELECT
    reviewer_id,
    tupleElement(
        argMax(
            tuple(reviewer_name),
            tuple(kafka_timestamp, kafka_partition, kafka_offset)
        ),
        1
    ) AS reviewer_name
FROM staging.ecommerce_events_current
GROUP BY reviewer_id;

INSERT INTO gold.dim_date_next
WITH bounds AS
(
    SELECT
        min(review_date) AS min_date,
        max(review_date) AS max_date
    FROM staging.ecommerce_events_current
)
SELECT
    addDays(min_date, day_offset) AS date,
    toYear(date) AS year,
    toUInt8(toQuarter(date)) AS quarter,
    toUInt8(toMonth(date)) AS month,
    toUInt8(toISOWeek(date)) AS week_of_year,
    toUInt8(toDayOfWeek(date)) AS day_of_week,
    toDayOfWeek(date) IN (6, 7) AS is_weekend
FROM bounds
ARRAY JOIN range(toUInt32(dateDiff('day', min_date, max_date) + 1)) AS day_offset;

INSERT INTO gold.fact_ecommerce_event_next
SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    asin,
    store_id,
    reviewer_id,
    review_date,
    review_timestamp,
    order_id,
    payment_id,
    shipping_id,
    unit_price,
    quantity,
    total_amount,
    overall_rating,
    helpful_yes,
    total_vote,
    payment_method,
    payment_status,
    shipping_method,
    carrier_name,
    shipping_status,
    kafka_timestamp,
    ingested_at,
    ingest_date,
    validation_status
FROM staging.ecommerce_events_current;

SELECT throwIf(
    (SELECT count() FROM gold.fact_ecommerce_event_next)
        != (SELECT count() FROM staging.ecommerce_events_current),
    'Gold fact next row count does not match staging current'
);

SELECT throwIf(
    (SELECT count() FROM gold.fact_ecommerce_event_next)
        != (SELECT uniqExact(tuple(kafka_topic, kafka_partition, kafka_offset))
            FROM gold.fact_ecommerce_event_next),
    'Gold fact next contains duplicate Kafka lineage'
);

SELECT throwIf(
    (SELECT count() FROM gold.dim_product_next)
        != (SELECT uniqExact(asin) FROM gold.dim_product_next),
    'Gold product dimension key is not unique'
);

SELECT throwIf(
    (SELECT count() FROM gold.dim_store_next)
        != (SELECT uniqExact(store_id) FROM gold.dim_store_next),
    'Gold store dimension key is not unique'
);

SELECT throwIf(
    (SELECT count() FROM gold.dim_reviewer_next)
        != (SELECT uniqExact(reviewer_id) FROM gold.dim_reviewer_next),
    'Gold reviewer dimension key is not unique'
);

SELECT throwIf(
    count() != 0,
    'Gold fact has an orphan product key'
)
FROM gold.fact_ecommerce_event_next AS fact
LEFT JOIN gold.dim_product_next AS dim ON fact.asin = dim.asin
WHERE isNull(dim.asin)
SETTINGS join_use_nulls = 1;

SELECT throwIf(
    count() != 0,
    'Gold fact has an orphan store key'
)
FROM gold.fact_ecommerce_event_next AS fact
LEFT JOIN gold.dim_store_next AS dim ON fact.store_id = dim.store_id
WHERE isNull(dim.store_id)
SETTINGS join_use_nulls = 1;

SELECT throwIf(
    count() != 0,
    'Gold fact has an orphan reviewer key'
)
FROM gold.fact_ecommerce_event_next AS fact
LEFT JOIN gold.dim_reviewer_next AS dim ON fact.reviewer_id = dim.reviewer_id
WHERE isNull(dim.reviewer_id)
SETTINGS join_use_nulls = 1;

SELECT throwIf(
    count() != 0,
    'Gold fact has an orphan review date'
)
FROM gold.fact_ecommerce_event_next AS fact
LEFT JOIN gold.dim_date_next AS dim ON fact.review_date = dim.date
WHERE isNull(dim.date)
SETTINGS join_use_nulls = 1;

SELECT throwIf(
    (
        SELECT tuple(
            count(),
            sum(quantity),
            sum(total_amount),
            avg(overall_rating)
        )
        FROM staging.ecommerce_events_current
    )
    !=
    (
        SELECT tuple(
            count(),
            sum(quantity),
            sum(total_amount),
            avg(overall_rating)
        )
        FROM gold.fact_ecommerce_event_next
    ),
    'Gold fact next KPI totals do not match staging current'
);

EXCHANGE TABLES gold.dim_product AND gold.dim_product_next;
EXCHANGE TABLES gold.dim_store AND gold.dim_store_next;
EXCHANGE TABLES gold.dim_reviewer AND gold.dim_reviewer_next;
EXCHANGE TABLES gold.dim_date AND gold.dim_date_next;
EXCHANGE TABLES gold.fact_ecommerce_event AND gold.fact_ecommerce_event_next;

DROP TABLE gold.dim_product_next;
DROP TABLE gold.dim_store_next;
DROP TABLE gold.dim_reviewer_next;
DROP TABLE gold.dim_date_next;
DROP TABLE gold.fact_ecommerce_event_next;

SELECT
    'CLICKHOUSE_GOLD_BOOTSTRAP_OK' AS marker,
    count() AS event_count,
    sum(quantity) AS quantity_sum,
    sum(total_amount) AS gross_item_value,
    round(avg(overall_rating), 4) AS avg_rating,
    (SELECT count() FROM gold.dim_product) AS product_rows,
    (SELECT count() FROM gold.dim_store) AS store_rows,
    (SELECT count() FROM gold.dim_reviewer) AS reviewer_rows,
    (SELECT count() FROM gold.dim_date) AS date_rows
FROM gold.fact_ecommerce_event;
