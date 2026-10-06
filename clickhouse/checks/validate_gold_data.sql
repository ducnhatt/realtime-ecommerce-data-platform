SELECT throwIf(
    (SELECT count() FROM gold.fact_ecommerce_event)
        != (SELECT count() FROM staging.ecommerce_events_current),
    'Gold fact row count does not match staging current'
);

SELECT throwIf(
    (SELECT count() FROM gold.fact_ecommerce_event)
        != (SELECT uniqExact(tuple(kafka_topic, kafka_partition, kafka_offset))
            FROM gold.fact_ecommerce_event),
    'Gold fact Kafka lineage is not unique'
);

SELECT throwIf(
    (
        SELECT tuple(count(), sum(quantity), sum(total_amount), avg(overall_rating))
        FROM staging.ecommerce_events_current
    )
    !=
    (
        SELECT tuple(count(), sum(quantity), sum(total_amount), avg(overall_rating))
        FROM gold.fact_ecommerce_event
    ),
    'Gold KPI totals do not match staging current'
);

SELECT throwIf(
    count() != 0,
    'Gold fact has broken dimension relationships'
)
FROM
(
    SELECT fact.kafka_topic
    FROM gold.fact_ecommerce_event AS fact
    LEFT JOIN gold.dim_product AS product ON fact.asin = product.asin
    LEFT JOIN gold.dim_store AS store ON fact.store_id = store.store_id
    LEFT JOIN gold.dim_reviewer AS reviewer
        ON fact.reviewer_id = reviewer.reviewer_id
    LEFT JOIN gold.dim_date AS calendar ON fact.review_date = calendar.date
    WHERE isNull(product.asin)
       OR isNull(store.store_id)
       OR isNull(reviewer.reviewer_id)
       OR isNull(calendar.date)
)
SETTINGS join_use_nulls = 1;

SELECT
    'CLICKHOUSE_GOLD_DATA_CONTRACT_OK' AS marker,
    count() AS event_count,
    sum(quantity) AS quantity_sum,
    sum(total_amount) AS gross_item_value,
    round(avg(overall_rating), 4) AS avg_rating,
    (SELECT count() FROM gold.dim_product) AS product_rows,
    (SELECT count() FROM gold.dim_store) AS store_rows,
    (SELECT count() FROM gold.dim_reviewer) AS reviewer_rows,
    (SELECT count() FROM gold.dim_date) AS date_rows
FROM gold.fact_ecommerce_event;
