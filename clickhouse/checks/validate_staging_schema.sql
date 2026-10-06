SELECT throwIf(
    count() != 1,
    'staging.ecommerce_events_raw must exist exactly once'
)
FROM system.tables
WHERE database = 'staging'
  AND name = 'ecommerce_events_raw';

SELECT throwIf(
    any(engine) != 'ReplacingMergeTree',
    'staging.ecommerce_events_raw must use ReplacingMergeTree'
)
FROM system.tables
WHERE database = 'staging'
  AND name = 'ecommerce_events_raw';

SELECT throwIf(
    any(partition_key) != 'toYYYYMM(ingest_date)',
    'unexpected staging partition key'
)
FROM system.tables
WHERE database = 'staging'
  AND name = 'ecommerce_events_raw';

SELECT throwIf(
    any(sorting_key) != 'kafka_topic, kafka_partition, kafka_offset, silver_row_hash',
    'unexpected staging sorting key'
)
FROM system.tables
WHERE database = 'staging'
  AND name = 'ecommerce_events_raw';

SELECT throwIf(
    count() != 41,
    'staging.ecommerce_events_raw must contain exactly 41 columns'
)
FROM system.columns
WHERE database = 'staging'
  AND table = 'ecommerce_events_raw';

SELECT throwIf(
    count() != 0,
    'staging schema has missing or mismatched required columns'
)
FROM
(
    SELECT
        tupleElement(required, 1) AS required_name,
        tupleElement(required, 2) AS required_type
    FROM
    (
        SELECT arrayJoin([
            tuple('kafka_topic', 'LowCardinality(String)'),
            tuple('kafka_partition', 'UInt16'),
            tuple('kafka_offset', 'Int64'),
            tuple('silver_row_hash', 'FixedString(64)'),
            tuple('load_batch_id', 'String'),
            tuple('loaded_at', 'DateTime64(3, \'UTC\')')
        ]) AS required
    ) AS expected
    LEFT JOIN system.columns AS actual
      ON actual.database = 'staging'
     AND actual.table = 'ecommerce_events_raw'
     AND actual.name = tupleElement(expected.required, 1)
     AND actual.type = tupleElement(expected.required, 2)
    WHERE coalesce(actual.name, '') = ''
) AS missing_or_mismatched;

SELECT throwIf(
    (SELECT count() FROM staging.ecommerce_events_conflicts) != 0,
    'same Kafka lineage has more than one Silver row hash'
);

SELECT
    'CLICKHOUSE_STAGING_CONTRACT_OK' AS marker,
    (SELECT count() FROM staging.ecommerce_events_raw FINAL) AS physical_current_rows,
    (SELECT count() FROM staging.ecommerce_events_current) AS logical_current_rows,
    (SELECT count() FROM staging.ecommerce_events_conflicts) AS conflicting_lineages;
