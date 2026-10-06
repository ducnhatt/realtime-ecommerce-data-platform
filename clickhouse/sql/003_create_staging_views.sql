CREATE OR REPLACE VIEW staging.ecommerce_events_conflicts AS
SELECT
    kafka_topic,
    kafka_partition,
    kafka_offset,
    uniqExact(silver_row_hash) AS payload_versions,
    groupArray(silver_row_hash) AS observed_hashes
FROM staging.ecommerce_events_raw FINAL
GROUP BY
    kafka_topic,
    kafka_partition,
    kafka_offset
HAVING payload_versions > 1;

CREATE OR REPLACE VIEW staging.ecommerce_events_current AS
SELECT * EXCEPT (lineage_payload_versions)
FROM
(
    SELECT
        *,
        count() OVER
        (
            PARTITION BY
                kafka_topic,
                kafka_partition,
                kafka_offset
        ) AS lineage_payload_versions
    FROM staging.ecommerce_events_raw FINAL
)
WHERE lineage_payload_versions = 1;
