"""Contract check: verify Redis minute metric semantics."""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.redis_metrics import aggregate_minute_metrics
from pipeline.validation import validate_events


EXPECTED = {
    "events_total": 3,
    "valid_count": 1,
    "warning_count": 1,
    "invalid_count": 1,
    "record_count": 2,
    "quantity_sum": 3,
    "gross_item_value": 6_000_000.0,
    "successful_labeled_value": 5_000_000.0,
    "rating_sum": 7.0,
    "rating_count": 2,
}


def main() -> None:
    sample_dir = Path(os.getenv("VALIDATION_SAMPLE_DIR", "/opt/contract-samples"))
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    spark = (
        SparkSession.builder.appName("ecommerce-redis-metrics-contract-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        cases = [
            ("valid", (sample_dir / "valid-event.json").read_text("utf-8")),
            ("warning", (sample_dir / "warning-event.json").read_text("utf-8")),
            ("invalid", (sample_dir / "invalid-dlq-event.json").read_text("utf-8")),
        ]
        source = spark.createDataFrame(
            cases,
            "case_id STRING, raw_payload STRING",
        ).withColumns(
            {
                "kafka_topic": F.lit("metric-test"),
                "kafka_partition": F.lit(0),
                "kafka_offset": F.monotonically_increasing_id(),
                "kafka_timestamp": F.to_timestamp(
                    F.lit("2026-09-24 15:30:00"),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            }
        )

        validated = validate_events(source)
        statuses = {
            row["case_id"]: row["validation_status"]
            for row in validated.select("case_id", "validation_status").collect()
        }
        if statuses != {
            "valid": "VALID",
            "warning": "WARNING",
            "invalid": "INVALID",
        }:
            raise RuntimeError(f"Unexpected validation statuses: {statuses!r}")

        rows = aggregate_minute_metrics(validated).collect()
        if len(rows) != 1:
            raise RuntimeError(f"Expected one minute bucket, got {len(rows)}")
        actual = rows[0].asDict()

        failures = []
        if actual["ingest_minute"] != "202609241530":
            failures.append(
                f"ingest_minute expected 202609241530, got {actual['ingest_minute']}"
            )
        for metric, expected in EXPECTED.items():
            if actual[metric] != expected:
                failures.append(
                    f"{metric} expected {expected!r}, got {actual[metric]!r}"
                )

        spark.createDataFrame([actual]).show(truncate=False)
        if failures:
            raise RuntimeError("Redis metric contract failed:\n" + "\n".join(failures))

        print(
            "REDIS_METRICS_CONTRACT_OK "
            f"application_id={spark.sparkContext.applicationId} "
            "cases=3 minute_buckets=1 "
            "valid=1 warning=1 invalid=1 record_count=2"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
