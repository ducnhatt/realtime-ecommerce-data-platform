"""Contract check: verify the pure Bronze-to-Silver transformation."""

import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.silver import (
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    SILVER_SCHEMA_VERSION,
    transform_bronze_to_silver,
)
from pipeline.validation import validate_events


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def main() -> None:
    sample_dir = Path(os.getenv("VALIDATION_SAMPLE_DIR", "/opt/contract-samples"))
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    spark = (
        SparkSession.builder.appName("ecommerce-silver-transform-contract-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        valid_payload = read_text(sample_dir / "valid-event.json")
        warning_payload = read_text(sample_dir / "warning-event.json")
        cases = [
            ("valid", valid_payload, 0),
            ("warning", warning_payload, 1),
            ("valid_duplicate", valid_payload, 0),
        ]
        source = (
            spark.createDataFrame(
                cases,
                "case_id STRING, raw_payload STRING, kafka_offset LONG",
            )
            .withColumn("kafka_topic", F.lit("silver-contract-test"))
            .withColumn("kafka_partition", F.lit(0))
            .withColumn(
                "kafka_timestamp",
                F.to_timestamp(F.lit("2026-09-25 10:00:00")),
            )
        )

        bronze = (
            validate_events(source)
            .withColumn("ingest_date", F.to_date("kafka_timestamp"))
            .select(
                "payload",
                "raw_payload",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "ingested_at",
                "validation_status",
                "validation_errors",
                "validation_warnings",
                "contract_version",
                "ingest_date",
            )
        )

        silver = transform_bronze_to_silver(bronze).cache()
        rows = silver.orderBy("kafka_offset").collect()
        failures = []

        if len(rows) != 2:
            failures.append(f"expected 2 deduplicated rows, got {len(rows)}")
        if silver.select(*LINEAGE_COLUMNS).distinct().count() != len(rows):
            failures.append("Silver lineage is not unique")
        if silver.columns != SILVER_COLUMNS:
            failures.append("Silver columns or ordering do not match the contract")

        data_types = dict(silver.dtypes)
        expected_types = {
            "overall_rating": "tinyint",
            "review_timestamp": "timestamp",
            "review_date": "date",
            "unit_price": "decimal(18,2)",
            "total_amount": "decimal(18,2)",
            "kafka_offset": "bigint",
            "ingest_date": "date",
        }
        for column_name, expected_type in expected_types.items():
            if data_types.get(column_name) != expected_type:
                failures.append(
                    f"{column_name}: expected type {expected_type}, "
                    f"got {data_types.get(column_name)}"
                )

        statuses = {row["validation_status"] for row in rows}
        if statuses != {"VALID", "WARNING"}:
            failures.append(f"expected VALID/WARNING, got {sorted(statuses)}")

        if any(
            row["silver_schema_version"] != SILVER_SCHEMA_VERSION
            or row["source_contract_version"] != "ecommerce-event-v1"
            for row in rows
        ):
            failures.append("schema/contract version mapping is incorrect")

        if any(
            row["total_amount"] != row["unit_price"] * row["quantity"]
            for row in rows
        ):
            failures.append("decimal total_amount invariant failed")

        warning_rows = [row for row in rows if row["validation_status"] == "WARNING"]
        if len(warning_rows) != 1 or not warning_rows[0]["validation_warnings"]:
            failures.append("warning flags were not preserved")
        if warning_rows and warning_rows[0]["reviewer_name"] is not None:
            failures.append("nullable reviewer_name was not preserved")

        forbidden_columns = {"payload", "raw_payload", "validation_errors"}
        leaked_columns = forbidden_columns.intersection(silver.columns)
        if leaked_columns:
            failures.append(f"technical/raw columns leaked: {sorted(leaked_columns)}")

        silver.select(
            *LINEAGE_COLUMNS,
            "reviewer_id",
            "overall_rating",
            "review_date",
            "unit_price",
            "quantity",
            "total_amount",
            "validation_status",
            "validation_warnings",
            "silver_schema_version",
        ).show(truncate=False)

        if failures:
            raise RuntimeError(
                "Silver transformation contract failed:\n" + "\n".join(failures)
            )

        print(
            "SILVER_TRANSFORM_CONTRACT_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"input_rows={len(cases)} output_rows={len(rows)} "
            "distinct_lineage=2 statuses=VALID,WARNING "
            f"schema_version={SILVER_SCHEMA_VERSION}"
        )
        silver.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
