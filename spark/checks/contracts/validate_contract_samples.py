"""Contract check: exercise the schema/validation routing fixtures."""

import json
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.validation import validate_events


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def build_cases(sample_dir: Path) -> list[tuple[str, str, str, str | None, str | None]]:
    valid_payload = json.loads(read_text(sample_dir / "valid-event.json"))

    missing_field = dict(valid_payload)
    missing_field.pop("orderID")

    wrong_type = dict(valid_payload)
    wrong_type["quantity"] = "two"

    return [
        ("valid", read_text(sample_dir / "valid-event.json"), "VALID", None, None),
        (
            "warning",
            read_text(sample_dir / "warning-event.json"),
            "WARNING",
            None,
            "W101_UNUSUAL_BRAND_CATEGORY",
        ),
        (
            "required_null",
            read_text(sample_dir / "invalid-dlq-event.json"),
            "INVALID",
            "E006_REQUIRED_NULL",
            None,
        ),
        ("malformed", '{"reviewerID":', "INVALID", "E001_MALFORMED_JSON", None),
        (
            "missing_field",
            json.dumps(missing_field, ensure_ascii=False),
            "INVALID",
            "E003_MISSING_FIELDS",
            None,
        ),
        (
            "wrong_type",
            json.dumps(wrong_type, ensure_ascii=False),
            "INVALID",
            "E005_TYPE_MISMATCH",
            None,
        ),
    ]


def main() -> None:
    sample_dir = Path(os.getenv("VALIDATION_SAMPLE_DIR", "/opt/contract-samples"))
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    spark = (
        SparkSession.builder.appName("ecommerce-contract-validation-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        cases = build_cases(sample_dir)
        input_df = spark.createDataFrame(
            cases,
            "case_id STRING, raw_payload STRING, expected_status STRING, "
            "expected_error STRING, expected_warning STRING",
        ).withColumns(
            {
                "kafka_topic": F.lit("contract-test"),
                "kafka_partition": F.lit(0),
                "kafka_offset": F.monotonically_increasing_id(),
                "kafka_timestamp": F.current_timestamp(),
            }
        )

        results = validate_events(input_df).cache()
        actual_rows = {
            row["case_id"]: row
            for row in results.select(
                "case_id",
                "expected_status",
                "expected_error",
                "expected_warning",
                "validation_status",
                "validation_errors",
                "validation_warnings",
                "missing_fields",
                "unexpected_fields",
            ).collect()
        }

        failures = []
        for case_id, row in actual_rows.items():
            if row["validation_status"] != row["expected_status"]:
                failures.append(
                    f"{case_id}: expected status {row['expected_status']}, "
                    f"got {row['validation_status']}"
                )
            if row["expected_error"] and row["expected_error"] not in row["validation_errors"]:
                failures.append(
                    f"{case_id}: missing expected error {row['expected_error']}; "
                    f"got {row['validation_errors']}"
                )
            if row["expected_warning"] and row["expected_warning"] not in row["validation_warnings"]:
                failures.append(
                    f"{case_id}: missing expected warning {row['expected_warning']}; "
                    f"got {row['validation_warnings']}"
                )

        results.select(
            "case_id",
            "validation_status",
            "validation_errors",
            "validation_warnings",
            "missing_fields",
            "unexpected_fields",
        ).orderBy("case_id").show(truncate=False)

        if failures:
            raise RuntimeError("Validation regression failed:\n" + "\n".join(failures))

        print(
            "CONTRACT_VALIDATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"cases={len(actual_rows)} statuses=VALID,WARNING,INVALID"
        )
        results.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
