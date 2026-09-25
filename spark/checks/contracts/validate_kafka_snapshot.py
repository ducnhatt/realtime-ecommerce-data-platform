"""Contract check: validate every event in a bounded Kafka snapshot."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.validation import validate_events


ALLOWED_STATUSES = ["VALID", "WARNING", "INVALID"]


def read_kafka_snapshot(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    """Capture the offsets available when this batch query starts."""

    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_payload"),
        )
    )


def collect_status_counts(validated: DataFrame) -> dict[str, int]:
    counts = {status: 0 for status in ALLOWED_STATUSES}
    for row in validated.groupBy("validation_status").count().collect():
        counts[row["validation_status"]] = row["count"]
    return counts


def assert_invariants(
    source_count: int,
    validated: DataFrame,
    status_counts: dict[str, int],
) -> tuple[int, int]:
    """Fail loudly on silent loss, duplication, or inconsistent routing state."""

    output_count = validated.count()
    lineage_count = (
        validated.select("kafka_topic", "kafka_partition", "kafka_offset")
        .distinct()
        .count()
    )

    status_errors = validated.filter(
        F.col("validation_status").isNull()
        | ~F.col("validation_status").isin(ALLOWED_STATUSES)
        | (
            (F.col("validation_status") == "INVALID")
            & (F.size("validation_errors") == 0)
        )
        | (
            (F.col("validation_status") != "INVALID")
            & (F.size("validation_errors") > 0)
        )
        | (
            (F.col("validation_status") == "WARNING")
            & (F.size("validation_warnings") == 0)
        )
        | (
            (F.col("validation_status") == "VALID")
            & (F.size("validation_warnings") > 0)
        )
    ).count()

    failures = []
    if source_count == 0:
        failures.append("Kafka snapshot is empty")
    if output_count != source_count:
        failures.append(
            f"row conservation failed: input={source_count}, output={output_count}"
        )
    if lineage_count != source_count:
        failures.append(
            f"lineage uniqueness failed: input={source_count}, "
            f"distinct_lineage={lineage_count}"
        )
    if sum(status_counts.values()) != output_count:
        failures.append(
            "status count reconciliation failed: "
            f"statuses={sum(status_counts.values())}, output={output_count}"
        )
    if status_errors:
        failures.append(f"status/error consistency failed for {status_errors} row(s)")

    if failures:
        raise RuntimeError("Kafka validation integration failed:\n" + "\n".join(failures))

    return output_count, lineage_count


def show_quality_summary(validated: DataFrame) -> None:
    print("VALIDATION_STATUS_COUNTS")
    validated.groupBy("validation_status").count().orderBy(
        "validation_status"
    ).show(truncate=False)

    print("VALIDATION_ERROR_COUNTS")
    validated.select(F.explode("validation_errors").alias("rule_id")).groupBy(
        "rule_id"
    ).count().orderBy(F.desc("count"), "rule_id").show(truncate=False)

    print("VALIDATION_WARNING_COUNTS")
    validated.select(F.explode("validation_warnings").alias("rule_id")).groupBy(
        "rule_id"
    ).count().orderBy(F.desc("count"), "rule_id").show(truncate=False)

    print("NON_VALID_LINEAGE_SAMPLE")
    validated.filter(F.col("validation_status") != "VALID").select(
        "kafka_partition",
        "kafka_offset",
        "validation_status",
        "validation_errors",
        "validation_warnings",
    ).orderBy("kafka_partition", "kafka_offset").show(20, truncate=False)


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")

    spark = (
        SparkSession.builder.appName("ecommerce-kafka-contract-validation-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    source = None
    validated = None
    try:
        source = read_kafka_snapshot(spark, bootstrap_servers, topic).cache()
        source_count = source.count()
        partitions_with_data = source.select("kafka_partition").distinct().count()

        validated = validate_events(source).cache()
        status_counts = collect_status_counts(validated)
        output_count, lineage_count = assert_invariants(
            source_count,
            validated,
            status_counts,
        )
        show_quality_summary(validated)

        print(
            "KAFKA_VALIDATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"bootstrap_servers={bootstrap_servers} "
            f"topic={topic} source_time_zone={source_time_zone} "
            f"message_count={source_count} output_count={output_count} "
            f"distinct_lineage={lineage_count} "
            f"partitions_with_data={partitions_with_data} "
            f"valid={status_counts['VALID']} "
            f"warning={status_counts['WARNING']} "
            f"invalid={status_counts['INVALID']}"
        )
    finally:
        if validated is not None:
            validated.unpersist()
        if source is not None:
            source.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
