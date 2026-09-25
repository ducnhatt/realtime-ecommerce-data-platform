"""Reconciliation check: compare Kafka with Bronze and DLQ Parquet."""

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.storage import configure_minio_s3a


LINEAGE_COLUMNS = ["kafka_topic", "kafka_partition", "kafka_offset"]


def read_kafka_snapshot(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
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
            F.col("value").cast("string").alias("raw_payload"),
        )
    )


def assert_reconciliation(
    source: DataFrame,
    bronze: DataFrame,
    dlq: DataFrame,
) -> dict[str, int]:
    source_count = source.count()
    bronze_count = bronze.count()
    dlq_count = dlq.count()
    sink = bronze.select(
        *LINEAGE_COLUMNS, "raw_payload", "validation_status"
    ).unionByName(
        dlq.select(*LINEAGE_COLUMNS, "raw_payload", "validation_status")
    ).cache()
    sink_count = sink.count()

    source_lineage_count = source.select(*LINEAGE_COLUMNS).distinct().count()
    bronze_lineage_count = bronze.select(*LINEAGE_COLUMNS).distinct().count()
    dlq_lineage_count = dlq.select(*LINEAGE_COLUMNS).distinct().count()
    sink_lineage = sink.select(*LINEAGE_COLUMNS).distinct().cache()
    sink_lineage_count = sink_lineage.count()

    overlap_count = (
        bronze.select(*LINEAGE_COLUMNS)
        .distinct()
        .join(dlq.select(*LINEAGE_COLUMNS).distinct(), LINEAGE_COLUMNS, "inner")
        .count()
    )
    missing_count = (
        source.select(*LINEAGE_COLUMNS)
        .distinct()
        .join(sink_lineage, LINEAGE_COLUMNS, "left_anti")
        .count()
    )
    extra_count = (
        sink_lineage.join(
            source.select(*LINEAGE_COLUMNS).distinct(),
            LINEAGE_COLUMNS,
            "left_anti",
        ).count()
    )

    source_hashes = source.select(
        *LINEAGE_COLUMNS,
        F.sha2("raw_payload", 256).alias("source_payload_hash"),
    )
    sink_hashes = sink.select(
        *LINEAGE_COLUMNS,
        F.sha2("raw_payload", 256).alias("sink_payload_hash"),
    )
    payload_mismatch_count = (
        source_hashes.join(sink_hashes, LINEAGE_COLUMNS, "inner")
        .filter(
            ~F.col("source_payload_hash").eqNullSafe(F.col("sink_payload_hash"))
        )
        .count()
    )

    bronze_route_violations = bronze.filter(
        F.col("validation_status").isNull()
        | ~F.col("validation_status").isin("VALID", "WARNING")
        | F.col("validation_errors").isNull()
        | (F.size("validation_errors") > 0)
    ).count()
    dlq_route_violations = dlq.filter(
        F.col("validation_status").isNull()
        | (F.col("validation_status") != "INVALID")
        | F.col("validation_errors").isNull()
        | (F.size("validation_errors") == 0)
    ).count()
    partition_date_violations = (
        bronze.select("kafka_timestamp", "ingest_date")
        .unionByName(dlq.select("kafka_timestamp", "ingest_date"))
        .filter(
            F.col("ingest_date").isNull()
            | (F.col("ingest_date") != F.to_date("kafka_timestamp"))
        )
        .count()
    )
    contract_violations = (
        bronze.select("contract_version")
        .unionByName(dlq.select("contract_version"))
        .filter(
            F.col("contract_version").isNull()
            | (F.col("contract_version") != "ecommerce-event-v1")
        )
        .count()
    )

    failures = []
    if source_count == 0:
        failures.append("Kafka snapshot is empty")
    if source_lineage_count != source_count:
        failures.append(
            f"Kafka lineage is not unique: rows={source_count}, "
            f"lineage={source_lineage_count}"
        )
    if bronze_lineage_count != bronze_count:
        failures.append(
            f"Bronze contains duplicate lineage: rows={bronze_count}, "
            f"lineage={bronze_lineage_count}"
        )
    if dlq_lineage_count != dlq_count:
        failures.append(
            f"DLQ contains duplicate lineage: rows={dlq_count}, "
            f"lineage={dlq_lineage_count}"
        )
    if sink_count != source_count:
        failures.append(
            f"row reconciliation failed: Kafka={source_count}, sinks={sink_count}"
        )
    if sink_lineage_count != sink_count:
        failures.append(
            f"sink union contains duplicate lineage: rows={sink_count}, "
            f"lineage={sink_lineage_count}"
        )
    if overlap_count:
        failures.append(f"Bronze/DLQ lineage overlap={overlap_count}")
    if missing_count:
        failures.append(f"Kafka lineage missing from sinks={missing_count}")
    if extra_count:
        failures.append(f"sink lineage absent from current Kafka snapshot={extra_count}")
    if payload_mismatch_count:
        failures.append(f"raw payload hash mismatches={payload_mismatch_count}")
    if bronze_route_violations:
        failures.append(f"Bronze routing violations={bronze_route_violations}")
    if dlq_route_violations:
        failures.append(f"DLQ routing violations={dlq_route_violations}")
    if partition_date_violations:
        failures.append(f"partition date violations={partition_date_violations}")
    if contract_violations:
        failures.append(f"contract version violations={contract_violations}")

    metrics = {
        "kafka_count": source_count,
        "bronze_count": bronze_count,
        "dlq_count": dlq_count,
        "sink_count": sink_count,
        "distinct_lineage": sink_lineage_count,
        "missing": missing_count,
        "extra": extra_count,
        "overlap": overlap_count,
        "payload_mismatches": payload_mismatch_count,
    }

    sink_lineage.unpersist()
    sink.unpersist()

    if failures:
        raise RuntimeError("Bronze/DLQ reconciliation failed:\n" + "\n".join(failures))
    return metrics


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    dlq_bucket = os.getenv("MINIO_DLQ_BUCKET", "dlq")

    spark = (
        SparkSession.builder.appName("ecommerce-bronze-dlq-reconciliation-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    source = None
    bronze = None
    dlq = None
    try:
        source = read_kafka_snapshot(spark, bootstrap_servers, topic).cache()
        bronze_path = f"s3a://{bronze_bucket}/{topic}"
        dlq_path = f"s3a://{dlq_bucket}/{topic}"
        bronze = spark.read.parquet(bronze_path).cache()
        dlq = spark.read.parquet(dlq_path).cache()

        metrics = assert_reconciliation(source, bronze, dlq)
        bronze_files = (
            spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.parquet")
            .load(bronze_path)
            .count()
        )
        dlq_files = (
            spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.parquet")
            .load(dlq_path)
            .count()
        )

        print("PHYSICAL_STATUS_COUNTS")
        bronze.select("validation_status").unionByName(
            dlq.select("validation_status")
        ).groupBy("validation_status").count().orderBy(
            "validation_status"
        ).show(truncate=False)

        print(
            "BRONZE_DLQ_RECONCILIATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"kafka_count={metrics['kafka_count']} "
            f"bronze_count={metrics['bronze_count']} "
            f"dlq_count={metrics['dlq_count']} "
            f"sink_count={metrics['sink_count']} "
            f"distinct_lineage={metrics['distinct_lineage']} "
            f"missing={metrics['missing']} extra={metrics['extra']} "
            f"overlap={metrics['overlap']} "
            f"payload_mismatches={metrics['payload_mismatches']} "
            f"bronze_files={bronze_files} dlq_files={dlq_files}"
        )
    finally:
        for frame in (source, bronze, dlq):
            if frame is not None:
                frame.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
