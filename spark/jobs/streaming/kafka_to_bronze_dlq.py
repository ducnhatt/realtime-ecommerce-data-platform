"""Streaming job: route validated Kafka events to Bronze and DLQ."""

import json
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.storage import configure_minio_s3a
from pipeline.bronze_manifest import (
    committed_batch_status_counts,
    committed_manifest_time,
)
from pipeline.validation import validate_events
from src.monitoring.monitoring_bronze import MonitoringBronze


BRONZE_COLUMNS = [
    "payload",
    "raw_payload",
    "message_key",
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
]

DLQ_COLUMNS = [
    "raw_payload",
    "payload",
    "message_key",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "validation_status",
    "validation_errors",
    "validation_warnings",
    "missing_fields",
    "unexpected_fields",
    "contract_version",
    "ingest_date",
]


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    max_offsets_per_trigger: int,
) -> DataFrame:
    """Read Kafka continuously; checkpoints supersede startingOffsets on restart."""

    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
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


def prepare_outputs(source: DataFrame) -> tuple[DataFrame, DataFrame]:
    validated = (
        validate_events(source)
        .withColumn("ingest_date", F.to_date("kafka_timestamp"))
    )

    bronze = validated.filter(
        F.col("validation_status").isin("VALID", "WARNING")
    ).select(*BRONZE_COLUMNS)
    dlq = validated.filter(F.col("validation_status") == "INVALID").select(
        *DLQ_COLUMNS
    )
    return bronze, dlq


def start_parquet_sink(
    df: DataFrame,
    query_name: str,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str,
    output_partitions: int,
):
    output = df.coalesce(output_partitions)
    return (
        output.writeStream.queryName(query_name)
        .format("parquet")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("contract_version", "ingest_date")
        .trigger(processingTime=trigger_interval)
        .start()
    )


def parse_kafka_offsets(value):
    """Normalize Spark Kafka progress offsets from JSON text or a mapping."""

    if value in (None, ""):
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return {}
    return value if isinstance(value, dict) else {}


def total_kafka_offset(offsets) -> int:
    """Sum exclusive Kafka offsets across every topic partition."""

    total = 0
    for partitions in offsets.values():
        if not isinstance(partitions, dict):
            continue
        for offset in partitions.values():
            try:
                total += int(offset)
            except (TypeError, ValueError):
                continue
    return total


def monitor_committed_batches(
    queries,
    bronze_monitor: MonitoringBronze,
    spark: SparkSession,
    bronze_output: str,
    dlq_output: str,
    bronze_checkpoint: str,
    dlq_checkpoint: str,
    poll_interval_seconds: int = 2,
) -> None:
    """Emit quality only when both physical sink manifests are committed."""

    seen_progress = set()
    pending_progress = {}
    monitored_batches = set()
    expected_queries = {query.name for query in queries}

    while all(query.isActive for query in queries):
        for query in queries:
            query_key = str(query.id)
            for progress in query.recentProgress:
                batch_id = int(progress["batchId"])
                progress_key = (query_key, batch_id, progress.get("timestamp"))
                if progress_key in seen_progress:
                    continue
                seen_progress.add(progress_key)

                sources = progress.get("sources", [])
                source_progress = sources[0] if sources else {}
                start_offset = parse_kafka_offsets(
                    source_progress.get("startOffset", {})
                )
                end_offset = parse_kafka_offsets(
                    source_progress.get("endOffset", {})
                )
                offsets_available = bool(end_offset)
                offset_delta = (
                    total_kafka_offset(end_offset)
                    - total_kafka_offset(start_offset)
                    if offsets_available
                    else None
                )
                compact_end_offset = json.dumps(
                    end_offset,
                    separators=(",", ":"),
                    sort_keys=True,
                )
                print(
                    "BRONZE_DLQ_BATCH_COMMITTED "
                    f"query={query.name} "
                    f"batch_id={batch_id} "
                    f"spark_input_rows={progress.get('numInputRows', 0)} "
                    "kafka_offset_delta="
                    f"{offset_delta if offset_delta is not None else 'UNAVAILABLE'} "
                    "offset_observation="
                    f"{'AVAILABLE' if offsets_available else 'UNAVAILABLE'} "
                    f"end_offset={compact_end_offset}",
                    flush=True,
                )
                pending_progress.setdefault(batch_id, {})[query.name] = progress

        for batch_id in sorted(pending_progress):
            if batch_id in monitored_batches:
                continue
            progress_by_query = pending_progress[batch_id]
            if not expected_queries.issubset(progress_by_query):
                continue

            try:
                bronze_result = committed_batch_status_counts(
                    spark, bronze_output, bronze_checkpoint, batch_id
                )
                dlq_result = committed_batch_status_counts(
                    spark, dlq_output, dlq_checkpoint, batch_id
                )
                if bronze_result is None or dlq_result is None:
                    continue
                bronze_counts, bronze_expected = bronze_result
                dlq_counts, dlq_expected = dlq_result
                if bronze_expected is not None or dlq_expected is not None:
                    if bronze_expected != dlq_expected:
                        raise RuntimeError("compacted sink Kafka offset boundaries differ")
                    physical_rows = sum(bronze_counts.values()) + sum(dlq_counts.values())
                    if physical_rows != bronze_expected:
                        raise RuntimeError(
                            f"compacted batch physical rows {physical_rows} "
                            f"!= Kafka offset delta {bronze_expected}"
                        )
                committed_at = max(
                    committed_manifest_time(spark, bronze_output, batch_id),
                    committed_manifest_time(spark, dlq_output, batch_id),
                )
            except Exception as exc:
                print(
                    "BRONZE_MONITORING_RETRY "
                    f"batch_id={batch_id} error={exc!r}",
                    flush=True,
                )
                continue
            unexpected = (
                set(bronze_counts) - {"VALID", "WARNING"}
            ) | (set(dlq_counts) - {"INVALID"})
            if unexpected:
                print(
                    "BRONZE_MONITORING_SKIPPED "
                    f"batch_id={batch_id} unexpected_statuses={sorted(unexpected)}",
                    flush=True,
                )
                continue
            valid_count = bronze_counts.get("VALID", 0)
            warning_count = bronze_counts.get("WARNING", 0)
            invalid_count = dlq_counts.get("INVALID", 0)
            durations = [
                int(progress.get("durationMs", {}).get("triggerExecution", 0))
                for progress in progress_by_query.values()
            ]
            written = bronze_monitor.log_committed_batch(
                batch_id=batch_id,
                batch_input_rows=valid_count + warning_count + invalid_count,
                valid_count=valid_count,
                warning_count=warning_count,
                invalid_count=invalid_count,
                duration_ms=max(durations),
                committed_at=committed_at,
            )
            if written == 3:
                monitored_batches.add(batch_id)
                del pending_progress[batch_id]

        time.sleep(poll_interval_seconds)


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    dlq_bucket = os.getenv("MINIO_DLQ_BUCKET", "dlq")
    checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints")
    trigger_interval = os.getenv("STREAM_TRIGGER_INTERVAL", "25 seconds")
    output_partitions = int(os.getenv("STREAM_OUTPUT_PARTITIONS", "1"))
    max_offsets_per_trigger = int(
        os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "10000")
    )
    monitoring_dir = os.getenv("MONITORING_DIR", "/opt/monitoring")
    bronze_monitor_file = os.getenv(
        "BRONZE_MONITOR_FILE", "spark_bronze_monitor.jsonl"
    )

    if output_partitions < 1:
        raise ValueError("STREAM_OUTPUT_PARTITIONS must be at least 1")
    if max_offsets_per_trigger < 1:
        raise ValueError("KAFKA_MAX_OFFSETS_PER_TRIGGER must be at least 1")

    spark = (
        SparkSession.builder.appName("ecommerce-kafka-to-bronze-dlq-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.internal.io.cloud.AbortableStreamBasedCheckpointFileManager",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)
    bronze_monitor = MonitoringBronze(
        path=os.path.join(monitoring_dir, bronze_monitor_file),
        topic=topic,
        application_id=spark.sparkContext.applicationId,
    )

    bronze_output = f"s3a://{bronze_bucket}/{topic}"
    dlq_output = f"s3a://{dlq_bucket}/{topic}"
    checkpoint_root = f"s3a://{checkpoint_bucket}/kafka_to_bronze_v1"

    queries = []
    try:
        source = read_kafka_stream(
            spark,
            bootstrap_servers,
            topic,
            max_offsets_per_trigger,
        )
        bronze, dlq = prepare_outputs(source)

        queries.append(
            start_parquet_sink(
                bronze,
                "ecommerce_bronze_v1",
                bronze_output,
                f"{checkpoint_root}/bronze",
                trigger_interval,
                output_partitions,
            )
        )
        queries.append(
            start_parquet_sink(
                dlq,
                "ecommerce_dlq_v1",
                dlq_output,
                f"{checkpoint_root}/dlq",
                trigger_interval,
                output_partitions,
            )
        )

        print(
            "BRONZE_DLQ_STREAMING_STARTED "
            f"application_id={spark.sparkContext.applicationId} "
            f"topic={topic} trigger='{trigger_interval}' "
            f"bronze_query_id={queries[0].id} "
            f"bronze_run_id={queries[0].runId} "
            f"dlq_query_id={queries[1].id} "
            f"dlq_run_id={queries[1].runId} "
            f"output_partitions={output_partitions}"
        )

        monitor_committed_batches(
            queries, bronze_monitor, spark, bronze_output, dlq_output,
            f"{checkpoint_root}/bronze", f"{checkpoint_root}/dlq",
        )

        failures = [
            f"{query.name}: {query.exception()}"
            for query in queries
            if query.exception() is not None
        ]
        if failures:
            raise RuntimeError("Streaming query failed:\n" + "\n".join(failures))
        raise RuntimeError("A streaming query terminated unexpectedly without an error")
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
