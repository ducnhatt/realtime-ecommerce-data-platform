"""Streaming job: route validated Kafka events to Bronze and DLQ."""

import json
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.storage import configure_minio_s3a
from pipeline.validation import validate_events


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
    validated = validate_events(source).withColumn(
        "ingest_date",
        F.to_date("kafka_timestamp"),
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


def monitor_committed_batches(queries, poll_interval_seconds: int = 2) -> None:
    """Print one observable marker after each query finishes a micro-batch."""

    last_reported_batch = {str(query.id): -1 for query in queries}

    while all(query.isActive for query in queries):
        for query in queries:
            query_key = str(query.id)
            for progress in query.recentProgress:
                batch_id = int(progress["batchId"])
                if batch_id <= last_reported_batch[query_key]:
                    continue

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
                last_reported_batch[query_key] = batch_id

        time.sleep(poll_interval_seconds)


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    dlq_bucket = os.getenv("MINIO_DLQ_BUCKET", "dlq")
    checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints")
    trigger_interval = os.getenv("STREAM_TRIGGER_INTERVAL", "10 seconds")
    output_partitions = int(os.getenv("STREAM_OUTPUT_PARTITIONS", "1"))
    max_offsets_per_trigger = int(
        os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "10000")
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

        monitor_committed_batches(queries)

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
