"""Incremental job: process newly discovered Bronze files into Silver."""

import os
import time

from pyspark.sql import SparkSession

from pipeline.silver import transform_bronze_to_silver
from pipeline.storage import configure_minio_s3a
from pipeline.bronze_manifest import committed_status_counts
from src.monitoring.monitoring_silver import MonitoringSilver


def committed_output_files(spark, dataset_path):
    """Snapshot files visible through Spark's committed sink metadata."""
    path = spark._jvm.org.apache.hadoop.fs.Path(dataset_path)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return set()
    return set(spark.read.parquet(dataset_path).inputFiles())


def new_committed_output_files(previous, current):
    """Return append-only file delta and fail if committed files disappeared."""
    removed = previous - current
    if removed:
        raise RuntimeError(
            f"Silver committed files disappeared during run: count={len(removed)}"
        )
    return sorted(current - previous)


def main() -> None:
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints")
    silver_prefix = os.getenv(
        "SILVER_INCREMENTAL_PREFIX",
        "ecommerce_reviews_incremental_v1",
    )
    checkpoint_prefix = os.getenv(
        "SILVER_INCREMENTAL_CHECKPOINT_PREFIX",
        "bronze_to_silver_incremental_v1",
    )
    max_files_per_trigger = int(
        os.getenv("SILVER_MAX_FILES_PER_TRIGGER", "10")
    )
    output_partitions = int(
        os.getenv("SILVER_INCREMENTAL_OUTPUT_PARTITIONS", "2")
    )
    if max_files_per_trigger < 1:
        raise ValueError("SILVER_MAX_FILES_PER_TRIGGER must be at least 1")
    if output_partitions < 1:
        raise ValueError(
            "SILVER_INCREMENTAL_OUTPUT_PARTITIONS must be at least 1"
        )

    bronze_path = f"s3a://{bronze_bucket}/{topic}"
    silver_path = f"s3a://{silver_bucket}/{silver_prefix}"
    checkpoint_path = f"s3a://{checkpoint_bucket}/{checkpoint_prefix}"
    monitoring_path = os.path.join(
        os.getenv("MONITORING_DIR", "/opt/monitoring"),
        os.getenv("SILVER_MONITOR_FILE", "silver_stream_monitor.jsonl"),
    )

    spark = (
        SparkSession.builder.appName("ecommerce-bronze-to-silver-incremental-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.internal.io.cloud.AbortableStreamBasedCheckpointFileManager",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)
    silver_monitor = MonitoringSilver(monitoring_path, topic)

    query = None
    started_at = time.monotonic()
    try:
        # Capture the committed file set rather than numeric manifest IDs.
        # Spark periodically replaces a numbered log with ``<id>.compact``;
        # comparing committed files remains correct across that transition.
        previous_output_files = committed_output_files(spark, silver_path)
        # File streaming requires an explicit schema. Reading Parquet footers here
        # discovers the already-versioned Bronze schema without scanning all rows.
        bronze_schema = spark.read.parquet(bronze_path).schema
        bronze_stream = (
            spark.readStream.schema(bronze_schema)
            .option("basePath", bronze_path)
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .parquet(bronze_path)
        )

        # Stateful global deduplication would retain unbounded state. Bronze has
        # already passed the unique Kafka-lineage gate; reconciliation remains the
        # cross-batch duplicate detector for this Parquet v1 implementation.
        silver_stream = transform_bronze_to_silver(
            bronze_stream,
            deduplicate=False,
        ).repartition(output_partitions, "ingest_date")

        query = (
            silver_stream.writeStream.queryName(
                "ecommerce_bronze_to_silver_incremental_v1"
            )
            .format("parquet")
            .outputMode("append")
            .option("path", silver_path)
            .option("checkpointLocation", checkpoint_path)
            .option("compression", "snappy")
            .partitionBy("silver_schema_version", "ingest_date")
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        current_output_files = committed_output_files(spark, silver_path)
        new_output_files = new_committed_output_files(
            previous_output_files,
            current_output_files,
        )
        output_rows = sum(
            committed_status_counts(
                spark,
                silver_path,
                new_output_files,
            ).values()
        )

        progress = query.recentProgress
        input_rows = sum(
            int(batch.get("numInputRows", 0)) for batch in progress
        )
        non_empty_batches = sum(
            int(batch.get("numInputRows", 0) > 0) for batch in progress
        )
        last_batch_id = (
            max(int(batch["batchId"]) for batch in progress)
            if progress
            else -1
        )
        duration_ms = (time.monotonic() - started_at) * 1000

        silver_monitor.log_run(
            run_id=str(query.runId),
            input_rows=input_rows,
            output_rows=output_rows,
            non_empty_batches=non_empty_batches,
            duration_ms=duration_ms,
            last_batch_id=last_batch_id,
        )

        print(
            "BRONZE_TO_SILVER_INCREMENTAL_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"query_id={query.id} run_id={query.runId} "
            f"bronze_path={bronze_path} silver_path={silver_path} "
            f"checkpoint_path={checkpoint_path} "
            f"input_rows={input_rows} non_empty_batches={non_empty_batches} "
            f"output_rows={output_rows} "
            f"new_output_files={len(new_output_files)} "
            f"last_batch_id={last_batch_id} "
            f"max_files_per_trigger={max_files_per_trigger} "
            f"output_partitions={output_partitions}"
        )
    except Exception:
        progress = query.recentProgress if query is not None else []
        silver_monitor.log_failure(
            run_id=(
                str(query.runId)
                if query is not None
                else spark.sparkContext.applicationId
            ),
            last_batch_id=(
                max(int(batch["batchId"]) for batch in progress)
                if progress else -1
            ),
        )
        raise
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
