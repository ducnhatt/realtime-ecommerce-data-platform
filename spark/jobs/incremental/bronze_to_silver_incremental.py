"""Incremental job: process newly discovered Bronze files into Silver."""

import os

from pyspark.sql import SparkSession

from pipeline.silver import transform_bronze_to_silver
from pipeline.storage import configure_minio_s3a


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

    query = None
    try:
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

        progress = query.recentProgress
        input_rows = sum(
            int(batch.get("numInputRows", 0)) for batch in progress
        )
        non_empty_batches = sum(
            1 for batch in progress if int(batch.get("numInputRows", 0)) > 0
        )
        last_batch_id = (
            int(progress[-1]["batchId"])
            if progress
            else -1
        )

        print(
            "BRONZE_TO_SILVER_INCREMENTAL_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"query_id={query.id} run_id={query.runId} "
            f"bronze_path={bronze_path} silver_path={silver_path} "
            f"checkpoint_path={checkpoint_path} "
            f"input_rows={input_rows} non_empty_batches={non_empty_batches} "
            f"last_batch_id={last_batch_id} "
            f"max_files_per_trigger={max_files_per_trigger} "
            f"output_partitions={output_partitions}"
        )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
