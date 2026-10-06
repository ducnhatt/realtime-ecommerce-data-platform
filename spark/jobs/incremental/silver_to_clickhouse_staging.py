"""Incrementally load newly discovered Silver files into ClickHouse staging."""

import os
import uuid

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from pipeline.clickhouse import assert_silver_columns, build_staging_batch
from pipeline.storage import configure_minio_s3a


def main() -> None:
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints")
    silver_prefix = os.getenv(
        "SILVER_INCREMENTAL_PREFIX",
        "ecommerce_reviews_incremental_v1",
    )
    checkpoint_prefix = os.getenv(
        "CLICKHOUSE_STAGING_CHECKPOINT_PREFIX",
        "silver_to_clickhouse_staging_v1",
    )
    max_files_per_trigger = int(
        os.getenv("CLICKHOUSE_STAGING_MAX_FILES_PER_TRIGGER", "10")
    )
    output_partitions = int(
        os.getenv("CLICKHOUSE_STAGING_OUTPUT_PARTITIONS", "2")
    )
    catalog = "clickhouse"
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    user = os.getenv("CLICKHOUSE_USER", "ecommerce")
    password = os.environ["CLICKHOUSE_PASSWORD"]
    database = os.getenv("CLICKHOUSE_STAGING_DATABASE", "staging")
    table = os.getenv("CLICKHOUSE_STAGING_TABLE", "ecommerce_events_raw")
    target = f"{catalog}.{database}.{table}"

    if max_files_per_trigger < 1:
        raise ValueError(
            "CLICKHOUSE_STAGING_MAX_FILES_PER_TRIGGER must be at least 1"
        )
    if output_partitions < 1:
        raise ValueError(
            "CLICKHOUSE_STAGING_OUTPUT_PARTITIONS must be at least 1"
        )

    silver_path = f"s3a://{silver_bucket}/{silver_prefix}"
    checkpoint_path = f"s3a://{checkpoint_bucket}/{checkpoint_prefix}"
    load_run_id = uuid.uuid4().hex
    batch_metrics = []

    spark = (
        SparkSession.builder.appName(
            "ecommerce-silver-to-clickhouse-staging-v1"
        )
        # Hash serialization must not depend on a machine-local timezone.
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.internal.io.cloud.AbortableStreamBasedCheckpointFileManager",
        )
        .config(
            f"spark.sql.catalog.{catalog}",
            "com.clickhouse.spark.ClickHouseCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.host", host)
        .config(f"spark.sql.catalog.{catalog}.protocol", "http")
        .config(f"spark.sql.catalog.{catalog}.http_port", http_port)
        .config(f"spark.sql.catalog.{catalog}.user", user)
        .config(f"spark.sql.catalog.{catalog}.password", password)
        .config(f"spark.sql.catalog.{catalog}.database", database)
        .config(f"spark.{catalog}.write.format", "json")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    query = None
    try:
        silver_schema = spark.read.parquet(silver_path).schema
        silver_stream = (
            spark.readStream.schema(silver_schema)
            .option("basePath", silver_path)
            .option("maxFilesPerTrigger", max_files_per_trigger)
            .parquet(silver_path)
        )
        assert_silver_columns(silver_stream)

        def write_batch(batch: DataFrame, batch_id: int) -> None:
            persisted = batch.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                input_rows = persisted.count()
                batch_metrics.append((int(batch_id), int(input_rows)))
                if input_rows == 0:
                    print(
                        "SILVER_CLICKHOUSE_BATCH_SKIPPED "
                        f"batch_id={batch_id} input_rows=0"
                    )
                    return

                load_batch_id = f"{load_run_id}:{batch_id}"
                staging = build_staging_batch(persisted, load_batch_id)
                staging.repartition(output_partitions).writeTo(target).append()
                print(
                    "SILVER_CLICKHOUSE_BATCH_COMMITTED "
                    f"batch_id={batch_id} input_rows={input_rows} "
                    f"load_batch_id={load_batch_id} "
                    f"writer_partitions={output_partitions}"
                )
            finally:
                persisted.unpersist()

        query = (
            silver_stream.writeStream.queryName(
                "ecommerce_silver_to_clickhouse_staging_v1"
            )
            .foreachBatch(write_batch)
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        print(
            "SILVER_TO_CLICKHOUSE_STAGING_INCREMENTAL_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"query_id={query.id} run_id={query.runId} "
            f"silver_path={silver_path} target={database}.{table} "
            f"checkpoint_path={checkpoint_path} "
            f"input_rows={sum(rows for _, rows in batch_metrics)} "
            f"non_empty_batches={sum(1 for _, rows in batch_metrics if rows > 0)} "
            f"last_batch_id={batch_metrics[-1][0] if batch_metrics else -1} "
            f"max_files_per_trigger={max_files_per_trigger} "
            f"writer_partitions={output_partitions}"
        )
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
