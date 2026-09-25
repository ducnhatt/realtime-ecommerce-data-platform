"""Recovery job: fully rebuild and read back the Silver dataset."""

import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.silver import (
    LINEAGE_COLUMNS,
    SILVER_SCHEMA_VERSION,
    transform_bronze_to_silver,
)
from pipeline.storage import configure_minio_s3a


def count_parquet_files(spark: SparkSession, path_text: str) -> int:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_text)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not filesystem.exists(path):
        return 0

    files = filesystem.listFiles(path, True)
    count = 0
    while files.hasNext():
        status = files.next()
        if status.isFile() and status.getPath().getName().endswith(".parquet"):
            count += 1
    return count


def validate_bronze(bronze: DataFrame) -> dict[str, int]:
    summary = bronze.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.sum(
            F.when(
                ~F.col("validation_status").isin("VALID", "WARNING"), 1
            ).otherwise(0)
        ).alias("invalid_status_count"),
        F.sum(
            F.when(
                F.col("kafka_topic").isNull()
                | F.col("kafka_partition").isNull()
                | F.col("kafka_offset").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_lineage_count"),
        F.sum(
            F.when(F.size("validation_errors") > 0, 1).otherwise(0)
        ).alias("rows_with_errors"),
        F.sum(
            F.when(
                F.col("contract_version") != "ecommerce-event-v1", 1
            ).otherwise(0)
        ).alias("unsupported_contract_count"),
    ).first().asDict()

    summary["duplicate_lineages"] = (
        bronze.groupBy(*LINEAGE_COLUMNS)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    failures = [
        f"{name}={value}"
        for name, value in summary.items()
        if name != "row_count" and value != 0
    ]
    if summary["row_count"] == 0:
        failures.append("row_count=0")
    if failures:
        raise RuntimeError(
            "Bronze pre-write gate failed:\n" + "\n".join(failures)
        )
    return summary


def validate_silver(silver: DataFrame, expected_count: int) -> dict[str, int]:
    required_columns = [
        column_name
        for column_name in silver.columns
        if column_name != "reviewer_name"
    ]
    null_required = F.lit(False)
    for column_name in required_columns:
        null_required = null_required | F.col(column_name).isNull()

    summary = silver.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(F.struct(*LINEAGE_COLUMNS)).alias("distinct_lineage"),
        F.sum(F.when(null_required, 1).otherwise(0)).alias(
            "null_required_count"
        ),
        F.sum(
            F.when(
                ~F.col("validation_status").isin("VALID", "WARNING"), 1
            ).otherwise(0)
        ).alias("invalid_status_count"),
        F.sum(
            F.when(
                F.col("silver_schema_version") != SILVER_SCHEMA_VERSION, 1
            ).otherwise(0)
        ).alias("wrong_schema_version_count"),
        F.sum(
            F.when(
                F.col("source_contract_version") != "ecommerce-event-v1", 1
            ).otherwise(0)
        ).alias("wrong_contract_version_count"),
        F.sum(
            F.when(
                F.col("total_amount")
                != F.col("unit_price") * F.col("quantity"),
                1,
            ).otherwise(0)
        ).alias("amount_mismatch_count"),
    ).first().asDict()

    failures = []
    if summary["row_count"] != expected_count:
        failures.append(
            f"row_count expected={expected_count}, actual={summary['row_count']}"
        )
    if summary["distinct_lineage"] != expected_count:
        failures.append(
            "distinct_lineage "
            f"expected={expected_count}, actual={summary['distinct_lineage']}"
        )
    for name, value in summary.items():
        if name not in {"row_count", "distinct_lineage"} and value != 0:
            failures.append(f"{name}={value}")

    if failures:
        raise RuntimeError(
            "Silver read-back gate failed:\n" + "\n".join(failures)
        )
    return summary


def main() -> None:
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    output_partitions = int(os.getenv("SILVER_OUTPUT_PARTITIONS", "1"))
    if output_partitions < 1:
        raise ValueError("SILVER_OUTPUT_PARTITIONS must be at least 1")

    bronze_path = f"s3a://{bronze_bucket}/{topic}"
    silver_path = f"s3a://{silver_bucket}/{topic}"

    spark = (
        SparkSession.builder.appName("ecommerce-bronze-to-silver-batch-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    bronze = None
    silver = None
    written = None
    try:
        bronze = spark.read.parquet(bronze_path).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        bronze_metrics = validate_bronze(bronze)

        silver = transform_bronze_to_silver(bronze).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        validate_silver(silver, bronze_metrics["row_count"])

        (
            silver.repartition(output_partitions, "ingest_date")
            .write.mode("overwrite")
            .option("compression", "snappy")
            .partitionBy("silver_schema_version", "ingest_date")
            .parquet(silver_path)
        )

        written = spark.read.parquet(silver_path).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        written_metrics = validate_silver(
            written, bronze_metrics["row_count"]
        )
        parquet_files = count_parquet_files(spark, silver_path)
        if parquet_files == 0:
            raise RuntimeError(f"No Silver Parquet files found at {silver_path}")

        print(
            "BRONZE_TO_SILVER_BATCH_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"bronze_path={bronze_path} silver_path={silver_path} "
            f"bronze_count={bronze_metrics['row_count']} "
            f"silver_count={written_metrics['row_count']} "
            f"distinct_lineage={written_metrics['distinct_lineage']} "
            f"parquet_files={parquet_files} "
            f"output_partitions={output_partitions} "
            f"schema_version={SILVER_SCHEMA_VERSION}"
        )
    finally:
        if written is not None:
            written.unpersist()
        if silver is not None:
            silver.unpersist()
        if bronze is not None:
            bronze.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
