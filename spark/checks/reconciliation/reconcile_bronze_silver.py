"""Reconciliation check: compare physical Bronze and Silver Parquet."""

import os

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.silver import (
    LINEAGE_COLUMNS,
    SILVER_COLUMNS,
    SILVER_SCHEMA_VERSION,
    transform_bronze_to_silver,
)
from pipeline.storage import configure_minio_s3a


def row_hash(df: DataFrame, alias: str) -> DataFrame:
    canonical = F.to_json(
        F.struct(*[F.col(column_name) for column_name in SILVER_COLUMNS]),
        {"ignoreNullFields": "false"},
    )
    return df.select(
        *LINEAGE_COLUMNS,
        F.sha2(canonical, 256).alias(alias),
    )


def count_parquet_files(spark: SparkSession, path_text: str) -> int:
    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(path_text)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    files = filesystem.listFiles(path, True)
    count = 0
    while files.hasNext():
        status = files.next()
        if status.isFile() and status.getPath().getName().endswith(".parquet"):
            count += 1
    return count


def main() -> None:
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    silver_prefix = os.getenv("SILVER_DATASET_PREFIX", topic)
    bronze_path = f"s3a://{bronze_bucket}/{topic}"
    silver_path = f"s3a://{silver_bucket}/{silver_prefix}"

    spark = (
        SparkSession.builder.appName("ecommerce-bronze-silver-reconciliation-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    bronze = None
    expected = None
    actual = None
    try:
        bronze = spark.read.parquet(bronze_path).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        expected = transform_bronze_to_silver(bronze).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        actual = spark.read.parquet(silver_path).select(*SILVER_COLUMNS).persist(
            StorageLevel.MEMORY_AND_DISK
        )

        bronze_count = bronze.count()
        expected_count = expected.count()
        silver_count = actual.count()
        silver_distinct_lineage = actual.select(*LINEAGE_COLUMNS).distinct().count()

        expected_lineage = expected.select(*LINEAGE_COLUMNS)
        actual_lineage = actual.select(*LINEAGE_COLUMNS)
        missing = expected_lineage.join(
            actual_lineage, LINEAGE_COLUMNS, "left_anti"
        ).count()
        extra = actual_lineage.join(
            expected_lineage, LINEAGE_COLUMNS, "left_anti"
        ).count()

        expected_hashes = row_hash(expected, "expected_hash")
        actual_hashes = row_hash(actual, "actual_hash")
        payload_mismatches = (
            expected_hashes.join(actual_hashes, LINEAGE_COLUMNS, "inner")
            .filter(F.col("expected_hash") != F.col("actual_hash"))
            .count()
        )

        wrong_version = actual.filter(
            (F.col("silver_schema_version") != SILVER_SCHEMA_VERSION)
            | (F.col("source_contract_version") != "ecommerce-event-v1")
        ).count()
        invalid_status = actual.filter(
            ~F.col("validation_status").isin("VALID", "WARNING")
        ).count()
        amount_mismatches = actual.filter(
            F.col("total_amount") != F.col("unit_price") * F.col("quantity")
        ).count()

        expected_types = dict(expected.dtypes)
        actual_types = dict(actual.dtypes)
        schema_mismatches = [
            column_name
            for column_name in SILVER_COLUMNS
            if expected_types.get(column_name) != actual_types.get(column_name)
        ]

        status_counts = {
            row["validation_status"]: row["count"]
            for row in actual.groupBy("validation_status").count().collect()
        }
        ingest_dates = actual.select("ingest_date").distinct().count()
        parquet_files = count_parquet_files(spark, silver_path)

        failures = []
        if expected_count != bronze_count:
            failures.append(
                f"Bronze transform count mismatch: bronze={bronze_count}, "
                f"expected={expected_count}"
            )
        if silver_count != expected_count:
            failures.append(
                f"Silver count mismatch: expected={expected_count}, "
                f"actual={silver_count}"
            )
        if silver_distinct_lineage != silver_count:
            failures.append(
                f"duplicate Silver lineage: rows={silver_count}, "
                f"distinct={silver_distinct_lineage}"
            )
        for label, value in {
            "missing": missing,
            "extra": extra,
            "payload_mismatches": payload_mismatches,
            "wrong_version": wrong_version,
            "invalid_status": invalid_status,
            "amount_mismatches": amount_mismatches,
        }.items():
            if value != 0:
                failures.append(f"{label}={value}")
        if schema_mismatches:
            failures.append(f"schema_mismatches={schema_mismatches}")
        if parquet_files == 0:
            failures.append("parquet_files=0")

        if failures:
            raise RuntimeError(
                "Bronze/Silver reconciliation failed:\n" + "\n".join(failures)
            )

        print(
            "BRONZE_SILVER_RECONCILIATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"bronze_count={bronze_count} silver_count={silver_count} "
            f"distinct_lineage={silver_distinct_lineage} "
            f"valid_count={status_counts.get('VALID', 0)} "
            f"warning_count={status_counts.get('WARNING', 0)} "
            f"missing={missing} extra={extra} "
            f"payload_mismatches={payload_mismatches} "
            f"schema_mismatches={len(schema_mismatches)} "
            f"ingest_dates={ingest_dates} parquet_files={parquet_files}"
        )
    finally:
        if actual is not None:
            actual.unpersist()
        if expected is not None:
            expected.unpersist()
        if bronze is not None:
            bronze.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
