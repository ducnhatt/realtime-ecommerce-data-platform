"""Smoke check: write/read/delete isolated Parquet through MinIO S3A."""

import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pipeline.storage import configure_minio_s3a


def main() -> None:
    bronze_bucket = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    run_id = uuid.uuid4().hex
    test_path = f"s3a://{bronze_bucket}/_connectivity_test/run_id={run_id}"

    spark = (
        SparkSession.builder.appName("ecommerce-minio-s3a-smoke-test")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    jvm = spark.sparkContext._jvm
    path = jvm.org.apache.hadoop.fs.Path(test_path)
    filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    try:
        expected_count = 100
        expected_sum = 5050
        source = spark.range(1, expected_count + 1).withColumn(
            "storage_test_run_id", F.lit(run_id)
        )

        source.coalesce(2).write.mode("errorifexists").parquet(test_path)

        actual = spark.read.parquet(test_path).agg(
            F.count("id").alias("row_count"),
            F.sum("id").alias("id_sum"),
            F.countDistinct("storage_test_run_id").alias("run_id_count"),
        ).first()

        if actual["row_count"] != expected_count:
            raise RuntimeError(
                f"row count mismatch: expected={expected_count}, "
                f"actual={actual['row_count']}"
            )
        if actual["id_sum"] != expected_sum:
            raise RuntimeError(
                f"sum mismatch: expected={expected_sum}, actual={actual['id_sum']}"
            )
        if actual["run_id_count"] != 1:
            raise RuntimeError(
                f"run id mismatch: expected=1, actual={actual['run_id_count']}"
            )

        parquet_files = sum(
            1
            for status in filesystem.listStatus(path)
            if status.isFile() and status.getPath().getName().endswith(".parquet")
        )
        if parquet_files == 0:
            raise RuntimeError(f"no Parquet data files found at: {test_path}")
        if not filesystem.delete(path, True):
            raise RuntimeError(f"temporary test cleanup returned false: {test_path}")
        if filesystem.exists(path):
            raise RuntimeError(f"temporary test path still exists: {test_path}")

        print(
            "MINIO_S3A_SMOKE_TEST_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"bucket={bronze_bucket} row_count={actual['row_count']} "
            f"id_sum={actual['id_sum']} parquet_files={parquet_files} cleanup=true"
        )
    finally:
        # Cleanup is best-effort here. If the primary operation failed because
        # MinIO/DNS is unavailable, another S3A call must not hide that root
        # exception with a second cleanup exception.
        try:
            if filesystem.exists(path):
                filesystem.delete(path, True)
        except Exception as cleanup_error:
            print(
                "MINIO_S3A_SMOKE_TEST_CLEANUP_SKIPPED "
                f"error_type={type(cleanup_error).__name__}"
            )
        spark.stop()


if __name__ == "__main__":
    main()
