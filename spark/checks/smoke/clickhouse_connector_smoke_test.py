"""Smoke check: Spark DataSource V2 write/read/cleanup against ClickHouse."""

import os
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    catalog = "clickhouse"
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    user = os.getenv("CLICKHOUSE_USER", "ecommerce")
    password = os.environ["CLICKHOUSE_PASSWORD"]
    database = os.getenv("CLICKHOUSE_STAGING_DATABASE", "staging")
    table_name = f"spark_connector_smoke_{uuid.uuid4().hex}"
    table_identifier = f"{catalog}.{database}.{table_name}"

    spark = (
        SparkSession.builder.appName("ecommerce-clickhouse-connector-smoke-test")
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

    table_created = False
    try:
        spark.sql(
            f"""
            CREATE TABLE {table_identifier}
            (
                id BIGINT NOT NULL,
                payload STRING NOT NULL
            )
            USING ClickHouse
            TBLPROPERTIES
            (
                engine = 'MergeTree()',
                order_by = 'id'
            )
            """
        )
        table_created = True

        expected_count = 100
        expected_sum = 5050
        source = spark.range(1, expected_count + 1).select(
            F.col("id").cast("long").alias("id"),
            F.concat(F.lit("spark-clickhouse-"), F.col("id")).alias("payload"),
        )
        source.repartition(2).writeTo(table_identifier).append()

        actual = spark.table(table_identifier).agg(
            F.count("id").alias("row_count"),
            F.sum("id").alias("id_sum"),
            F.countDistinct("payload").alias("distinct_payloads"),
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
        if actual["distinct_payloads"] != expected_count:
            raise RuntimeError(
                "payload distinctness mismatch: "
                f"expected={expected_count}, actual={actual['distinct_payloads']}"
            )

        spark.sql(f"DROP TABLE {table_identifier}")
        table_created = False

        print(
            "SPARK_CLICKHOUSE_CONNECTOR_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"endpoint={host}:{http_port} connector_api=DataSourceV2 "
            f"row_count={actual['row_count']} id_sum={actual['id_sum']} "
            "writer_partitions=2 cleanup=true"
        )
    finally:
        if table_created:
            try:
                spark.sql(f"DROP TABLE IF EXISTS {table_identifier}")
            except Exception as cleanup_error:
                print(
                    "SPARK_CLICKHOUSE_CONNECTOR_CLEANUP_FAILED "
                    f"table={table_identifier} "
                    f"error_type={type(cleanup_error).__name__}"
                )
        spark.stop()


if __name__ == "__main__":
    main()
