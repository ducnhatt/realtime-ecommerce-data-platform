"""Read-only reconciliation across Silver, ClickHouse staging, and Gold."""

import os
from functools import reduce

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.clickhouse import (
    SILVER_ROW_HASH_COLUMN,
    STAGING_SILVER_COLUMNS,
    assert_silver_columns,
    with_silver_row_hash,
)
from pipeline.silver import LINEAGE_COLUMNS
from pipeline.storage import configure_minio_s3a


GOLD_FACT_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "asin",
    "store_id",
    "reviewer_id",
    "review_date",
    "review_timestamp",
    "order_id",
    "payment_id",
    "shipping_id",
    "unit_price",
    "quantity",
    "total_amount",
    "overall_rating",
    "helpful_yes",
    "total_vote",
    "payment_method",
    "payment_status",
    "shipping_method",
    "carrier_name",
    "shipping_status",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
    "validation_status",
]


def configure_clickhouse(spark_builder, catalog: str):
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    http_port = os.getenv("CLICKHOUSE_HTTP_PORT", "8123")
    user = os.getenv("CLICKHOUSE_USER", "ecommerce")
    password = os.environ["CLICKHOUSE_PASSWORD"]
    staging_database = os.getenv("CLICKHOUSE_STAGING_DATABASE", "staging")
    return (
        spark_builder.config(
            f"spark.sql.catalog.{catalog}",
            "com.clickhouse.spark.ClickHouseCatalog",
        )
        .config(f"spark.sql.catalog.{catalog}.host", host)
        .config(f"spark.sql.catalog.{catalog}.protocol", "http")
        .config(f"spark.sql.catalog.{catalog}.http_port", http_port)
        .config(f"spark.sql.catalog.{catalog}.user", user)
        .config(f"spark.sql.catalog.{catalog}.password", password)
        .config(f"spark.sql.catalog.{catalog}.database", staging_database)
    )


def distinct_lineage_count(df: DataFrame) -> int:
    return df.select(*LINEAGE_COLUMNS).distinct().count()


def lineage_delta(left: DataFrame, right: DataFrame) -> int:
    return (
        left.select(*LINEAGE_COLUMNS)
        .join(right.select(*LINEAGE_COLUMNS), LINEAGE_COLUMNS, "left_anti")
        .count()
    )


def payload_mismatch_count(
    left: DataFrame,
    right: DataFrame,
    compared_columns,
) -> int:
    left_alias = left.alias("left_data")
    right_alias = right.alias("right_data")
    joined = left_alias.join(right_alias, LINEAGE_COLUMNS, "inner")
    mismatch = reduce(
        lambda current, column: current
        | ~F.col(f"left_data.{column}").eqNullSafe(
            F.col(f"right_data.{column}")
        ),
        compared_columns,
        F.lit(False),
    )
    return joined.filter(mismatch).count()


def collect_kpis(df: DataFrame):
    return df.agg(
        F.count(F.lit(1)).alias("event_count"),
        F.sum("quantity").alias("quantity_sum"),
        F.sum("total_amount").alias("gross_item_value"),
        F.avg("overall_rating").alias("avg_rating"),
    ).first()


def main() -> None:
    catalog = "clickhouse"
    silver_bucket = os.getenv("MINIO_SILVER_BUCKET", "silver")
    silver_prefix = os.getenv(
        "SILVER_INCREMENTAL_PREFIX",
        "ecommerce_reviews_incremental_v1",
    )
    staging_database = os.getenv("CLICKHOUSE_STAGING_DATABASE", "staging")
    gold_database = os.getenv("CLICKHOUSE_GOLD_DATABASE", "gold")
    silver_path = f"s3a://{silver_bucket}/{silver_prefix}"
    staging_current_identifier = (
        f"{catalog}.{staging_database}.ecommerce_events_current"
    )
    staging_conflicts_identifier = (
        f"{catalog}.{staging_database}.ecommerce_events_conflicts"
    )
    gold_identifier = f"{catalog}.{gold_database}.fact_ecommerce_event"

    spark = configure_clickhouse(
        SparkSession.builder.appName(
            "ecommerce-silver-staging-gold-reconciliation-v1"
        ).config("spark.sql.session.timeZone", "UTC"),
        catalog,
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    silver = None
    staging = None
    gold = None
    try:
        silver_source = spark.read.parquet(silver_path)
        assert_silver_columns(silver_source)
        silver = (
            with_silver_row_hash(silver_source)
            .select(
                *STAGING_SILVER_COLUMNS,
                SILVER_ROW_HASH_COLUMN,
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        staging = (
            spark.table(staging_current_identifier)
            .select(
                *STAGING_SILVER_COLUMNS,
                SILVER_ROW_HASH_COLUMN,
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        gold = (
            spark.table(gold_identifier)
            .select(*GOLD_FACT_COLUMNS)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        silver_count = silver.count()
        staging_count = staging.count()
        gold_count = gold.count()
        silver_distinct = distinct_lineage_count(silver)
        staging_distinct = distinct_lineage_count(staging)
        gold_distinct = distinct_lineage_count(gold)
        staging_conflicts = spark.table(staging_conflicts_identifier).count()

        silver_missing_in_staging = lineage_delta(silver, staging)
        staging_extra_vs_silver = lineage_delta(staging, silver)
        silver_staging_hash_mismatches = (
            silver.select(*LINEAGE_COLUMNS, SILVER_ROW_HASH_COLUMN)
            .alias("silver")
            .join(
                staging.select(*LINEAGE_COLUMNS, SILVER_ROW_HASH_COLUMN).alias(
                    "staging"
                ),
                LINEAGE_COLUMNS,
                "inner",
            )
            .filter(
                F.col(f"silver.{SILVER_ROW_HASH_COLUMN}")
                != F.col(f"staging.{SILVER_ROW_HASH_COLUMN}")
            )
            .count()
        )

        staging_gold = staging.select(*GOLD_FACT_COLUMNS)
        staging_missing_in_gold = lineage_delta(staging_gold, gold)
        gold_extra_vs_staging = lineage_delta(gold, staging_gold)
        gold_payload_mismatches = payload_mismatch_count(
            staging_gold,
            gold,
            [
                column
                for column in GOLD_FACT_COLUMNS
                if column not in LINEAGE_COLUMNS
            ],
        )

        staging_kpis = collect_kpis(staging_gold)
        gold_kpis = collect_kpis(gold)
        kpi_mismatches = sum(
            1
            for metric in (
                "event_count",
                "quantity_sum",
                "gross_item_value",
                "avg_rating",
            )
            if staging_kpis[metric] != gold_kpis[metric]
        )

        failures = []
        if silver_count != staging_count or staging_count != gold_count:
            failures.append(
                "row count mismatch: "
                f"silver={silver_count}, staging={staging_count}, gold={gold_count}"
            )
        for label, value in {
            "silver_duplicate_lineage": silver_count - silver_distinct,
            "staging_duplicate_lineage": staging_count - staging_distinct,
            "gold_duplicate_lineage": gold_count - gold_distinct,
            "staging_conflicts": staging_conflicts,
            "silver_missing_in_staging": silver_missing_in_staging,
            "staging_extra_vs_silver": staging_extra_vs_silver,
            "silver_staging_hash_mismatches": silver_staging_hash_mismatches,
            "staging_missing_in_gold": staging_missing_in_gold,
            "gold_extra_vs_staging": gold_extra_vs_staging,
            "gold_payload_mismatches": gold_payload_mismatches,
            "kpi_mismatches": kpi_mismatches,
        }.items():
            if value != 0:
                failures.append(f"{label}={value}")

        if failures:
            raise RuntimeError(
                "Silver/staging/Gold reconciliation failed:\n"
                + "\n".join(failures)
            )

        print(
            "SILVER_STAGING_GOLD_RECONCILIATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"silver_path={silver_path} "
            f"staging={staging_database}.ecommerce_events_current "
            f"gold={gold_database}.fact_ecommerce_event "
            f"silver_count={silver_count} staging_count={staging_count} "
            f"gold_count={gold_count} distinct_lineage={gold_distinct} "
            f"staging_conflicts={staging_conflicts} "
            f"silver_staging_missing={silver_missing_in_staging} "
            f"silver_staging_extra={staging_extra_vs_silver} "
            f"silver_staging_hash_mismatches={silver_staging_hash_mismatches} "
            f"staging_gold_missing={staging_missing_in_gold} "
            f"staging_gold_extra={gold_extra_vs_staging} "
            f"gold_payload_mismatches={gold_payload_mismatches} "
            f"kpi_mismatches={kpi_mismatches} "
            f"event_count={gold_kpis['event_count']} "
            f"quantity_sum={gold_kpis['quantity_sum']} "
            f"gross_item_value={gold_kpis['gross_item_value']} "
            f"avg_rating={gold_kpis['avg_rating']}"
        )
    finally:
        if gold is not None:
            gold.unpersist()
        if staging is not None:
            staging.unpersist()
        if silver is not None:
            silver.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
