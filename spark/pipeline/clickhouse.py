"""Silver-to-ClickHouse staging contract and deterministic row hashing."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline.silver import LINEAGE_COLUMNS, SILVER_COLUMNS


SILVER_ROW_HASH_COLUMN = "silver_row_hash"

# Text/audit fields remain in Silver and are included in the immutable payload
# hash, but they are not copied into analytical staging v1.
STAGING_EXCLUDED_SILVER_COLUMNS = {
    "helpful",
    "review_text",
    "review_summary",
    "day_diff",
}

# Keep the exact physical-table order even though DataSource V2 resolves by
# name. This also makes schema review and connector diagnostics unambiguous.
STAGING_SILVER_COLUMNS = [
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "reviewer_id",
    "reviewer_name",
    "asin",
    "product_name",
    "category",
    "sub_category",
    "brand",
    "store_id",
    "store_name",
    "city",
    "region",
    "store_type",
    "review_timestamp",
    "review_date",
    "order_id",
    "unit_price",
    "quantity",
    "total_amount",
    "overall_rating",
    "helpful_yes",
    "total_vote",
    "payment_id",
    "payment_method",
    "payment_status",
    "shipping_id",
    "shipping_method",
    "carrier_name",
    "shipping_status",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
    "validation_status",
    "validation_warnings",
    "source_contract_version",
    "silver_schema_version",
]

STAGING_COLUMNS = [
    *STAGING_SILVER_COLUMNS,
    SILVER_ROW_HASH_COLUMN,
    "load_batch_id",
    "loaded_at",
]

if set(STAGING_SILVER_COLUMNS) != (
    set(SILVER_COLUMNS) - STAGING_EXCLUDED_SILVER_COLUMNS
):
    raise RuntimeError(
        "Internal staging mapping must contain exactly the 38 selected "
        "ecommerce-silver-v1 fields"
    )
if len(STAGING_COLUMNS) != 41:
    raise RuntimeError("Internal ClickHouse staging mapping must have 41 columns")

STAGING_REQUIRED_COLUMNS = [
    column
    for column in STAGING_SILVER_COLUMNS
    if column != "reviewer_name"
]


def assert_silver_columns(silver: DataFrame) -> None:
    """Fail before streaming starts if the Silver interface drifted."""

    actual = set(silver.columns)
    expected = set(SILVER_COLUMNS)
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing or extra:
        raise RuntimeError(
            "Silver schema columns do not match ecommerce-silver-v1: "
            f"missing={missing}, extra={extra}"
        )


def with_silver_row_hash(silver: DataFrame) -> DataFrame:
    """Hash all 42 ordered Silver fields using stable UTC JSON encoding."""

    payload_json = F.to_json(
        F.struct(*[F.col(column) for column in SILVER_COLUMNS]),
        {
            "ignoreNullFields": "false",
            "dateFormat": "yyyy-MM-dd",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
        },
    )
    return silver.withColumn(SILVER_ROW_HASH_COLUMN, F.sha2(payload_json, 256))


def build_staging_batch(silver: DataFrame, load_batch_id: str) -> DataFrame:
    """Validate one Silver micro-batch and map it to 41 staging columns."""

    null_condition = F.lit(False)
    for column in STAGING_REQUIRED_COLUMNS:
        null_condition = null_condition | F.col(column).isNull()

    metrics = silver.agg(
        F.count(F.lit(1)).alias("row_count"),
        F.countDistinct(F.struct(*[F.col(c) for c in LINEAGE_COLUMNS])).alias(
            "distinct_lineage"
        ),
        F.sum(F.when(null_condition, 1).otherwise(0)).alias("null_required"),
        F.sum(
            F.when(~F.col("validation_status").isin("VALID", "WARNING"), 1)
            .otherwise(0)
        ).alias("invalid_status"),
        F.sum(
            F.when(
                F.col("total_amount")
                != F.col("unit_price") * F.col("quantity"),
                1,
            ).otherwise(0)
        ).alias("amount_mismatch"),
    ).first()

    failures = []
    if metrics["distinct_lineage"] != metrics["row_count"]:
        failures.append(
            "duplicate lineage in Silver batch: "
            f"rows={metrics['row_count']}, "
            f"distinct={metrics['distinct_lineage']}"
        )
    for metric in ("null_required", "invalid_status", "amount_mismatch"):
        if metrics[metric] != 0:
            failures.append(f"{metric}={metrics[metric]}")
    if failures:
        raise RuntimeError(
            "Silver-to-ClickHouse staging contract failed: "
            + "; ".join(failures)
        )

    return (
        with_silver_row_hash(silver)
        .select(*STAGING_SILVER_COLUMNS, SILVER_ROW_HASH_COLUMN)
        .withColumn("load_batch_id", F.lit(load_batch_id))
        .withColumn("loaded_at", F.current_timestamp())
        .select(*STAGING_COLUMNS)
    )
