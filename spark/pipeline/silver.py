"""Deterministic Bronze-to-Silver transformation for ecommerce-silver-v1."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


SILVER_SCHEMA_VERSION = "ecommerce-silver-v1"
LINEAGE_COLUMNS = ["kafka_topic", "kafka_partition", "kafka_offset"]
BUSINESS_STATUSES = ["VALID", "WARNING"]

SILVER_COLUMNS = [
    "reviewer_id",
    "asin",
    "reviewer_name",
    "helpful",
    "review_text",
    "overall_rating",
    "review_summary",
    "review_timestamp",
    "review_date",
    "day_diff",
    "helpful_yes",
    "total_vote",
    "product_name",
    "category",
    "sub_category",
    "brand",
    "unit_price",
    "order_id",
    "quantity",
    "total_amount",
    "store_id",
    "store_name",
    "city",
    "region",
    "store_type",
    "payment_id",
    "payment_method",
    "payment_status",
    "shipping_id",
    "shipping_method",
    "carrier_name",
    "shipping_status",
    "kafka_topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingested_at",
    "ingest_date",
    "validation_status",
    "validation_warnings",
    "source_contract_version",
    "silver_schema_version",
]


def transform_bronze_to_silver(
    bronze: DataFrame,
    *,
    deduplicate: bool = True,
) -> DataFrame:
    """Flatten and type eligible Bronze rows; optionally deduplicate a batch."""

    eligible = bronze.filter(F.col("validation_status").isin(BUSINESS_STATUSES))

    transformed = eligible.select(
        F.col("payload.reviewerID").alias("reviewer_id"),
        F.col("payload.asin").alias("asin"),
        F.col("payload.reviewerName").alias("reviewer_name"),
        F.col("payload.helpful").cast("array<int>").alias("helpful"),
        F.col("payload.reviewText").alias("review_text"),
        F.col("payload.overall").cast("tinyint").alias("overall_rating"),
        F.col("payload.summary").alias("review_summary"),
        F.to_timestamp(
            F.from_unixtime(F.col("payload.unixReviewTime"))
        ).alias("review_timestamp"),
        F.to_date(F.col("payload.reviewTime"), "MM dd, yyyy").alias(
            "review_date"
        ),
        F.col("payload.day_diff").cast("int").alias("day_diff"),
        F.col("payload.helpful_yes").cast("int").alias("helpful_yes"),
        F.col("payload.total_vote").cast("int").alias("total_vote"),
        F.col("payload.productName").alias("product_name"),
        F.col("payload.category").alias("category"),
        F.col("payload.sub_category").alias("sub_category"),
        F.col("payload.brand").alias("brand"),
        F.col("payload.unitPrice").cast("decimal(18,2)").alias("unit_price"),
        F.col("payload.orderID").alias("order_id"),
        F.col("payload.quantity").cast("int").alias("quantity"),
        F.col("payload.totalAmount").cast("decimal(18,2)").alias(
            "total_amount"
        ),
        F.col("payload.storeID").alias("store_id"),
        F.col("payload.storeName").alias("store_name"),
        F.col("payload.city").alias("city"),
        F.col("payload.region").alias("region"),
        F.col("payload.storeType").alias("store_type"),
        F.col("payload.paymentID").alias("payment_id"),
        F.col("payload.paymentMethod").alias("payment_method"),
        F.col("payload.paymentStatus").alias("payment_status"),
        F.col("payload.shippingID").alias("shipping_id"),
        F.col("payload.shippingMethod").alias("shipping_method"),
        F.col("payload.carrierName").alias("carrier_name"),
        F.col("payload.shippingStatus").alias("shipping_status"),
        F.col("kafka_topic"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset").cast("long").alias("kafka_offset"),
        F.col("kafka_timestamp").cast("timestamp").alias("kafka_timestamp"),
        F.col("ingested_at").cast("timestamp").alias("ingested_at"),
        F.col("ingest_date").cast("date").alias("ingest_date"),
        F.col("validation_status"),
        F.col("validation_warnings"),
        F.col("contract_version").alias("source_contract_version"),
        F.lit(SILVER_SCHEMA_VERSION).alias("silver_schema_version"),
    )

    if deduplicate:
        transformed = transformed.dropDuplicates(LINEAGE_COLUMNS)
    return transformed.select(*SILVER_COLUMNS)
