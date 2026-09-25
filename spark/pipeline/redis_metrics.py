"""Deterministic row-to-minute metrics for the Redis realtime read model."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


BUSINESS_STATUSES = ["VALID", "WARNING"]
SUCCESSFUL_PAYMENT_STATUS = "Thành công"


def aggregate_minute_metrics(validated: DataFrame) -> DataFrame:
    """Aggregate validated events by Kafka ingestion minute.

    INVALID rows contribute only to event/quality counters. Business-like
    measures are restricted to VALID and WARNING rows.
    """

    business_row = F.col("validation_status").isin(BUSINESS_STATUSES)
    paid_business_row = business_row & (
        F.col("paymentStatus") == SUCCESSFUL_PAYMENT_STATUS
    )
    rated_business_row = business_row & F.col("overall").isNotNull()

    metric_rows = validated.withColumn(
        "ingest_minute",
        F.date_format("kafka_timestamp", "yyyyMMddHHmm"),
    ).withColumns(
        {
            "events_total": F.lit(1).cast("long"),
            "valid_count": F.when(
                F.col("validation_status") == "VALID", 1
            ).otherwise(0).cast("long"),
            "warning_count": F.when(
                F.col("validation_status") == "WARNING", 1
            ).otherwise(0).cast("long"),
            "invalid_count": F.when(
                F.col("validation_status") == "INVALID", 1
            ).otherwise(0).cast("long"),
            "record_count": F.when(business_row, 1).otherwise(0).cast("long"),
            "quantity_sum": F.when(business_row, F.col("quantity"))
            .otherwise(0)
            .cast("long"),
            "gross_item_value": F.when(business_row, F.col("totalAmount"))
            .otherwise(0.0)
            .cast("double"),
            "successful_labeled_value": F.when(
                paid_business_row,
                F.col("totalAmount"),
            ).otherwise(0.0).cast("double"),
            "rating_sum": F.when(rated_business_row, F.col("overall"))
            .otherwise(0.0)
            .cast("double"),
            "rating_count": F.when(rated_business_row, 1)
            .otherwise(0)
            .cast("long"),
        }
    )

    return metric_rows.groupBy("ingest_minute").agg(
        *[
            F.sum(column).alias(column)
            for column in [
                "events_total",
                "valid_count",
                "warning_count",
                "invalid_count",
                "record_count",
                "quantity_sum",
                "gross_item_value",
                "successful_labeled_value",
                "rating_sum",
                "rating_count",
            ]
        ]
    )
