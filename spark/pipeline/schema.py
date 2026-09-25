"""Source contract v1 represented as an explicit Spark schema."""

from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)


CONTRACT_VERSION = "ecommerce-event-v1"

SOURCE_FIELDS = [
    "reviewerID",
    "asin",
    "reviewerName",
    "helpful",
    "reviewText",
    "overall",
    "summary",
    "unixReviewTime",
    "reviewTime",
    "day_diff",
    "helpful_yes",
    "total_vote",
    "productName",
    "category",
    "sub_category",
    "brand",
    "unitPrice",
    "orderID",
    "quantity",
    "totalAmount",
    "storeID",
    "storeName",
    "city",
    "region",
    "storeType",
    "paymentID",
    "paymentMethod",
    "paymentStatus",
    "shippingID",
    "shippingMethod",
    "carrierName",
    "shippingStatus",
]

NULLABLE_FIELDS = {"reviewerName"}

STRING_FIELDS = [
    "reviewerID",
    "asin",
    "reviewerName",
    "reviewText",
    "summary",
    "reviewTime",
    "productName",
    "category",
    "sub_category",
    "brand",
    "orderID",
    "storeID",
    "storeName",
    "city",
    "region",
    "storeType",
    "paymentID",
    "paymentMethod",
    "paymentStatus",
    "shippingID",
    "shippingMethod",
    "carrierName",
    "shippingStatus",
]

INTEGER_FIELDS = [
    "unixReviewTime",
    "day_diff",
    "helpful_yes",
    "total_vote",
    "quantity",
]

NUMBER_FIELDS = ["overall", "unitPrice", "totalAmount"]

SOURCE_SCHEMA = StructType(
    [
        StructField("reviewerID", StringType(), True),
        StructField("asin", StringType(), True),
        StructField("reviewerName", StringType(), True),
        StructField("helpful", ArrayType(IntegerType(), containsNull=True), True),
        StructField("reviewText", StringType(), True),
        StructField("overall", DoubleType(), True),
        StructField("summary", StringType(), True),
        StructField("unixReviewTime", LongType(), True),
        StructField("reviewTime", StringType(), True),
        StructField("day_diff", IntegerType(), True),
        StructField("helpful_yes", IntegerType(), True),
        StructField("total_vote", IntegerType(), True),
        StructField("productName", StringType(), True),
        StructField("category", StringType(), True),
        StructField("sub_category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("unitPrice", DoubleType(), True),
        StructField("orderID", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("storeID", StringType(), True),
        StructField("storeName", StringType(), True),
        StructField("city", StringType(), True),
        StructField("region", StringType(), True),
        StructField("storeType", StringType(), True),
        StructField("paymentID", StringType(), True),
        StructField("paymentMethod", StringType(), True),
        StructField("paymentStatus", StringType(), True),
        StructField("shippingID", StringType(), True),
        StructField("shippingMethod", StringType(), True),
        StructField("carrierName", StringType(), True),
        StructField("shippingStatus", StringType(), True),
    ]
)
