"""Native Spark parsing and validation for ecommerce-event-v1."""

from functools import reduce
from operator import or_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pipeline.schema import (
    CONTRACT_VERSION,
    INTEGER_FIELDS,
    NULLABLE_FIELDS,
    NUMBER_FIELDS,
    SOURCE_FIELDS,
    SOURCE_SCHEMA,
    STRING_FIELDS,
)


CATEGORIES = ["Điện tử", "Thời trang", "Gia dụng", "Mỹ phẩm"]
SUBCATEGORIES = [
    "Điện thoại", "Laptop", "Tai nghe", "Đồng hồ thông minh",
    "Áo thun", "Quần Jeans", "Giày Sneaker", "Áo khoác",
    "Máy xay sinh tố", "Lò vi sóng", "Nồi chiên không dầu", "Máy hút bụi",
    "Son môi", "Serum", "Nước hoa", "Sữa rửa mặt",
]
BRANDS = [
    "Apple", "Samsung", "Sony", "Nike", "Adidas", "Philips",
    "Lock&Lock", "L'Oreal", "Dyson", "LG",
]
CITIES = [
    "Hà Nội", "Hải Phòng", "Quảng Ninh", "Đà Nẵng", "Huế", "Nha Trang",
    "Hồ Chí Minh", "Cần Thơ", "Đồng Nai",
]
REGIONS = ["Miền Bắc", "Miền Trung", "Miền Nam"]
STORE_TYPES = ["Cửa hàng Vật lý", "Web Online", "Mobile App"]
PAYMENT_METHODS = ["Tiền mặt (COD)", "Thẻ Tín Dụng", "Momo", "ZaloPay", "VNPay"]
PAYMENT_STATUSES = ["Thành công", "Đang xử lý", "Thất bại"]
SHIPPING_METHODS = ["Hỏa tốc", "Tiêu chuẩn", "Tiết kiệm"]
CARRIERS = ["Giao Hàng Nhanh", "Viettel Post", "Shopee Express", "Ninja Van", "J&T Express"]
SHIPPING_STATUSES = ["Đã giao", "Đang vận chuyển", "Hoàn hàng"]

CATEGORY_SUBCATEGORIES = {
    "Điện tử": ["Điện thoại", "Laptop", "Tai nghe", "Đồng hồ thông minh"],
    "Thời trang": ["Áo thun", "Quần Jeans", "Giày Sneaker", "Áo khoác"],
    "Gia dụng": ["Máy xay sinh tố", "Lò vi sóng", "Nồi chiên không dầu", "Máy hút bụi"],
    "Mỹ phẩm": ["Son môi", "Serum", "Nước hoa", "Sữa rửa mặt"],
}

REGION_CITIES = {
    "Miền Bắc": ["Hà Nội", "Hải Phòng", "Quảng Ninh"],
    "Miền Trung": ["Đà Nẵng", "Huế", "Nha Trang"],
    "Miền Nam": ["Hồ Chí Minh", "Cần Thơ", "Đồng Nai"],
}

NATURAL_CATEGORY_BRANDS = {
    "Điện tử": ["Apple", "Samsung", "Sony", "LG"],
    "Thời trang": ["Nike", "Adidas"],
    "Gia dụng": ["Philips", "Lock&Lock", "Dyson", "LG"],
    "Mỹ phẩm": ["L'Oreal"],
}


def _any(conditions: list[Column]) -> Column:
    return reduce(or_, conditions, F.lit(False))


def _rule(code: str, condition: Column) -> Column:
    return F.when(condition, F.lit(code))


def _rules_array(rules: list[Column]) -> Column:
    return F.array_compact(F.array(*rules))


def _variant_schema(field_name: str) -> Column:
    value = F.try_variant_get("json_variant", f"$.{field_name}", "variant")
    return F.schema_of_variant(value)


def _mapping_mismatch(parent: str, child: str, mapping: dict[str, list[str]]) -> Column:
    valid_condition = F.lit(False)
    for parent_value, child_values in mapping.items():
        valid_condition = valid_condition | (
            (F.col(parent) == parent_value) & F.col(child).isin(child_values)
        )
    return ~valid_condition


def validate_events(df: DataFrame) -> DataFrame:
    """Parse and classify rows that contain a `raw_payload` string column."""

    required_fields = F.array(*[F.lit(name) for name in SOURCE_FIELDS])

    parsed = (
        df.withColumn("json_variant", F.try_parse_json("raw_payload"))
        .withColumn("json_variant_schema", F.schema_of_variant("json_variant"))
        .withColumn("json_keys", F.json_object_keys("raw_payload"))
        .withColumn("payload", F.from_json("raw_payload", SOURCE_SCHEMA))
        .withColumn(
            "is_json_object",
            F.col("json_variant").isNotNull()
            & F.col("json_variant_schema").startswith("OBJECT<"),
        )
        .withColumn(
            "missing_fields",
            F.when(
                F.col("is_json_object"),
                F.array_except(required_fields, F.col("json_keys")),
            ).otherwise(required_fields),
        )
        .withColumn(
            "unexpected_fields",
            F.when(
                F.col("is_json_object"),
                F.array_except(F.col("json_keys"), required_fields),
            ).otherwise(F.array().cast("array<string>")),
        )
    )

    for field_name in SOURCE_FIELDS:
        parsed = parsed.withColumn(field_name, F.col(f"payload.{field_name}"))

    object_row = F.col("is_json_object")
    all_fields_present = object_row & (F.size("missing_fields") == 0)
    no_unexpected_fields = object_row & (F.size("unexpected_fields") == 0)

    schema_columns = {name: _variant_schema(name) for name in SOURCE_FIELDS}

    required_null = _any(
        [schema_columns[name] == "VOID" for name in SOURCE_FIELDS if name not in NULLABLE_FIELDS]
    )

    string_type_mismatch = _any(
        [
            schema_columns[name].isNotNull()
            & (schema_columns[name] != "VOID")
            & (schema_columns[name] != "STRING")
            for name in STRING_FIELDS
        ]
    )
    integer_type_mismatch = _any(
        [
            schema_columns[name].isNotNull()
            & (schema_columns[name] != "VOID")
            & (schema_columns[name] != "BIGINT")
            for name in INTEGER_FIELDS
        ]
    )
    number_type_mismatch = _any(
        [
            schema_columns[name].isNotNull()
            & (schema_columns[name] != "VOID")
            & ~schema_columns[name].rlike(r"^(BIGINT|DOUBLE|FLOAT|DECIMAL\()")
            for name in NUMBER_FIELDS
        ]
    )
    helpful_schema = schema_columns["helpful"]
    helpful_type_mismatch = (
        helpful_schema.isNotNull()
        & (helpful_schema != "VOID")
        & (helpful_schema != "ARRAY<BIGINT>")
    )
    type_mismatch = (
        string_type_mismatch
        | integer_type_mismatch
        | number_type_mismatch
        | helpful_type_mismatch
    )

    structural_errors = _rules_array(
        [
            _rule("E001_MALFORMED_JSON", F.col("json_variant").isNull()),
            _rule(
                "E002_TOP_LEVEL_NOT_OBJECT",
                F.col("json_variant").isNotNull() & ~object_row,
            ),
            _rule("E003_MISSING_FIELDS", object_row & (F.size("missing_fields") > 0)),
            _rule("E004_UNEXPECTED_FIELDS", object_row & (F.size("unexpected_fields") > 0)),
            _rule("E005_TYPE_MISMATCH", all_fields_present & type_mismatch),
            _rule("E006_REQUIRED_NULL", all_fields_present & required_null),
        ]
    )

    parsed = parsed.withColumn("structural_errors", structural_errors)
    eligible = (
        all_fields_present
        & no_unexpected_fields
        & (F.size("structural_errors") == 0)
    )

    field_errors = _rules_array(
        [
            _rule("E101_REVIEWER_ID_PATTERN", eligible & ~F.col("reviewerID").rlike(r"^[A-Z0-9]{14}$")),
            _rule("E102_ASIN_PATTERN", eligible & ~F.col("asin").rlike(r"^B0[A-Z0-9]{8}$")),
            _rule("E103_REVIEW_TEXT_EMPTY", eligible & (F.length(F.trim("reviewText")) == 0)),
            _rule("E104_OVERALL_RANGE", eligible & ~F.col("overall").isin([1.0, 2.0, 3.0, 4.0, 5.0])),
            _rule("E105_SUMMARY_EMPTY", eligible & (F.length(F.trim("summary")) == 0)),
            _rule("E106_REVIEW_TIME_RANGE", eligible & ~F.col("unixReviewTime").between(1672500000, 1735700000)),
            _rule("E107_DAY_DIFF_RANGE", eligible & ~F.col("day_diff").between(4, 1064)),
            _rule("E108_HELPFUL_YES_RANGE", eligible & ~F.col("helpful_yes").between(0, 500)),
            _rule("E109_TOTAL_VOTE_RANGE", eligible & ~F.col("total_vote").between(0, 500)),
            _rule("E110_PRODUCT_NAME_EMPTY", eligible & (F.length(F.trim("productName")) == 0)),
            _rule("E111_CATEGORY_ENUM", eligible & ~F.col("category").isin(CATEGORIES)),
            _rule("E112_SUBCATEGORY_ENUM", eligible & ~F.col("sub_category").isin(SUBCATEGORIES)),
            _rule("E113_BRAND_ENUM", eligible & ~F.col("brand").isin(BRANDS)),
            _rule("E114_UNIT_PRICE_RANGE", eligible & ~F.col("unitPrice").between(50000, 5000000)),
            _rule("E115_ORDER_ID_PATTERN", eligible & ~F.col("orderID").rlike(r"^ORD-[A-Z0-9]{10}$")),
            _rule("E116_QUANTITY_RANGE", eligible & ~F.col("quantity").between(1, 4)),
            _rule("E117_TOTAL_AMOUNT_RANGE", eligible & ~F.col("totalAmount").between(50000, 20000000)),
            _rule("E118_STORE_ID_PATTERN", eligible & ~F.col("storeID").rlike(r"^STR-.+-(?:[1-9]|10)$")),
            _rule("E119_STORE_NAME_EMPTY", eligible & (F.length(F.trim("storeName")) == 0)),
            _rule("E120_CITY_ENUM", eligible & ~F.col("city").isin(CITIES)),
            _rule("E121_REGION_ENUM", eligible & ~F.col("region").isin(REGIONS)),
            _rule("E122_STORE_TYPE_ENUM", eligible & ~F.col("storeType").isin(STORE_TYPES)),
            _rule("E123_PAYMENT_ID_PATTERN", eligible & ~F.col("paymentID").rlike(r"^PAY-[A-Z0-9]{10}$")),
            _rule("E124_PAYMENT_METHOD_ENUM", eligible & ~F.col("paymentMethod").isin(PAYMENT_METHODS)),
            _rule("E125_PAYMENT_STATUS_ENUM", eligible & ~F.col("paymentStatus").isin(PAYMENT_STATUSES)),
            _rule("E126_SHIPPING_ID_PATTERN", eligible & ~F.col("shippingID").rlike(r"^SHP-[A-Z0-9]{10}$")),
            _rule("E127_SHIPPING_METHOD_ENUM", eligible & ~F.col("shippingMethod").isin(SHIPPING_METHODS)),
            _rule("E128_CARRIER_ENUM", eligible & ~F.col("carrierName").isin(CARRIERS)),
            _rule("E129_SHIPPING_STATUS_ENUM", eligible & ~F.col("shippingStatus").isin(SHIPPING_STATUSES)),
            _rule("E130_HELPFUL_SHAPE", eligible & (F.size("helpful") != 2)),
        ]
    )

    parsed = parsed.withColumn("field_errors", field_errors)
    cross_eligible = eligible & (F.size("field_errors") == 0)

    cross_field_errors = _rules_array(
        [
            _rule(
                "E201_TOTAL_AMOUNT_MISMATCH",
                cross_eligible
                & (F.abs(F.col("totalAmount") - F.col("unitPrice") * F.col("quantity")) > 0.001),
            ),
            _rule(
                "E202_HELPFUL_MISMATCH",
                cross_eligible
                & (
                    (F.col("helpful").getItem(0) != F.col("helpful_yes"))
                    | (F.col("helpful").getItem(1) != F.col("total_vote"))
                ),
            ),
            _rule(
                "E203_HELPFUL_EXCEEDS_TOTAL",
                cross_eligible & (F.col("helpful_yes") > F.col("total_vote")),
            ),
            _rule(
                "E204_CATEGORY_SUBCATEGORY_MISMATCH",
                cross_eligible & _mapping_mismatch("category", "sub_category", CATEGORY_SUBCATEGORIES),
            ),
            _rule(
                "E205_REGION_CITY_MISMATCH",
                cross_eligible & _mapping_mismatch("region", "city", REGION_CITIES),
            ),
            _rule(
                "E206_REVIEW_TIME_MISMATCH",
                cross_eligible
                & (
                    F.to_date("reviewTime", "MM dd, yyyy")
                    != F.to_date(F.from_unixtime("unixReviewTime"))
                ),
            ),
        ]
    )

    warnings = _rules_array(
        [
            _rule(
                "W101_UNUSUAL_BRAND_CATEGORY",
                eligible & _mapping_mismatch("category", "brand", NATURAL_CATEGORY_BRANDS),
            ),
            _rule(
                "W102_PAYMENT_SHIPPING_INCONSISTENT",
                eligible
                & (
                    ((F.col("paymentStatus") == "Thất bại") & F.col("shippingStatus").isin(["Đã giao", "Đang vận chuyển"]))
                    | ((F.col("paymentStatus") == "Đang xử lý") & (F.col("shippingStatus") == "Đã giao"))
                ),
            ),
        ]
    )

    result = (
        parsed.withColumn(
            "validation_errors",
            F.concat("structural_errors", "field_errors", cross_field_errors),
        )
        .withColumn("validation_warnings", warnings)
        .withColumn(
            "validation_status",
            F.when(F.size("validation_errors") > 0, F.lit("INVALID"))
            .when(F.size("validation_warnings") > 0, F.lit("WARNING"))
            .otherwise(F.lit("VALID")),
        )
        .withColumn("contract_version", F.lit(CONTRACT_VERSION))
        .withColumn("ingested_at", F.current_timestamp())
        .drop("structural_errors", "field_errors")
    )

    return result
