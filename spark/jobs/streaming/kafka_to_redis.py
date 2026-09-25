"""Streaming job: materialize Kafka events into the Redis read model."""

import json
import os
import socket
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.redis_metrics import aggregate_minute_metrics
from pipeline.storage import configure_minio_s3a
from pipeline.validation import validate_events


REDIS_APPLY_BATCH_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end

local payload = cjson.decode(ARGV[1])
local ttl = tonumber(ARGV[2])
local latest_limit = tonumber(ARGV[3])

for field, value in pairs(payload.summary_int) do
    redis.call('HINCRBY', KEYS[2], field, value)
end
for field, value in pairs(payload.summary_float) do
    redis.call('HINCRBYFLOAT', KEYS[2], field, value)
end

for minute_key, metrics in pairs(payload.minutes) do
    for field, value in pairs(metrics.int_values) do
        redis.call('HINCRBY', minute_key, field, value)
    end
    for field, value in pairs(metrics.float_values) do
        redis.call('HINCRBYFLOAT', minute_key, field, value)
    end
    redis.call('EXPIRE', minute_key, ttl)
end

for _, event_json in ipairs(payload.latest_events) do
    redis.call('LPUSH', KEYS[3], event_json)
end
redis.call('LTRIM', KEYS[3], 0, latest_limit - 1)

redis.call(
    'HSET', KEYS[4],
    'last_batch_id', payload.batch_id,
    'last_input_rows', payload.input_rows,
    'last_minute_buckets', payload.minute_buckets,
    'last_updated_at', payload.updated_at,
    'status', 'OK'
)
redis.call('SET', KEYS[1], 'applied', 'EX', ttl)
return 1
"""

INTEGER_METRICS = [
    "events_total",
    "valid_count",
    "warning_count",
    "invalid_count",
    "record_count",
    "quantity_sum",
    "rating_count",
]

FLOAT_METRICS = [
    "gross_item_value",
    "successful_labeled_value",
    "rating_sum",
]


def encode_command(*parts: object) -> bytes:
    encoded = [str(part).encode("utf-8") for part in parts]
    command = [f"*{len(encoded)}\r\n".encode("ascii")]
    for part in encoded:
        command.extend(
            [f"${len(part)}\r\n".encode("ascii"), part, b"\r\n"]
        )
    return b"".join(command)


def read_response(stream):
    prefix = stream.read(1)
    if not prefix:
        raise RuntimeError("Redis closed the connection without a response")
    line = stream.readline()
    if not line.endswith(b"\r\n"):
        raise RuntimeError("Malformed Redis response")
    value = line[:-2]
    if prefix == b"+":
        return value.decode("utf-8")
    if prefix == b"-":
        raise RuntimeError(f"Redis error: {value.decode('utf-8')}")
    if prefix == b":":
        return int(value)
    if prefix == b"$":
        length = int(value)
        if length == -1:
            return None
        payload = stream.read(length)
        if stream.read(2) != b"\r\n":
            raise RuntimeError("Malformed Redis bulk response")
        return payload.decode("utf-8")
    raise RuntimeError(f"Unsupported Redis response prefix: {prefix!r}")


def execute(stream, connection, *parts: object):
    connection.sendall(encode_command(*parts))
    return read_response(stream)


def read_kafka_stream(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
    max_offsets_per_trigger: int,
) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("message_key"),
            F.col("value").cast("string").alias("raw_payload"),
        )
    )


def build_payload(batch_df: DataFrame, batch_id: int, namespace: str) -> dict:
    minute_rows = aggregate_minute_metrics(batch_df).collect()
    summary_int = {metric: 0 for metric in INTEGER_METRICS}
    summary_float = {metric: 0.0 for metric in FLOAT_METRICS}
    minutes = {}

    for row in minute_rows:
        values = row.asDict()
        minute_key = f"{namespace}:minute:{values['ingest_minute']}"
        int_values = {metric: int(values[metric]) for metric in INTEGER_METRICS}
        float_values = {
            metric: float(values[metric]) for metric in FLOAT_METRICS
        }
        minutes[minute_key] = {
            "int_values": int_values,
            "float_values": float_values,
        }
        for metric, value in int_values.items():
            summary_int[metric] += value
        for metric, value in float_values.items():
            summary_float[metric] += value

    latest_rows = (
        batch_df.filter(F.col("validation_status").isin("VALID", "WARNING"))
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            F.date_format("kafka_timestamp", "yyyy-MM-dd'T'HH:mm:ssXXX").alias(
                "kafka_timestamp"
            ),
            "orderID",
            "productName",
            "category",
            "region",
            "quantity",
            "totalAmount",
            "paymentStatus",
            "shippingStatus",
            "overall",
            "validation_status",
        )
        .orderBy(
            F.col("kafka_timestamp").desc(),
            F.col("kafka_partition").desc(),
            F.col("kafka_offset").desc(),
        )
        .limit(100)
        .toJSON()
        .collect()
    )

    return {
        "batch_id": int(batch_id),
        "input_rows": summary_int["events_total"],
        "minute_buckets": len(minutes),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "summary_int": summary_int,
        "summary_float": summary_float,
        "minutes": minutes,
        # LPUSH oldest-to-newest so index 0 remains the newest event.
        "latest_events": list(reversed(latest_rows)),
    }


def apply_batch_to_redis(
    batch_df: DataFrame,
    batch_id: int,
    host: str,
    port: int,
    password: str,
    namespace: str,
    ttl_seconds: int,
    latest_limit: int,
) -> None:
    payload = build_payload(batch_df, batch_id, namespace)
    if payload["input_rows"] == 0:
        print(f"REDIS_BATCH_EMPTY batch_id={batch_id}", flush=True)
        return

    marker_key = f"{namespace}:batch:{batch_id}"
    payload_json = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    )

    try:
        with socket.create_connection((host, port), timeout=10) as connection:
            connection.settimeout(30)
            with connection.makefile("rb") as stream:
                if execute(stream, connection, "AUTH", password) != "OK":
                    raise RuntimeError("Redis AUTH did not return OK")
                applied = execute(
                    stream,
                    connection,
                    "EVAL",
                    REDIS_APPLY_BATCH_SCRIPT,
                    4,
                    marker_key,
                    f"{namespace}:summary",
                    f"{namespace}:latest",
                    f"{namespace}:health",
                    payload_json,
                    ttl_seconds,
                    latest_limit,
                )
                if applied not in (0, 1):
                    raise RuntimeError(f"Unexpected Redis apply result: {applied}")
    except Exception as exc:
        print(
            "REDIS_BATCH_FAILED "
            f"batch_id={batch_id} input_rows={payload['input_rows']} "
            f"error_type={type(exc).__name__}",
            flush=True,
        )
        raise

    print(
        "REDIS_BATCH_COMMITTED "
        f"batch_id={batch_id} input_rows={payload['input_rows']} "
        f"minute_buckets={payload['minute_buckets']} applied={applied}",
        flush=True,
    )


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    checkpoint_bucket = os.getenv("MINIO_CHECKPOINT_BUCKET", "checkpoints")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.environ["REDIS_PASSWORD"]
    namespace = os.getenv("REDIS_KEY_PREFIX", "ecom:rt:v1")
    ttl_seconds = int(os.getenv("REDIS_METRIC_TTL_SECONDS", "172800"))
    latest_limit = int(os.getenv("REDIS_LATEST_LIMIT", "100"))
    trigger_interval = os.getenv("REDIS_STREAM_TRIGGER_INTERVAL", "10 seconds")
    max_offsets_per_trigger = int(
        os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "10000")
    )

    spark = (
        SparkSession.builder.appName("ecommerce-kafka-to-redis-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.internal.io.cloud.AbortableStreamBasedCheckpointFileManager",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio_s3a(spark)

    source = read_kafka_stream(
        spark,
        bootstrap_servers,
        topic,
        max_offsets_per_trigger,
    )
    validated = validate_events(source)
    checkpoint_path = f"s3a://{checkpoint_bucket}/kafka_to_redis_v1"

    query = None
    try:
        query = (
            validated.writeStream.queryName("ecommerce_redis_metrics_v1")
            .foreachBatch(
                lambda df, batch_id: apply_batch_to_redis(
                    df,
                    batch_id,
                    redis_host,
                    redis_port,
                    redis_password,
                    namespace,
                    ttl_seconds,
                    latest_limit,
                )
            )
            .option("checkpointLocation", checkpoint_path)
            .trigger(processingTime=trigger_interval)
            .start()
        )
        print(
            "KAFKA_REDIS_STREAMING_STARTED "
            f"application_id={spark.sparkContext.applicationId} "
            f"query_id={query.id} run_id={query.runId} "
            f"topic={topic} namespace={namespace} "
            f"trigger='{trigger_interval}'",
            flush=True,
        )
        query.awaitTermination()
        if query.exception() is not None:
            raise RuntimeError(f"Redis streaming query failed: {query.exception()}")
        raise RuntimeError("Redis streaming query terminated unexpectedly")
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
