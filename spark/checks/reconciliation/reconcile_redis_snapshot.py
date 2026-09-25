"""Reconciliation check: compare Kafka with the Redis read model."""

import json
import os
import socket

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.redis_metrics import aggregate_minute_metrics
from pipeline.validation import validate_events


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
FORBIDDEN_LATEST_FIELDS = {
    "raw_payload",
    "reviewerID",
    "reviewerName",
    "reviewText",
    "summary",
}


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
    if prefix == b"*":
        length = int(value)
        if length == -1:
            return None
        return [read_response(stream) for _ in range(length)]
    raise RuntimeError(f"Unsupported Redis response prefix: {prefix!r}")


def execute(stream, connection, *parts: object):
    connection.sendall(encode_command(*parts))
    return read_response(stream)


def pairs_to_dict(values: list[str]) -> dict[str, str]:
    if len(values) % 2:
        raise RuntimeError(f"Redis hash response has odd length: {len(values)}")
    return dict(zip(values[0::2], values[1::2]))


def read_kafka_snapshot(
    spark: SparkSession,
    bootstrap_servers: str,
    topic: str,
) -> DataFrame:
    return (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("failOnDataLoss", "true")
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


def expected_metrics(validated: DataFrame, namespace: str):
    minute_rows = aggregate_minute_metrics(validated).collect()
    summary = {metric: 0 for metric in INTEGER_METRICS}
    summary.update({metric: 0.0 for metric in FLOAT_METRICS})
    minutes = {}

    for row in minute_rows:
        values = row.asDict()
        key = f"{namespace}:minute:{values['ingest_minute']}"
        metrics = {}
        for metric in INTEGER_METRICS:
            metrics[metric] = int(values[metric])
            summary[metric] += metrics[metric]
        for metric in FLOAT_METRICS:
            metrics[metric] = float(values[metric])
            summary[metric] += metrics[metric]
        minutes[key] = metrics

    return summary, minutes


def assert_numeric_hash(
    label: str,
    actual: dict[str, str],
    expected: dict[str, int | float],
    failures: list[str],
) -> None:
    for metric in INTEGER_METRICS:
        actual_value = actual.get(metric)
        if actual_value is None or int(actual_value) != int(expected[metric]):
            failures.append(
                f"{label}.{metric}: expected={expected[metric]}, actual={actual_value}"
            )
    for metric in FLOAT_METRICS:
        actual_value = actual.get(metric)
        if actual_value is None or abs(float(actual_value) - float(expected[metric])) > 0.001:
            failures.append(
                f"{label}.{metric}: expected={expected[metric]}, actual={actual_value}"
            )


def reconcile_redis(
    host: str,
    port: int,
    password: str,
    namespace: str,
    expected_summary: dict,
    expected_minutes: dict,
) -> dict[str, int]:
    failures = []
    with socket.create_connection((host, port), timeout=10) as connection:
        connection.settimeout(30)
        with connection.makefile("rb") as stream:
            if execute(stream, connection, "AUTH", password) != "OK":
                raise RuntimeError("Redis AUTH did not return OK")

            actual_summary = pairs_to_dict(
                execute(stream, connection, "HGETALL", f"{namespace}:summary")
            )
            assert_numeric_hash(
                "summary", actual_summary, expected_summary, failures
            )

            cursor = "0"
            actual_minute_keys = set()
            while True:
                scan = execute(
                    stream,
                    connection,
                    "SCAN",
                    cursor,
                    "MATCH",
                    f"{namespace}:minute:*",
                    "COUNT",
                    1000,
                )
                cursor = scan[0]
                actual_minute_keys.update(scan[1])
                if cursor == "0":
                    break

            if actual_minute_keys != set(expected_minutes):
                failures.append(
                    "minute key mismatch: "
                    f"expected={sorted(expected_minutes)}, "
                    f"actual={sorted(actual_minute_keys)}"
                )

            for key, expected in expected_minutes.items():
                actual = pairs_to_dict(
                    execute(stream, connection, "HGETALL", key)
                )
                assert_numeric_hash(key, actual, expected, failures)

            latest_length = execute(
                stream, connection, "LLEN", f"{namespace}:latest"
            )
            expected_latest_length = min(expected_summary["record_count"], 100)
            if latest_length != expected_latest_length:
                failures.append(
                    "latest length mismatch: "
                    f"expected={expected_latest_length}, actual={latest_length}"
                )

            latest_values = execute(
                stream, connection, "LRANGE", f"{namespace}:latest", 0, -1
            )
            for index, value in enumerate(latest_values):
                event = json.loads(value)
                forbidden = FORBIDDEN_LATEST_FIELDS.intersection(event)
                if forbidden:
                    failures.append(
                        f"latest[{index}] contains forbidden fields: {sorted(forbidden)}"
                    )

            health = pairs_to_dict(
                execute(stream, connection, "HGETALL", f"{namespace}:health")
            )
            if health.get("status") != "OK":
                failures.append(f"health.status is not OK: {health!r}")

            try:
                last_batch_id = int(health.get("last_batch_id", "-1"))
                last_input_rows = int(health.get("last_input_rows", "-1"))
            except (TypeError, ValueError):
                last_batch_id = -1
                last_input_rows = -1
                failures.append(
                    "health batch fields must be integers: "
                    f"last_batch_id={health.get('last_batch_id')!r}, "
                    f"last_input_rows={health.get('last_input_rows')!r}"
                )

            if last_batch_id < 0 or last_input_rows < 0:
                failures.append(f"health batch fields are invalid: {health!r}")
            elif execute(
                stream,
                connection,
                "EXISTS",
                f"{namespace}:batch:{last_batch_id}",
            ) != 1:
                failures.append(
                    "health points to a missing idempotency marker: "
                    f"batch_id={last_batch_id}"
                )

    if failures:
        raise RuntimeError("Kafka/Redis reconciliation failed:\n" + "\n".join(failures))

    return {
        "events_total": expected_summary["events_total"],
        "valid_count": expected_summary["valid_count"],
        "warning_count": expected_summary["warning_count"],
        "invalid_count": expected_summary["invalid_count"],
        "record_count": expected_summary["record_count"],
        "minute_buckets": len(expected_minutes),
        "latest_length": min(expected_summary["record_count"], 100),
        "last_batch_id": last_batch_id,
        "last_input_rows": last_input_rows,
    }


def main() -> None:
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    topic = os.getenv("KAFKA_TOPIC", "ecommerce_reviews")
    source_time_zone = os.getenv("SOURCE_TIME_ZONE", "Asia/Ho_Chi_Minh")
    redis_host = os.getenv("REDIS_HOST", "redis")
    redis_port = int(os.getenv("REDIS_PORT", "6379"))
    redis_password = os.environ["REDIS_PASSWORD"]
    namespace = os.getenv("REDIS_KEY_PREFIX", "ecom:rt:v1")

    spark = (
        SparkSession.builder.appName("ecommerce-kafka-redis-reconciliation-v1")
        .config("spark.sql.session.timeZone", source_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        source = read_kafka_snapshot(spark, bootstrap_servers, topic)
        validated = validate_events(source).cache()
        expected_summary, expected_minutes = expected_metrics(validated, namespace)
        metrics = reconcile_redis(
            redis_host,
            redis_port,
            redis_password,
            namespace,
            expected_summary,
            expected_minutes,
        )
        print(
            "KAFKA_REDIS_RECONCILIATION_OK "
            f"application_id={spark.sparkContext.applicationId} "
            + " ".join(f"{key}={value}" for key, value in metrics.items())
        )
        validated.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
