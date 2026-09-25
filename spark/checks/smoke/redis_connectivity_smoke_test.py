"""Smoke check: verify authenticated Spark executor access to Redis."""

import os
import socket
import uuid

from pyspark.sql import SparkSession


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


def redis_round_trip(host: str, port: int, password: str, key: str, expected: str):
    with socket.create_connection((host, port), timeout=5) as connection:
        connection.settimeout(5)
        with connection.makefile("rb") as stream:
            if execute(stream, connection, "AUTH", password) != "OK":
                raise RuntimeError("Redis AUTH did not return OK")
            if execute(stream, connection, "PING") != "PONG":
                raise RuntimeError("Redis PING did not return PONG")
            if execute(stream, connection, "SET", key, expected, "EX", 60) != "OK":
                raise RuntimeError("Redis SET did not return OK")

            actual = execute(stream, connection, "GET", key)
            if actual != expected:
                raise RuntimeError(
                    f"Redis round-trip mismatch: expected={expected!r}, actual={actual!r}"
                )

            deleted = execute(stream, connection, "DEL", key)
            if deleted != 1:
                raise RuntimeError(f"Redis cleanup failed: deleted={deleted}")

    return host, port, deleted


def main() -> None:
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.environ["REDIS_PASSWORD"]
    key = f"ecom:rt:v1:_connectivity_test:{uuid.uuid4()}"
    expected = "spark-executor-round-trip"

    spark = SparkSession.builder.appName(
        "ecommerce-spark-redis-connectivity-smoke-test"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = spark.sparkContext.parallelize([0], 1).mapPartitions(
            lambda _: [redis_round_trip(host, port, password, key, expected)]
        ).collect()
        if result != [(host, port, 1)]:
            raise RuntimeError(f"Unexpected executor result: {result!r}")

        print(
            "SPARK_REDIS_CONNECTIVITY_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"endpoint={host}:{port} executor_tasks=1 cleanup=true"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
