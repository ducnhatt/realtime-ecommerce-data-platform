"""Smoke check: verify atomic Redis batch idempotency from Spark."""

import os
import socket
import uuid

from pyspark.sql import SparkSession


IDEMPOTENT_INCREMENT_SCRIPT = """
if redis.call('EXISTS', KEYS[1]) == 1 then
    return 0
end
redis.call('INCRBY', KEYS[2], ARGV[1])
redis.call('SET', KEYS[1], 'applied', 'EX', ARGV[2])
redis.call('EXPIRE', KEYS[2], ARGV[2])
return 1
"""


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


def verify_idempotency(host: str, port: int, password: str, suffix: str):
    marker_key = f"ecom:rt:v1:_test:batch:{suffix}"
    counter_key = f"ecom:rt:v1:_test:counter:{suffix}"
    increment = 7
    ttl_seconds = 60

    with socket.create_connection((host, port), timeout=5) as connection:
        connection.settimeout(5)
        with connection.makefile("rb") as stream:
            if execute(stream, connection, "AUTH", password) != "OK":
                raise RuntimeError("Redis AUTH did not return OK")

            first_apply = execute(
                stream,
                connection,
                "EVAL",
                IDEMPOTENT_INCREMENT_SCRIPT,
                2,
                marker_key,
                counter_key,
                increment,
                ttl_seconds,
            )
            replay_apply = execute(
                stream,
                connection,
                "EVAL",
                IDEMPOTENT_INCREMENT_SCRIPT,
                2,
                marker_key,
                counter_key,
                increment,
                ttl_seconds,
            )
            counter_value = execute(stream, connection, "GET", counter_key)

            if first_apply != 1:
                raise RuntimeError(f"First batch was not applied: {first_apply}")
            if replay_apply != 0:
                raise RuntimeError(f"Replay was not skipped: {replay_apply}")
            if counter_value != str(increment):
                raise RuntimeError(
                    f"Counter was applied more than once: {counter_value}"
                )

            deleted = execute(
                stream,
                connection,
                "DEL",
                marker_key,
                counter_key,
            )
            if deleted != 2:
                raise RuntimeError(f"Redis cleanup failed: deleted={deleted}")

    return first_apply, replay_apply, int(counter_value), deleted


def main() -> None:
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.environ["REDIS_PASSWORD"]
    suffix = str(uuid.uuid4())

    spark = SparkSession.builder.appName(
        "ecommerce-redis-lua-idempotency-test"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        result = spark.sparkContext.parallelize([0], 1).mapPartitions(
            lambda _: [verify_idempotency(host, port, password, suffix)]
        ).collect()
        if len(result) != 1:
            raise RuntimeError(f"Unexpected executor result count: {len(result)}")

        first_apply, replay_apply, counter_value, deleted = result[0]
        print(
            "REDIS_LUA_IDEMPOTENCY_OK "
            f"application_id={spark.sparkContext.applicationId} "
            f"first_apply={first_apply} replay_apply={replay_apply} "
            f"counter={counter_value} cleanup={deleted == 2}"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
