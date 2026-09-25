"""Shared object-storage configuration for local Spark jobs."""

import os

from pyspark.sql import SparkSession


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable is missing: {name}")
    return value


def configure_minio_s3a(spark: SparkSession) -> None:
    """Configure Hadoop S3A for the local MinIO-compatible endpoint."""

    endpoint = _required_env("MINIO_ENDPOINT")
    access_key = _required_env("MINIO_ACCESS_KEY")
    secret_key = _required_env("MINIO_SECRET_KEY")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    settings = {
        "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.endpoint": endpoint,
        "fs.s3a.endpoint.region": "us-east-1",
        "fs.s3a.access.key": access_key,
        "fs.s3a.secret.key": secret_key,
        "fs.s3a.aws.credentials.provider": (
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        ),
        "fs.s3a.path.style.access": "true",
        "fs.s3a.connection.ssl.enabled": "false",
        "fs.s3a.change.detection.mode": "none",
    }

    for key, value in settings.items():
        hadoop_conf.set(key, value)
