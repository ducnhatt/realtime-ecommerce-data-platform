"""Read the files recorded by a committed Spark Parquet micro-batch."""

import json
from datetime import datetime, timezone



def parse_manifest_lines(lines):
    """Return added Parquet paths from Spark's v1 metadata log."""
    return [
        entry["path"]
        for line in lines
        if line and not line.startswith("v")
        for entry in [json.loads(line)]
        if entry.get("action") == "add"
    ]


def numeric_batch_ids(names):
    """Ignore checksum and metadata sidecars in a Spark file-sink log."""
    return {int(name) for name in names if name.isdecimal()}


def parse_checkpoint_offsets(lines):
    """Read the single Kafka source offset map from a Spark v1 offset log."""
    if not lines or lines[0] != "v1":
        raise ValueError("missing or unsupported Spark checkpoint offset log")
    for line in reversed(lines[1:]):
        value = json.loads(line)
        if not isinstance(value, dict) or "batchWatermarkMs" in value:
            continue
        if len(value) != 1:
            raise ValueError("expected one Kafka topic in checkpoint offset log")
        topic, partitions = next(iter(value.items()))
        if not isinstance(partitions, dict):
            raise ValueError("Kafka partition offsets must be a mapping")
        return topic, {int(partition): int(offset) for partition, offset in partitions.items()}
    raise ValueError("Kafka source offsets missing from checkpoint log")


def offset_ranges(start, end):
    """Return half-open Kafka ranges and fail closed on changed partitions."""
    start_topic, start_offsets = start
    end_topic, end_offsets = end
    if start_topic != end_topic or set(start_offsets) != set(end_offsets):
        raise ValueError("Kafka topics or partitions changed across batch offsets")
    if any(end_offsets[p] < start_offsets[p] for p in start_offsets):
        raise ValueError("Kafka offset moved backwards")
    return start_topic, {
        partition: (start_offsets[partition], end_offsets[partition])
        for partition in start_offsets
    }


def _read_hadoop_lines(spark, uri):
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return None
    stream = fs.open(path)
    output = spark._jvm.java.io.ByteArrayOutputStream()
    try:
        spark._jvm.org.apache.hadoop.io.IOUtils.copyBytes(stream, output, 4096, False)
        return output.toString("UTF-8").splitlines()
    finally:
        stream.close()
        output.close()


def _manifest_uri(spark, dataset_path, batch_id):
    for suffix, kind in ((str(batch_id), "ordinary"), (f"{batch_id}.compact", "compact")):
        uri = f"{dataset_path}/_spark_metadata/{suffix}"
        path = spark._jvm.org.apache.hadoop.fs.Path(uri)
        fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        if fs.exists(path):
            return uri, kind
    return None, None


def committed_batch_ids(spark, dataset_path):
    """List only committed batch IDs; an absent output has no commits."""
    path = spark._jvm.org.apache.hadoop.fs.Path(
        f"{dataset_path}/_spark_metadata"
    )
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        return set()
    return numeric_batch_ids(
        status.getPath().getName() for status in fs.listStatus(path)
    )


def committed_file_paths(spark, dataset_path, batch_id):
    """Return None until the batch manifest exists; [] means an empty commit."""
    return parse_manifest_lines(lines) if (
        lines := _read_hadoop_lines(spark, f"{dataset_path}/_spark_metadata/{batch_id}")
    ) is not None else None


def committed_batch_status_counts(spark, dataset_path, checkpoint_path, batch_id):
    """Count one committed batch, including cumulative ``.compact`` logs.

    A compact manifest lists files from older batches, so its file list must
    never be counted directly as the current batch. Use checkpoint Kafka
    offset boundaries against committed Parquet rows instead.
    Returns ``None`` until the sink manifest is visible.
    """
    _, kind = _manifest_uri(spark, dataset_path, batch_id)
    if kind is None:
        return None
    if kind == "ordinary":
        paths = committed_file_paths(spark, dataset_path, batch_id)
        return committed_status_counts(spark, dataset_path, paths), None
    from pyspark.sql import functions as F
    if batch_id < 1:
        raise ValueError("compaction batch must have a prior checkpoint offset")
    previous_lines = _read_hadoop_lines(spark, f"{checkpoint_path}/offsets/{batch_id - 1}")
    current_lines = _read_hadoop_lines(spark, f"{checkpoint_path}/offsets/{batch_id}")
    if previous_lines is None or current_lines is None:
        raise RuntimeError("compaction batch checkpoint offset boundary is unavailable")
    topic, ranges = offset_ranges(
        parse_checkpoint_offsets(previous_lines), parse_checkpoint_offsets(current_lines)
    )
    expected_input = sum(end - start for start, end in ranges.values())
    predicate = None
    for partition, (start, end) in ranges.items():
        if start == end:
            continue
        branch = (
            (F.col("kafka_topic") == topic)
            & (F.col("kafka_partition") == partition)
            & (F.col("kafka_offset") >= start)
            & (F.col("kafka_offset") < end)
        )
        predicate = branch if predicate is None else predicate | branch
    if predicate is None:
        return {}, expected_input
    rows = (
        spark.read.parquet(dataset_path)
        .filter(predicate)
        .groupBy("validation_status")
        .count()
        .collect()
    )
    return {row["validation_status"]: row["count"] for row in rows}, expected_input


def committed_status_counts(spark, dataset_path, file_paths):
    """Count only the explicit files in this manifest, never the whole dataset."""
    if not file_paths:
        return {}
    rows = (
        spark.read.option("basePath", dataset_path)
        .parquet(*file_paths)
        .groupBy("validation_status")
        .count()
        .collect()
    )
    return {row["validation_status"]: row["count"] for row in rows}


def committed_manifest_time(spark, dataset_path, batch_id):
    """Use sink metadata modification time as the physical commit timestamp."""
    uri, _ = _manifest_uri(spark, dataset_path, batch_id)
    if uri is None:
        raise FileNotFoundError(f"no committed manifest for batch {batch_id}")
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    modified_ms = fs.getFileStatus(path).getModificationTime()
    return datetime.fromtimestamp(
        modified_ms / 1000, tz=timezone.utc
    ).isoformat()
