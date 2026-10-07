"""Contract for reading Spark Parquet commit manifests, including empty commits."""

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "spark"))

from pipeline.bronze_manifest import (  # noqa: E402
    numeric_batch_ids,
    offset_ranges,
    parse_checkpoint_offsets,
    parse_manifest_lines,
)


def main():
    assert parse_manifest_lines(["v1"]) == []
    assert parse_manifest_lines(
        [
            "v1",
            '{"path":"s3a://bronze/reviews/part-0.parquet","action":"add"}',
            '{"path":"s3a://bronze/reviews/old.parquet","action":"delete"}',
        ]
    ) == ["s3a://bronze/reviews/part-0.parquet"]
    assert numeric_batch_ids(["0", "1", ".1.crc", "2.compact"]) == {0, 1}
    previous = parse_checkpoint_offsets([
        "v1", '{"batchWatermarkMs":0}',
        '{"ecommerce_reviews":{"0":17,"1":15,"2":19}}',
    ])
    current = parse_checkpoint_offsets([
        "v1", '{"batchWatermarkMs":0}',
        '{"ecommerce_reviews":{"0":17,"1":16,"2":20}}',
    ])
    topic, ranges = offset_ranges(previous, current)
    assert topic == "ecommerce_reviews"
    assert ranges == {0: (17, 17), 1: (15, 16), 2: (19, 20)}
    assert sum(end - start for start, end in ranges.values()) == 2
    try:
        offset_ranges(current, previous)
    except ValueError as error:
        assert "backwards" in str(error)
    else:
        raise AssertionError("offset regression was accepted")
    print("BRONZE_MANIFEST_MONITORING_CONTRACT_OK cases=5")


if __name__ == "__main__":
    main()
