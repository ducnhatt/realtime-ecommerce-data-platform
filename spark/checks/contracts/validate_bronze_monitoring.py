"""Validate the latest non-empty Bronze monitoring batch."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


EXPECTED = {
    "quality_counts": "delta",
    "batch_duration": "gauge",
    "last_committed": "gauge",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        default="monitoring/spark_bronze_monitor.jsonl",
    )
    parser.add_argument("--expected-input-rows", type=int, default=None)
    args = parser.parse_args()

    events = [
        json.loads(line)
        for line in Path(args.path).read_text(encoding="utf-8").splitlines()
    ]
    events = [
        event
        for event in events
        if event.get("component") == "kafka_to_bronze_dlq"
    ]
    quality_events = [
        event
        for event in events
        if event.get("metric_name") == "quality_counts"
        and event.get("values", {}).get("batch_input_rows", 0) > 0
    ]
    if not quality_events:
        raise RuntimeError("No non-empty Bronze monitoring batch found")

    quality_event = quality_events[-1]
    run_id = quality_event["run_id"]
    batch_id = quality_event["dimensions"]["batch_id"]
    batch_events = [
        event
        for event in events
        if event.get("run_id") == run_id
        and event.get("dimensions", {}).get("batch_id") == batch_id
    ]
    by_name = {event["metric_name"]: event for event in batch_events}
    if len(batch_events) != 3 or set(by_name) != set(EXPECTED):
        raise RuntimeError("Expected exactly three events for the latest non-empty batch")
    for name, semantics in EXPECTED.items():
        if by_name[name].get("metric_semantics") != semantics:
            raise RuntimeError(f"{name}: expected semantics={semantics}")
        if by_name[name].get("status") != "OK":
            raise RuntimeError(f"{name}: expected status=OK")

    values = quality_event["values"]
    input_rows = values["batch_input_rows"]
    valid = values["valid_count"]
    warning = values["warning_count"]
    invalid = values["invalid_count"]
    if input_rows != valid + warning + invalid:
        raise RuntimeError("Bronze quality-count invariant failed")
    if args.expected_input_rows is not None and input_rows != args.expected_input_rows:
        raise RuntimeError(
            f"Expected input_rows={args.expected_input_rows}, actual={input_rows}"
        )

    duration_ms = by_name["batch_duration"]["values"]["duration_ms"]
    print(
        "BRONZE_MONITORING_CONTRACT_OK "
        f"run_id={run_id} batch_id={batch_id} input_rows={input_rows} "
        f"valid={valid} warning={warning} invalid={invalid} "
        f"duration_ms={duration_ms} events=3"
    )


if __name__ == "__main__":
    main()
