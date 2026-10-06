"""Validate the latest producer run in raw_data_monitor.jsonl."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


EXPECTED = {
    "delivery_counts": "delta",
    "delivery_outstanding": "gauge",
    "delivery_duration": "gauge",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        default="monitoring/raw_data_monitor.jsonl",
    )
    parser.add_argument("--expected-attempted", type=int, default=None)
    args = parser.parse_args()

    path = Path(args.path)
    events = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    producer_events = [event for event in events if event.get("component") == "producer"]
    if not producer_events:
        raise RuntimeError("No producer monitoring events found")

    latest_run_id = producer_events[-1].get("run_id")
    latest = [event for event in producer_events if event.get("run_id") == latest_run_id]
    by_name = {event["metric_name"]: event for event in latest}
    if set(by_name) != set(EXPECTED) or len(latest) != len(EXPECTED):
        raise RuntimeError(
            f"Expected exactly {sorted(EXPECTED)}, got "
            f"{sorted(event.get('metric_name') for event in latest)}"
        )

    for name, semantics in EXPECTED.items():
        event = by_name[name]
        if event.get("schema_version") != "monitoring-event-v1":
            raise RuntimeError(f"{name}: invalid schema_version")
        if event.get("metric_semantics") != semantics:
            raise RuntimeError(f"{name}: expected semantics={semantics}")

    counts = by_name["delivery_counts"]["values"]
    outstanding = by_name["delivery_outstanding"]["values"]["outstanding"]
    attempted = counts["attempted"]
    acknowledged = counts["acknowledged"]
    failed = counts["failed"]
    if attempted != acknowledged + failed + outstanding:
        raise RuntimeError("Producer delivery invariant failed")
    if args.expected_attempted is not None and attempted != args.expected_attempted:
        raise RuntimeError(
            f"Expected attempted={args.expected_attempted}, actual={attempted}"
        )
    if any(event.get("status") != "OK" for event in latest):
        raise RuntimeError("Latest producer monitoring run is not OK")

    duration_ms = by_name["delivery_duration"]["values"]["duration_ms"]
    print(
        "PRODUCER_MONITORING_CONTRACT_OK "
        f"run_id={latest_run_id} events=3 attempted={attempted} "
        f"acknowledged={acknowledged} failed={failed} "
        f"outstanding={outstanding} duration_ms={duration_ms}"
    )


if __name__ == "__main__":
    main()
