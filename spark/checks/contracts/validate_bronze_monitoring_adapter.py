"""Contract test for the Bronze/DLQ monitoring adapter."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.monitoring_bronze import MonitoringBronze  # noqa: E402


def main() -> None:
    with tempfile.TemporaryDirectory() as temp_dir:
        target = Path(temp_dir) / "spark_bronze_monitor.jsonl"
        monitor = MonitoringBronze(
            target,
            topic="ecommerce_reviews",
            application_id="app-contract-test",
        )
        written = monitor.log_committed_batch(
            batch_id=7,
            batch_input_rows=20,
            valid_count=4,
            warning_count=13,
            invalid_count=3,
            duration_ms=25000,
            committed_at="2026-09-27T15:10:00.000Z",
        )
        events = [
            json.loads(line)
            for line in target.read_text(encoding="utf-8").splitlines()
        ]
        if written != 3 or len(events) != 3:
            raise RuntimeError("Bronze adapter must append exactly three events")
        expected = {
            "quality_counts": "delta",
            "batch_duration": "gauge",
            "last_committed": "gauge",
        }
        actual = {
            event["metric_name"]: event["metric_semantics"]
            for event in events
        }
        if actual != expected:
            raise RuntimeError(f"Unexpected Bronze monitoring contract: {actual}")
        if any(event["status"] != "OK" for event in events):
            raise RuntimeError("Expected OK status for balanced quality counts")
        if len({event["event_id"] for event in events}) != 3:
            raise RuntimeError("Expected deterministic unique IDs per metric")

    print(
        "BRONZE_MONITORING_ADAPTER_CONTRACT_OK "
        "events=3 input_rows=20 valid=4 warning=13 invalid=3"
    )


if __name__ == "__main__":
    main()
