"""Host-side contract checks for the bounded Silver monitoring adapter."""

import json
import sys
import tempfile
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.monitoring_silver import MonitoringSilver  # noqa: E402


def main():
    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "silver_stream_monitor.jsonl"
        monitor = MonitoringSilver(path, "ecommerce_reviews")
        assert monitor.log_run(
            run_id="run-non-empty",
            input_rows=20,
            output_rows=20,
            non_empty_batches=1,
            duration_ms=25000,
            last_batch_id=7,
        ) == 3
        assert monitor.log_run(
            run_id="run-empty",
            input_rows=0,
            output_rows=0,
            non_empty_batches=0,
            duration_ms=5000,
            last_batch_id=8,
        ) == 3
        assert monitor.log_run(
            run_id="run-mismatch",
            input_rows=20,
            output_rows=19,
            non_empty_batches=1,
            duration_ms=25000,
            last_batch_id=9,
        ) == 3
        assert monitor.log_failure(run_id="run-failed", last_batch_id=10)
        events = [json.loads(line) for line in path.read_text().splitlines()]
        assert len(events) == 10
        expected_names = {"run_counts", "run_performance", "checkpoint_progress"}
        for run_id, expected_status in (
            ("run-non-empty", "OK"),
            ("run-empty", "OK"),
            ("run-mismatch", "WARNING"),
        ):
            run_events = [event for event in events if event["run_id"] == run_id]
            assert {event["metric_name"] for event in run_events} == expected_names
            assert {event["status"] for event in run_events} == {expected_status}
            assert len({event["event_id"] for event in run_events}) == 3
        failed = [event for event in events if event["run_id"] == "run-failed"]
        assert len(failed) == 1
        assert failed[0]["status"] == "FAILED"
        assert failed[0]["metric_name"] == "run_failure"
        assert failed[0]["values"] == {"failure_count": 1}
    print("SILVER_MONITORING_ADAPTER_CONTRACT_OK runs=4 events=10")


if __name__ == "__main__":
    main()
