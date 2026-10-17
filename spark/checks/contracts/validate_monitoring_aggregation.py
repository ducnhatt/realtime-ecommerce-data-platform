"""Contract tests for pure event-time aggregation, with no runtime writes."""

import sys
import unittest
import uuid
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.collector_aggregation import aggregate_windows  # noqa: E402
from src.monitoring.jsonl_writer import build_monitoring_event  # noqa: E402


def event(metric: str, semantics: str, values: dict, time: str,
          *, component: str = "producer", status: str = "OK") -> dict:
    return build_monitoring_event(
        event_id=str(uuid.uuid4()), component=component, metric_name=metric,
        metric_semantics=semantics, status=status, values=values,
        dimensions={"topic": "ecommerce_reviews"}, run_id="test-run",
        event_time=time, observed_at=time,
    )


class AggregationContractTest(unittest.TestCase):
    def test_delta_sum_and_idempotent_retry_ignored(self) -> None:
        first = event("delivery_counts", "delta", {"attempted": 20}, "2026-09-28T04:01:00Z")
        second = event("delivery_counts", "delta", {"attempted": 3}, "2026-09-28T04:02:00Z")
        retry = {**first, "observed_at": "2026-09-28T04:03:00.000Z"}
        history = aggregate_windows([first, retry, second])
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["values"], {"attempted": 23})
        self.assertEqual(history[0]["event_count"], 2)

    def test_window_boundary_and_late_input_are_event_time_based(self) -> None:
        later = event("delivery_counts", "delta", {"attempted": 2}, "2026-09-28T04:05:00Z")
        earlier = event("delivery_counts", "delta", {"attempted": 1}, "2026-09-28T04:04:59Z")
        history = aggregate_windows([later, earlier])
        self.assertEqual([item["window_start"] for item in history],
                         ["2026-09-28T04:00:00.000Z", "2026-09-28T04:05:00.000Z"])

    def test_gauge_last_and_duration_statistics(self) -> None:
        first = event("delivery_duration", "gauge", {"duration_ms": 100.0}, "2026-09-28T04:01:00Z")
        second = event("delivery_duration", "gauge", {"duration_ms": 300.0}, "2026-09-28T04:02:00Z")
        result = aggregate_windows([second, first])[0]
        self.assertEqual(result["values"], {"duration_ms": 300.0})
        self.assertEqual(result["statistics"], {"duration_ms_avg": 200.0, "duration_ms_max": 300.0})

    def test_progress_gauge_uses_max_not_sum(self) -> None:
        first = event("checkpoint_progress", "gauge", {"last_batch_id": 8},
                      "2026-09-28T04:01:00Z", component="bronze_to_silver_incremental")
        second = event("checkpoint_progress", "gauge", {"last_batch_id": 7},
                       "2026-09-28T04:02:00Z", component="bronze_to_silver_incremental")
        self.assertEqual(aggregate_windows([first, second])[0]["values"], {"last_batch_id": 8})

    def test_cumulative_takes_latest_snapshot_and_status_counts(self) -> None:
        first = event("snapshot", "cumulative", {"total": 100}, "2026-09-28T04:01:00Z")
        second = event("snapshot", "cumulative", {"total": 101}, "2026-09-28T04:02:00Z", status="WARNING")
        result = aggregate_windows([second, first])[0]
        self.assertEqual(result["values"], {"total": 101})
        self.assertEqual(result["status"], "WARNING")
        self.assertEqual(result["status_counts"], {"OK": 1, "WARNING": 1})

    def test_conflicting_duplicate_id_and_schema_drift_fail(self) -> None:
        first = event("delivery_counts", "delta", {"attempted": 1}, "2026-09-28T04:01:00Z")
        changed = {**first, "values": {"attempted": 2}}
        with self.assertRaisesRegex(ValueError, "conflicting payload"):
            aggregate_windows([first, changed])
        other_shape = event("delivery_counts", "delta", {"failed": 0}, "2026-09-28T04:02:00Z")
        with self.assertRaisesRegex(ValueError, "inconsistent semantics/values"):
            aggregate_windows([first, other_shape])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AggregationContractTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(f"MONITORING_AGGREGATION_CONTRACT_OK tests={result.testsRun}")
