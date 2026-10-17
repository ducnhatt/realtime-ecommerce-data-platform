"""Focused contract for five alert rules and OPEN/RESOLVED transitions."""

import sys
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.collector_alerts import evaluate_alerts, transition_alerts  # noqa: E402
from src.monitoring.jsonl_writer import build_monitoring_event  # noqa: E402


def moment(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def event(component: str, metric: str, values: dict, time: str, *, batch_id: int | None = None) -> dict:
    dimensions = {"topic": "ecommerce_reviews"}
    if batch_id is not None:
        dimensions["batch_id"] = batch_id
    return build_monitoring_event(
        component=component, metric_name=metric, metric_semantics="delta",
        status="OK", values=values, dimensions=dimensions, run_id="test-run",
        event_id=str(uuid.uuid4()), event_time=time, observed_at=time,
    )


class AlertContractTest(unittest.TestCase):
    def test_delivery_gap_is_critical(self) -> None:
        producer = event("producer", "delivery_counts",
                         {"attempted": 20, "acknowledged": 19, "failed": 1},
                         "2026-09-28T10:00:00Z")
        active = evaluate_alerts([producer], moment("2026-09-28T10:00:10Z"))
        self.assertEqual({item["alert_name"] for item in active.values()}, {"producer_delivery_gap"})
        self.assertEqual(next(iter(active.values()))["severity"], "CRITICAL")

    def test_freshness_warning_critical_then_resolved_without_storm(self) -> None:
        producer = event("producer", "delivery_counts",
                         {"attempted": 20, "acknowledged": 20, "failed": 0},
                         "2026-09-28T10:00:00Z")
        self.assertEqual(evaluate_alerts([producer], moment("2026-09-28T10:01:15Z")), {})
        warning = evaluate_alerts([producer], moment("2026-09-28T10:01:16Z"))
        opened, transitions = transition_alerts({}, warning, at=moment("2026-09-28T10:01:16Z"),
                                                 source_identity="producer-offset")
        self.assertEqual([(item["transition"], item["severity"]) for item in transitions],
                         [("OPEN", "WARNING")])
        _, repeated = transition_alerts(opened, warning, at=moment("2026-09-28T10:02:00Z"),
                                        source_identity="producer-offset")
        self.assertEqual(repeated, [])
        critical = evaluate_alerts([producer], moment("2026-09-28T10:05:01Z"))
        opened, escalation = transition_alerts(opened, critical,
                                               at=moment("2026-09-28T10:05:01Z"),
                                               source_identity="producer-offset")
        self.assertEqual([(item["transition"], item["severity"]) for item in escalation],
                         [("RESOLVED", "WARNING"), ("OPEN", "CRITICAL")])
        committed = event("kafka_to_bronze_dlq", "last_committed",
                          {"committed_at_epoch_ms": int(moment("2026-09-28T10:05:10Z").timestamp() * 1000)},
                          "2026-09-28T10:05:10Z")
        healthy = evaluate_alerts([producer, committed], moment("2026-09-28T10:05:20Z"))
        _, resolved = transition_alerts(opened, healthy,
                                        at=moment("2026-09-28T10:05:20Z"),
                                        source_identity="bronze-offset")
        self.assertEqual([(item["transition"], item["severity"]) for item in resolved],
                         [("RESOLVED", "CRITICAL")])

    def test_three_slow_nonempty_bronze_batches(self) -> None:
        records = []
        for batch in range(3):
            time = f"2026-09-28T10:0{batch}:00Z"
            records.append(event("kafka_to_bronze_dlq", "quality_counts",
                                 {"batch_input_rows": 20, "valid_count": 20,
                                  "warning_count": 0, "invalid_count": 0}, time, batch_id=batch))
            records.append(event("kafka_to_bronze_dlq", "batch_duration",
                                 {"duration_ms": 26000}, time, batch_id=batch))
        active = evaluate_alerts(records, moment("2026-09-28T10:03:00Z"))
        self.assertEqual(next(iter(active.values()))["alert_name"], "bronze_batch_duration")
        self.assertEqual(next(iter(active.values()))["severity"], "WARNING")

    def test_invalid_rate_requires_100_rows(self) -> None:
        records = [event("kafka_to_bronze_dlq", "quality_counts",
                         {"batch_input_rows": 20, "valid_count": 14,
                          "warning_count": 0, "invalid_count": 6},
                         f"2026-09-28T10:0{batch}:00Z", batch_id=batch)
                   for batch in range(5)]
        self.assertEqual(evaluate_alerts(records[:4], moment("2026-09-28T10:05:00Z")), {})
        active = evaluate_alerts(records, moment("2026-09-28T10:05:00Z"))
        self.assertEqual(next(iter(active.values()))["alert_name"], "bronze_invalid_rate")
        self.assertEqual(next(iter(active.values()))["severity"], "WARNING")

    def test_silver_mismatch_critical_then_latest_run_resolves(self) -> None:
        mismatch = event("bronze_to_silver_incremental", "run_counts",
                         {"input_rows": 18, "output_rows": 17, "non_empty_batches": 1},
                         "2026-09-28T10:00:00Z")
        healthy = event("bronze_to_silver_incremental", "run_counts",
                        {"input_rows": 2, "output_rows": 2, "non_empty_batches": 1},
                        "2026-09-28T10:10:00Z")
        active = evaluate_alerts([mismatch], moment("2026-09-28T10:01:00Z"))
        self.assertEqual(next(iter(active.values()))["alert_name"], "bronze_silver_row_mismatch")
        self.assertEqual(next(iter(active.values()))["severity"], "CRITICAL")
        self.assertEqual(evaluate_alerts([mismatch, healthy], moment("2026-09-28T10:11:00Z")), {})


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(AlertContractTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(f"MONITORING_ALERT_CONTRACT_OK tests={result.testsRun}")
