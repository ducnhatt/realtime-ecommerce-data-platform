"""Unit/contract gate for the observability v1 JSONL writer."""

from __future__ import annotations

import json
import logging
import math
import sys
import tempfile
import unittest
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.jsonl_writer import (  # noqa: E402
    MonitoringEventValidationError,
    build_monitoring_event,
    write_monitoring_event,
)


FIXED_TIME = datetime(2026, 9, 27, 10, 30, 45, 123000, tzinfo=timezone.utc)


class MonitoringWriterContractTest(unittest.TestCase):
    def test_builds_complete_v1_envelope(self) -> None:
        event_id = str(uuid.uuid4())
        event = build_monitoring_event(
            component="producer",
            metric_name="delivery_run_completed",
            metric_semantics="delta",
            status="OK",
            run_id="producer-run-1",
            event_id=event_id,
            event_time=FIXED_TIME,
            observed_at=FIXED_TIME,
            values={"attempted": 20, "duration_ms": 1250.5},
            dimensions={"topic": "ecommerce_reviews", "controlled": True},
        )

        self.assertEqual(event["event_id"], event_id)
        self.assertEqual(event["event_time"], "2026-09-27T10:30:45.123Z")
        self.assertEqual(event["observed_at"], "2026-09-27T10:30:45.123Z")
        self.assertEqual(event["schema_version"], "monitoring-event-v1")
        self.assertEqual(event["values"]["attempted"], 20)
        self.assertEqual(event["dimensions"]["topic"], "ecommerce_reviews")

    def test_appends_without_overwriting_and_each_line_parses(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = Path(temp_dir) / "raw_data_monitor.jsonl"
            for run_number in (1, 2):
                written = write_monitoring_event(
                    target,
                    component="producer",
                    metric_name="delivery_run_completed",
                    metric_semantics="delta",
                    status="OK",
                    run_id=f"producer-run-{run_number}",
                    values={"attempted": 20, "acknowledged": 20},
                    dimensions={"topic": "ecommerce_reviews"},
                )
                self.assertTrue(written)

            lines = target.read_text(encoding="utf-8").splitlines()
            self.assertEqual(len(lines), 2)
            events = [json.loads(line) for line in lines]
            self.assertEqual([event["run_id"] for event in events], [
                "producer-run-1",
                "producer-run-2",
            ])
            self.assertEqual(len({event["event_id"] for event in events}), 2)

    def test_rejects_invalid_semantics_status_and_values(self) -> None:
        common = {
            "component": "producer",
            "metric_name": "delivery_run_completed",
            "metric_semantics": "delta",
            "status": "OK",
            "values": {"attempted": 20},
        }

        with self.assertRaises(MonitoringEventValidationError):
            build_monitoring_event(**{**common, "metric_semantics": "sum"})
        with self.assertRaises(MonitoringEventValidationError):
            build_monitoring_event(**{**common, "status": "SUCCESS"})
        with self.assertRaises(MonitoringEventValidationError):
            build_monitoring_event(**{**common, "values": {"attempted": True}})
        with self.assertRaises(MonitoringEventValidationError):
            build_monitoring_event(
                **{**common, "values": {"duration_ms": math.inf}}
            )

    def test_rejects_naive_timestamp(self) -> None:
        with self.assertRaises(MonitoringEventValidationError):
            build_monitoring_event(
                component="producer",
                metric_name="delivery_run_completed",
                metric_semantics="delta",
                status="OK",
                values={"attempted": 1},
                event_time=datetime(2026, 9, 27, 10, 30, 45),
            )

    def test_io_failure_returns_false_and_caller_continues(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            logger = logging.getLogger("monitoring-writer-contract-test")
            with self.assertLogs(logger, level="WARNING") as captured:
                written = write_monitoring_event(
                    temp_dir,
                    component="producer",
                    metric_name="delivery_run_completed",
                    metric_semantics="delta",
                    status="OK",
                    values={"attempted": 1},
                    logger=logger,
                )

            caller_continued = True
            self.assertFalse(written)
            self.assertTrue(caller_continued)
            self.assertIn("MONITORING_WRITE_FAILED", captured.output[0])


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(
        MonitoringWriterContractTest
    )
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(
        "MONITORING_JSONL_WRITER_CONTRACT_OK "
        f"tests={result.testsRun} schema_version=monitoring-event-v1"
    )
