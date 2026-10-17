"""Contract tests for bounded JSONL reads; no Docker services required."""

import json
import sys
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.collector_reader import (  # noqa: E402
    load_quarantine_reasons,
    read_complete_events,
)
from src.monitoring.jsonl_writer import build_monitoring_event  # noqa: E402


def sample_event() -> dict:
    return build_monitoring_event(
        component="producer",
        metric_name="delivery_counts",
        metric_semantics="delta",
        status="OK",
        values={"attempted": 2},
        run_id="test-run",
        event_time="2026-09-28T04:00:00Z",
        observed_at="2026-09-28T04:00:01Z",
    )


class CollectorReaderContractTest(unittest.TestCase):
    def test_real_quarantine_registry_selects_three_bronze_events(self) -> None:
        registry = PROJECT_ROOT / "src/monitoring/known_invalid_events_v1.json"
        reasons = load_quarantine_reasons(registry)
        self.assertEqual(set(reasons), {"spark_bronze_monitor.jsonl"})
        self.assertEqual(len(reasons["spark_bronze_monitor.jsonl"]), 3)

    def test_missing_source_and_resume_without_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.jsonl"
            self.assertEqual(read_complete_events(path).next_offset, 0)
            path.write_text(json.dumps(sample_event()) + "\n", encoding="utf-8")
            first = read_complete_events(path)
            self.assertEqual(len(first.accepted), 1)
            self.assertEqual(len(read_complete_events(path, offset=first.next_offset).accepted), 0)

    def test_partial_tail_is_not_consumed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.jsonl"
            line = json.dumps(sample_event())
            path.write_text(line + "\n" + line[:30], encoding="utf-8")
            first = read_complete_events(path)
            self.assertEqual(len(first.accepted), 1)
            self.assertTrue(first.incomplete_tail)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(line[30:] + "\n")
            second = read_complete_events(path, offset=first.next_offset)
            self.assertEqual(len(second.accepted), 1)
            self.assertFalse(second.incomplete_tail)

    def test_invalid_and_quarantined_are_audited_not_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.jsonl"
            event = sample_event()
            path.write_text(
                "not-json\n" + json.dumps(event) + "\n", encoding="utf-8"
            )
            result = read_complete_events(path, quarantine_reasons={event["event_id"]: "known bad"})
            self.assertEqual(len(result.accepted), 0)
            self.assertEqual(len(result.rejected), 2)
            self.assertTrue(result.rejected[0].reason.startswith("invalid_event:"))
            self.assertEqual(result.rejected[1].event_id, event["event_id"])
            self.assertEqual(result.rejected[1].reason, "quarantined:known bad")

    def test_missing_schema_field_and_truncated_source_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.jsonl"
            event = sample_event()
            del event["observed_at"]
            path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            result = read_complete_events(path)
            self.assertEqual(len(result.accepted), 0)
            self.assertEqual(len(result.rejected), 1)
            with self.assertRaises(RuntimeError):
                read_complete_events(path, offset=result.next_offset + 1)


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CollectorReaderContractTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(f"MONITORING_COLLECTOR_READER_CONTRACT_OK tests={result.testsRun}")
