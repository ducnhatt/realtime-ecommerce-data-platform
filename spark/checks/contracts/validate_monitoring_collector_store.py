"""Contract tests for collector output/cursor ordering and crash retry."""

import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from datetime import datetime, timezone


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.monitoring.collector_store import collect_once  # noqa: E402
from src.monitoring.jsonl_writer import build_monitoring_event  # noqa: E402


def event(attempted: int, *, metric: str = "delivery_counts",
          at: str = "2026-09-28T04:01:00Z") -> dict:
    return build_monitoring_event(
        event_id=str(uuid.uuid4()), component="producer", metric_name=metric,
        metric_semantics="delta", status="OK",
        values={"attempted": attempted, "acknowledged": attempted, "failed": 0},
        dimensions={"topic": "ecommerce_reviews"}, run_id="test-run",
        event_time=at, observed_at=at,
    )


def moment(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


def append(path: Path, value: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(value) + "\n")


def setup_registry(root: Path, bad_id: str | None = None) -> Path:
    path = root / "registry.json"
    entries = [] if bad_id is None else [{
        "source": "spark_bronze_monitor.jsonl",
        "reason": "known false zero",
        "event_ids": {"quality_counts": bad_id},
    }]
    path.write_text(json.dumps({"schema_version": "monitoring-quarantine-v1",
                                "entries": entries}), encoding="utf-8")
    return path


class CollectorStoreContractTest(unittest.TestCase):
    def test_freshness_transitions_without_new_source_and_then_resolve(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            append(root / "raw_data_monitor.jsonl", event(20, at="2026-09-28T10:00:00Z"))
            registry = setup_registry(root)
            collect_once(root, registry, now=moment("2026-09-28T10:00:10Z"))
            self.assertEqual(collect_once(root, registry, now=moment("2026-09-28T10:01:16Z"))["changed"], True)
            self.assertEqual(collect_once(root, registry, now=moment("2026-09-28T10:02:00Z"))["changed"], False)
            collect_once(root, registry, now=moment("2026-09-28T10:05:01Z"))
            committed = build_monitoring_event(
                component="kafka_to_bronze_dlq", metric_name="last_committed",
                metric_semantics="gauge", status="OK",
                values={"committed_at_epoch_ms": int(moment("2026-09-28T10:05:10Z").timestamp() * 1000)},
                dimensions={"topic": "ecommerce_reviews", "batch_id": 1},
                run_id="bronze-test", event_time="2026-09-28T10:05:10Z",
                observed_at="2026-09-28T10:05:10Z",
            )
            append(root / "spark_bronze_monitor.jsonl", committed)
            collect_once(root, registry, now=moment("2026-09-28T10:05:20Z"))
            alerts = [json.loads(line) for line in
                      (root / "aggregated/alerts.jsonl").read_text().splitlines()]
            self.assertEqual([(item["transition"], item["severity"]) for item in alerts],
                             [("OPEN", "WARNING"), ("RESOLVED", "WARNING"),
                              ("OPEN", "CRITICAL"), ("RESOLVED", "CRITICAL")])
            summary = json.loads((root / "aggregated/summary.json").read_text())
            self.assertEqual(summary["alerts_status"], "OK")
            self.assertEqual(collect_once(root, registry, now=moment("2026-09-28T10:06:00Z"))["changed"], False)

    def test_dry_run_does_not_create_runtime_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            append(root / "raw_data_monitor.jsonl", event(1))
            registry = setup_registry(root)
            result = collect_once(root, registry, dry_run=True)
            self.assertEqual(result["accepted_events"], 1)
            self.assertFalse((root / "aggregated").exists())
            self.assertFalse((root / "state").exists())
            self.assertFalse((root / "rejected").exists())

    def test_first_run_quarantine_and_no_input_rerun_are_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            good = event(2)
            bad = event(0)
            append(root / "raw_data_monitor.jsonl", good)
            append(root / "spark_bronze_monitor.jsonl", bad)
            registry = setup_registry(root, bad["event_id"])
            first = collect_once(root, registry)
            self.assertEqual((first["accepted_events"], first["rejected_events"]), (1, 1))
            summary_path = root / "aggregated/summary.json"
            before = summary_path.read_bytes()
            state = json.loads((root / "state/collector_state.json").read_text())
            summary = json.loads(before)
            self.assertEqual(state["snapshot_id"], summary["snapshot_id"])
            self.assertEqual(collect_once(root, registry)["changed"], False)
            self.assertEqual(summary_path.read_bytes(), before)
            self.assertEqual(len((root / "rejected/rejected_events.jsonl").read_text().splitlines()), 1)

    def test_crash_after_journal_replays_without_double_count(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw_data_monitor.jsonl"
            append(source, event(2))
            registry = setup_registry(root)
            collect_once(root, registry)
            append(source, event(3))
            with self.assertRaisesRegex(RuntimeError, "injected collector failure"):
                collect_once(root, registry, fail_after_stage="journal")
            repaired = collect_once(root, registry)
            self.assertEqual(repaired["accepted_events"], 2)
            history = [json.loads(line) for line in
                       (root / "aggregated/history.jsonl").read_text().splitlines()]
            self.assertEqual(history[0]["values"],
                             {"attempted": 5, "acknowledged": 5, "failed": 0})
            self.assertEqual(history[0]["event_count"], 2)
            self.assertEqual(collect_once(root, registry)["changed"], False)

    def test_partial_line_waits_for_newline_before_cursor_moves(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw_data_monitor.jsonl"
            line = json.dumps(event(4))
            source.write_text(line[:25], encoding="utf-8")
            registry = setup_registry(root)
            initial = collect_once(root, registry)
            self.assertEqual(initial["accepted_events"], 0)
            state = json.loads((root / "state/collector_state.json").read_text())
            self.assertEqual(state["sources"][source.name]["offset"], 0)
            with source.open("a", encoding="utf-8") as handle:
                handle.write(line[25:] + "\n")
            self.assertEqual(collect_once(root, registry)["accepted_events"], 1)

    def test_truncation_and_registry_change_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "raw_data_monitor.jsonl"
            append(source, event(2))
            registry = setup_registry(root)
            collect_once(root, registry)
            registry.write_text(registry.read_text() + "\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "registry changed"):
                collect_once(root, registry)
            setup_registry(root)
            source.write_text("", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "source (shorter|generation changed|truncated)"):
                collect_once(root, registry)

    def test_state_committed_last_after_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            append(root / "raw_data_monitor.jsonl", event(1))
            registry = setup_registry(root)
            with self.assertRaisesRegex(RuntimeError, "injected collector failure"):
                collect_once(root, registry, fail_after_stage="summary")
            self.assertFalse((root / "state/collector_state.json").exists())
            result = collect_once(root, registry)
            self.assertEqual(result["accepted_events"], 1)
            self.assertTrue((root / "state/collector_state.json").exists())


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(CollectorStoreContractTest)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
    print(f"MONITORING_COLLECTOR_STORE_CONTRACT_OK tests={result.testsRun}")
