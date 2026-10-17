"""Check audited quarantine IDs against the immutable Bronze monitoring log."""

import argparse
import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_REGISTRY = PROJECT_ROOT / "src/monitoring/known_invalid_events_v1.json"
DEFAULT_SOURCE_DIR = PROJECT_ROOT / "monitoring"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--source-dir", type=Path, default=DEFAULT_SOURCE_DIR)
    args = parser.parse_args()

    registry = json.loads(args.registry.read_text(encoding="utf-8"))
    if registry.get("schema_version") != "monitoring-quarantine-v1":
        raise RuntimeError("Unsupported quarantine registry version")

    total = 0
    seen_ids = set()
    for entry in registry["entries"]:
        evidence = entry["evidence"]
        if evidence["batch_input_rows"] != sum(
            evidence[name] for name in ("valid_count", "warning_count", "invalid_count")
        ):
            raise RuntimeError("Manifest evidence counts do not reconcile")
        source = args.source_dir / entry["source"]
        events = [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines()]
        for metric_name, event_id in entry["event_ids"].items():
            if event_id in seen_ids:
                raise RuntimeError(f"Duplicate quarantined event_id: {event_id}")
            seen_ids.add(event_id)
            matches = [event for event in events if event.get("event_id") == event_id]
            if len(matches) != 1:
                raise RuntimeError(f"Expected exactly one {event_id} in {source}, got {len(matches)}")
            event = matches[0]
            if (
                event.get("component") != "kafka_to_bronze_dlq"
                or event.get("run_id") != entry["run_id"]
                or event.get("dimensions", {}).get("batch_id") != entry["batch_id"]
                or event.get("metric_name") != metric_name
            ):
                raise RuntimeError(f"Quarantine identity mismatch: {event_id}")
            if metric_name == "quality_counts" and event.get("values") != {
                name: 0 for name in evidence
            }:
                raise RuntimeError(f"Historical quality event was not the known false zero: {event_id}")
            total += 1

    if total != 3:
        raise RuntimeError(f"Expected three audited historical events, got {total}")
    print(f"MONITORING_QUARANTINE_CONTRACT_OK entries={len(registry['entries'])} events={total}")


if __name__ == "__main__":
    main()
