"""Single-writer, incremental monitoring collector with replay-safe file outputs."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.monitoring.collector_aggregation import aggregate_windows, deduplicate_events
from src.monitoring.collector_alerts import ALERT_RULE_VERSION, evaluate_alerts, transition_alerts
from src.monitoring.collector_reader import load_quarantine_reasons, read_complete_events


SOURCES = (
    "raw_data_monitor.jsonl",
    "spark_bronze_monitor.jsonl",
    "silver_stream_monitor.jsonl",
)
STATE_VERSION = "monitoring-collector-state-v1"
PREFIX_BYTES = 4096


def _json_bytes(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":"), allow_nan=False) + "\n").encode("utf-8")


def _jsonl_bytes(items: list[dict[str, Any]]) -> bytes:
    return b"".join(_json_bytes(item) for item in items)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _prefix_fingerprint(path: Path, offset: int) -> tuple[int, str]:
    length = min(offset, PREFIX_BYTES)
    if not path.exists():
        if offset:
            raise RuntimeError(f"source missing with nonzero cursor: {path}")
        return 0, hashlib.sha256(b"").hexdigest()
    with path.open("rb") as handle:
        prefix = handle.read(length)
    if len(prefix) != length:
        raise RuntimeError(f"source shorter than cursor fingerprint: {path}")
    return length, hashlib.sha256(prefix).hexdigest()


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.",
                                                    suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _maybe_fail(stage: str, fail_after_stage: str | None) -> None:
    if stage == fail_after_stage:
        raise RuntimeError(f"injected collector failure after {stage}")


def collect_once(
    root: str | Path,
    registry_path: str | Path,
    *,
    dry_run: bool = False,
    fail_after_stage: str | None = None,
    now: datetime | None = None,
) -> dict[str, int | bool | str]:
    """Build derived snapshots, then commit source cursors last.

    Airflow must be the sole collector writer. Existing raw JSONL is never
    changed. An accepted-event journal lets retries rebuild the same windows
    even if a previous attempt wrote outputs before its cursor.
    """

    root = Path(root)
    registry_path = Path(registry_path)
    registry_digest = hashlib.sha256(registry_path.read_bytes()).hexdigest()
    quarantine = load_quarantine_reasons(registry_path)
    state_path = root / "state/collector_state.json"
    journal_path = root / "state/accepted_events.jsonl"
    rejected_path = root / "rejected/rejected_events.jsonl"
    history_path = root / "aggregated/history.jsonl"
    alerts_path = root / "aggregated/alerts.jsonl"
    summary_path = root / "aggregated/summary.json"

    has_state = state_path.exists()
    if has_state and any(not path.exists() for path in (
        journal_path, rejected_path, history_path, alerts_path, summary_path
    )):
        raise RuntimeError("committed collector state has missing derived output")
    state = json.loads(state_path.read_text(encoding="utf-8")) if has_state else {
        "schema_version": STATE_VERSION,
        "registry_sha256": registry_digest,
        "sources": {},
        "accepted_events": 0,
        "rejected_events": 0,
    }
    if state.get("schema_version") != STATE_VERSION:
        raise RuntimeError("collector state schema mismatch")
    if state.get("registry_sha256") != registry_digest:
        raise RuntimeError("quarantine registry changed after cursor commit; explicit reprocess required")

    journal = _read_jsonl(journal_path)
    rejected = _read_jsonl(rejected_path)
    alert_history = _read_jsonl(alerts_path)
    if len(journal) < state["accepted_events"] or len(rejected) < state["rejected_events"]:
        raise RuntimeError("event journal or rejected audit is behind committed state")

    new_events: list[dict[str, Any]] = []
    new_rejected: list[dict[str, Any]] = []
    next_sources: dict[str, dict[str, Any]] = {}
    for name in SOURCES:
        source = root / name
        previous = state["sources"].get(name, {"offset": 0})
        offset = previous["offset"]
        if "prefix_sha256" in previous:
            length, digest = _prefix_fingerprint(source, previous["prefix_length"])
            if length != previous["prefix_length"] or digest != previous["prefix_sha256"]:
                raise RuntimeError(f"source generation changed; explicit reprocess required: {source}")
        read = read_complete_events(source, offset=offset,
                                    quarantine_reasons=quarantine.get(name))
        new_events.extend(read.accepted)
        new_rejected.extend(vars(item) for item in read.rejected)
        length, digest = _prefix_fingerprint(source, read.next_offset)
        next_sources[name] = {"offset": read.next_offset,
                              "prefix_length": length, "prefix_sha256": digest}

    source_changed = not has_state or any(
        next_sources[name]["offset"] != state["sources"].get(name, {}).get("offset", 0)
        for name in SOURCES
    )
    accepted = deduplicate_events([*journal, *new_events])
    rejected_index = {(item["source"], item["byte_offset"], item["line_sha256"])
                      for item in rejected}
    for item in new_rejected:
        identity = (item["source"], item["byte_offset"], item["line_sha256"])
        if identity not in rejected_index:
            rejected.append(item)
            rejected_index.add(identity)
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None:
        raise ValueError("collector now must be timezone-aware")
    active_alerts = evaluate_alerts(accepted, observed_at)
    source_identity = hashlib.sha256(_json_bytes(next_sources)).hexdigest()
    next_open, transitions = transition_alerts(
        state.get("open_alerts", {}), active_alerts,
        at=observed_at, source_identity=source_identity,
    )
    alert_index = {item["transition_id"]: item for item in alert_history}
    for item in transitions:
        if item["transition_id"] not in alert_index:
            alert_history.append(item)
            alert_index[item["transition_id"]] = item
        if item["transition"] == "OPEN":
            next_open[item["fingerprint"]]["opened_at"] = alert_index[item["transition_id"]]["occurred_at"]
    changed = (source_changed or state.get("alert_rule_version") != ALERT_RULE_VERSION
               or bool(transitions))
    if not changed:
        return {"changed": False, "accepted_events": len(journal),
                "rejected_events": len(rejected), "windows": len(_read_jsonl(history_path)),
                "alerts_status": "OPEN" if next_open else "OK",
                "alert_transitions": len(alert_history)}

    history = aggregate_windows(accepted)
    snapshot_payload = {
        "sources": next_sources,
        "registry_sha256": registry_digest,
        "event_ids": sorted(event["event_id"] for event in accepted),
        "rejected": sorted((item["source"], item["byte_offset"], item["line_sha256"])
                           for item in rejected),
        "alert_rule_version": ALERT_RULE_VERSION,
        "alert_transitions": sorted(item["transition_id"] for item in alert_history),
        "open_alerts": sorted((key, value["severity"]) for key, value in next_open.items()),
    }
    snapshot_id = hashlib.sha256(_json_bytes(snapshot_payload)).hexdigest()
    history = [{"snapshot_id": snapshot_id, **window} for window in history]
    latest: dict[tuple[str, str, str | None], dict[str, Any]] = {}
    for window in history:
        latest[(window["component"], window["metric_name"], window["topic"])] = window
    summary = {
        "schema_version": "monitoring-summary-v1",
        "snapshot_id": snapshot_id,
        "generated_at": observed_at.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "accepted_events": len(accepted),
        "rejected_events": len(rejected),
        "window_count": len(history),
        "latest": list(latest.values()),
        "alerts_status": "OPEN" if next_open else "OK",
        "open_alerts": [{"fingerprint": key, **value} for key, value in sorted(next_open.items())],
    }
    next_state = {
        "schema_version": STATE_VERSION,
        "snapshot_id": snapshot_id,
        "registry_sha256": registry_digest,
        "sources": next_sources,
        "accepted_events": len(accepted),
        "rejected_events": len(rejected),
        "alert_rule_version": ALERT_RULE_VERSION,
        "open_alerts": next_open,
        "updated_at": summary["generated_at"],
    }

    if dry_run:
        return {"changed": True, "accepted_events": len(accepted),
                "rejected_events": len(rejected), "windows": len(history),
                "alerts_status": summary["alerts_status"],
                "alert_transitions": len(alert_history)}

    for stage, path, content in (
        ("journal", journal_path, _jsonl_bytes(accepted)),
        ("rejected", rejected_path, _jsonl_bytes(rejected)),
        ("history", history_path, _jsonl_bytes(history)),
        ("alerts", alerts_path, _jsonl_bytes(alert_history)),
        ("summary", summary_path, _json_bytes(summary)),
        ("state", state_path, _json_bytes(next_state)),
    ):
        _atomic_write(path, content)
        _maybe_fail(stage, fail_after_stage)
    return {"changed": True, "accepted_events": len(accepted),
            "rejected_events": len(rejected), "windows": len(history),
            "alerts_status": summary["alerts_status"],
            "alert_transitions": len(alert_history)}
