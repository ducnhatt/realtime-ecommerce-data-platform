"""Read one consistent, derived monitoring snapshot for the local dashboard."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class SnapshotUnavailable(RuntimeError):
    """Collector output is absent, incomplete, or mid-commit."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotUnavailable(f"Cannot read {path.name}: {error}") from error
    if not isinstance(value, dict):
        raise SnapshotUnavailable(f"{path.name} must contain a JSON object")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        rows = [json.loads(line) for line in lines]
    except (OSError, json.JSONDecodeError) as error:
        raise SnapshotUnavailable(f"Cannot read {path.name}: {error}") from error
    if any(not isinstance(row, dict) for row in rows):
        raise SnapshotUnavailable(f"{path.name} contains a non-object row")
    return rows


def load_dashboard_snapshot(root: str | Path) -> dict[str, Any]:
    """Read only collector outputs, rejecting torn multi-file snapshots.

    The collector atomically replaces files individually and commits state last.
    Comparing state before/after the read keeps the dashboard from presenting a
    mixture of generations during a concurrent collector run.
    """

    root = Path(root)
    state_path = root / "state/collector_state.json"
    state_before = _read_json(state_path)
    summary = _read_json(root / "aggregated/summary.json")
    history = _read_jsonl(root / "aggregated/history.jsonl")
    alerts = _read_jsonl(root / "aggregated/alerts.jsonl")
    state_after = _read_json(state_path)
    snapshot_id = state_before.get("snapshot_id")
    if not snapshot_id or state_after.get("snapshot_id") != snapshot_id:
        raise SnapshotUnavailable("Collector state changed during dashboard read; refresh")
    if summary.get("schema_version") != "monitoring-summary-v1":
        raise SnapshotUnavailable("Unsupported monitoring summary schema")
    if summary.get("snapshot_id") != snapshot_id:
        raise SnapshotUnavailable("Summary and collector state do not match; refresh")
    if summary.get("window_count") != len(history):
        raise SnapshotUnavailable("Summary and history row count do not match; refresh")
    if any(row.get("snapshot_id") != snapshot_id for row in history):
        raise SnapshotUnavailable("History contains rows from another snapshot; refresh")
    return {"summary": summary, "history": history, "alerts": alerts}
