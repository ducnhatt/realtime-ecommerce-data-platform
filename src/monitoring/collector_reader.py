"""Read complete monitoring JSONL records without changing source files."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from src.monitoring.jsonl_writer import (
    SCHEMA_VERSION,
    MonitoringEventValidationError,
    build_monitoring_event,
)


EVENT_FIELDS = frozenset({
    "event_id", "event_time", "observed_at", "component", "metric_name",
    "metric_semantics", "status", "run_id", "values", "dimensions",
    "schema_version",
})


@dataclass(frozen=True)
class RejectedEvent:
    source: str
    byte_offset: int
    event_id: str | None
    reason: str
    line_sha256: str


@dataclass(frozen=True)
class ReadResult:
    accepted: tuple[dict[str, Any], ...]
    rejected: tuple[RejectedEvent, ...]
    next_offset: int
    source_size: int
    incomplete_tail: bool


def load_quarantine_reasons(path: str | Path) -> dict[str, dict[str, str]]:
    """Map source basename and event ID to the audited exclusion reason."""

    registry = json.loads(Path(path).read_text(encoding="utf-8"))
    if registry.get("schema_version") != "monitoring-quarantine-v1":
        raise ValueError("unsupported quarantine registry version")
    by_source: dict[str, dict[str, str]] = {}
    for entry in registry["entries"]:
        source = entry["source"]
        if Path(source).name != source or not entry["reason"]:
            raise ValueError("invalid quarantine source or reason")
        reasons = by_source.setdefault(source, {})
        for event_id in entry["event_ids"].values():
            if event_id in reasons:
                raise ValueError(f"duplicate quarantine event_id: {event_id}")
            reasons[event_id] = entry["reason"]
    return by_source


def validate_envelope(payload: Any) -> dict[str, Any]:
    """Require every v1 field; never fill absent timestamps with current time."""

    if not isinstance(payload, dict) or payload.keys() != EVENT_FIELDS:
        raise MonitoringEventValidationError("event must have exactly the v1 envelope fields")
    if payload["schema_version"] != SCHEMA_VERSION:
        raise MonitoringEventValidationError("unsupported schema_version")
    if not isinstance(payload["event_time"], str) or not isinstance(payload["observed_at"], str):
        raise MonitoringEventValidationError("timestamps must be ISO-8601 strings")
    if payload["run_id"] is not None and not isinstance(payload["run_id"], str):
        raise MonitoringEventValidationError("run_id must be a string or null")
    normalized = build_monitoring_event(
        component=payload["component"],
        metric_name=payload["metric_name"],
        metric_semantics=payload["metric_semantics"],
        status=payload["status"],
        values=payload["values"],
        dimensions=payload["dimensions"],
        run_id=payload["run_id"],
        event_id=payload["event_id"],
        event_time=payload["event_time"],
        observed_at=payload["observed_at"],
    )
    return normalized


def read_complete_events(
    path: str | Path,
    *,
    offset: int = 0,
    quarantine_reasons: Mapping[str, str] | None = None,
) -> ReadResult:
    """Read through complete newline-terminated records from a byte cursor.

    A partial last line remains unread for the next call. A shorter/replaced
    source raises instead of silently applying an old cursor to new bytes.
    """

    source = Path(path)
    if offset < 0:
        raise ValueError("offset must be non-negative")
    if not source.exists():
        if offset:
            raise RuntimeError(f"source disappeared after cursor {offset}: {source}")
        return ReadResult((), (), 0, 0, False)

    accepted: list[dict[str, Any]] = []
    rejected: list[RejectedEvent] = []
    quarantine = quarantine_reasons or {}
    with source.open("rb") as handle:
        source_size = source.stat().st_size
        if source_size < offset:
            raise RuntimeError(
                f"source truncated below cursor: path={source} size={source_size} offset={offset}"
            )
        handle.seek(offset)
        next_offset = offset
        incomplete_tail = False
        while handle.tell() < source_size:
            line_offset = handle.tell()
            line = handle.readline()
            if handle.tell() > source_size or not line.endswith(b"\n"):
                incomplete_tail = True
                break
            next_offset = handle.tell()
            digest = hashlib.sha256(line).hexdigest()
            event_id: str | None = None
            try:
                payload = json.loads(line.decode("utf-8"))
                if isinstance(payload, dict) and isinstance(payload.get("event_id"), str):
                    event_id = payload["event_id"]
                event = validate_envelope(payload)
            except (UnicodeDecodeError, json.JSONDecodeError, MonitoringEventValidationError, TypeError, ValueError) as exc:
                rejected.append(RejectedEvent(source.name, line_offset, event_id,
                                              f"invalid_event:{type(exc).__name__}", digest))
                continue
            if event["event_id"] in quarantine:
                rejected.append(RejectedEvent(source.name, line_offset, event["event_id"],
                                              f"quarantined:{quarantine[event['event_id']]}", digest))
            else:
                accepted.append(event)

    return ReadResult(tuple(accepted), tuple(rejected), next_offset, source_size, incomplete_tail)
