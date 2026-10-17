"""Pure, replay-safe five-minute aggregation for validated monitoring events."""

from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable


WINDOW = timedelta(minutes=5)
MAX_GAUGES = frozenset({
    ("kafka_to_bronze_dlq", "last_committed", "committed_at_epoch_ms"),
    ("bronze_to_silver_incremental", "checkpoint_progress", "last_batch_id"),
})


def _instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("event timestamp must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def deduplicate_events(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Ignore exact retries, but fail on conflicting payloads for one event ID."""

    seen: dict[str, str] = {}
    unique: list[dict[str, Any]] = []
    for event in events:
        event_id = event["event_id"]
        # observed_at is the writer attempt time and may differ on an idempotent retry.
        identity_payload = {key: value for key, value in event.items() if key != "observed_at"}
        fingerprint = json.dumps(identity_payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        previous = seen.get(event_id)
        if previous is None:
            seen[event_id] = fingerprint
            unique.append(event)
        elif previous != fingerprint:
            raise ValueError(f"conflicting payload for event_id={event_id}")
    return unique


def _combine_numbers(numbers: list[int | float]) -> int | float:
    if all(isinstance(value, int) and not isinstance(value, bool) for value in numbers):
        return sum(numbers)
    return math.fsum(numbers)


def _duration_statistics(events: list[dict[str, Any]]) -> dict[str, float]:
    durations = [float(event["values"]["duration_ms"]) for event in events
                 if "duration_ms" in event["values"]]
    if not durations:
        return {}
    result = {"duration_ms_avg": math.fsum(durations) / len(durations),
              "duration_ms_max": max(durations)}
    if len(durations) >= 20:
        ordered = sorted(durations)
        result["duration_ms_p95"] = ordered[math.ceil(0.95 * len(ordered)) - 1]
    return result


def aggregate_windows(events: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return deterministic metric series by UTC five-minute event-time window.

    This function never reads/writes files or advances a cursor. The caller
    must pass only validated, non-quarantined events.
    """

    groups: dict[tuple[datetime, str, str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for event in deduplicate_events(events):
        moment = _instant(event["event_time"])
        start = moment.replace(minute=(moment.minute // 5) * 5, second=0, microsecond=0)
        topic = event["dimensions"].get("topic")
        if topic is not None and not isinstance(topic, str):
            raise ValueError("topic dimension must be a string")
        groups[(start, event["component"], event["metric_name"], topic)].append(event)

    history: list[dict[str, Any]] = []
    for (start, component, metric_name, topic), group in sorted(
        groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2], item[0][3] or "")
    ):
        ordered = sorted(group, key=lambda event: (
            _instant(event["event_time"]), _instant(event["observed_at"]), event["event_id"]
        ))
        semantics = {event["metric_semantics"] for event in ordered}
        value_keys = {frozenset(event["values"]) for event in ordered}
        if len(semantics) != 1 or len(value_keys) != 1:
            raise ValueError(f"inconsistent semantics/values for {component}.{metric_name}")
        semantic = semantics.pop()
        if semantic == "delta":
            values = {name: _combine_numbers([event["values"][name] for event in ordered])
                      for name in sorted(ordered[0]["values"])}
        elif semantic in {"gauge", "cumulative"}:
            values = dict(ordered[-1]["values"])
            if semantic == "gauge":
                for name in values:
                    if (component, metric_name, name) in MAX_GAUGES:
                        values[name] = max(event["values"][name] for event in ordered)
        else:
            raise ValueError(f"unsupported metric_semantics={semantic}")
        history.append({
            "window_start": _timestamp(start),
            "window_end": _timestamp(start + WINDOW),
            "component": component,
            "metric_name": metric_name,
            "metric_semantics": semantic,
            "topic": topic,
            "values": values,
            "statistics": _duration_statistics(ordered),
            "status": ordered[-1]["status"],
            "status_counts": dict(sorted(Counter(event["status"] for event in ordered).items())),
            "event_count": len(ordered),
        })
    return history
