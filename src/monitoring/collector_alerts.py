"""Observability v1 alert signals and idempotent OPEN/RESOLVED transitions."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


ALERT_RULE_VERSION = "monitoring-alert-rules-v1"
NAMESPACE = uuid.UUID("da79196d-d72c-40a8-aa0a-e801dc480ce9")


def _instant(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _latest(events: list[dict[str, Any]], component: str, metric: str) -> dict[str, Any] | None:
    matching = [event for event in events if event["component"] == component
                and event["metric_name"] == metric]
    return max(matching, key=lambda event: (event["event_time"], event["event_id"])) if matching else None


def _signal(name: str, component: str, severity: str, topic: str | None,
            value: float, threshold: float, evidence_event_id: str) -> tuple[str, dict[str, Any]]:
    fingerprint = hashlib.sha256(f"{name}|{component}|{topic or ''}".encode()).hexdigest()[:24]
    return fingerprint, {
        "alert_name": name, "component": component, "topic": topic,
        "severity": severity, "observed_value": value, "threshold": threshold,
        "evidence_event_id": evidence_event_id,
    }


def evaluate_alerts(events: Iterable[dict[str, Any]], now: datetime) -> dict[str, dict[str, Any]]:
    """Evaluate five v1 rules from validated, non-quarantined event history."""

    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    current = now.astimezone(timezone.utc)
    records = list(events)
    active: dict[str, dict[str, Any]] = {}

    producer = _latest(records, "producer", "delivery_counts")
    bronze_commit = _latest(records, "kafka_to_bronze_dlq", "last_committed")
    if producer:
        attempted = producer["values"]["attempted"]
        acknowledged = producer["values"]["acknowledged"]
        gap = attempted - acknowledged
        topic = producer["dimensions"].get("topic")
        if gap > 0:
            key, signal = _signal("producer_delivery_gap", "producer", "CRITICAL",
                                  topic, gap, 0, producer["event_id"])
            active[key] = signal
        if acknowledged > 0:
            committed_at = (
                datetime.fromtimestamp(
                    bronze_commit["values"]["committed_at_epoch_ms"] / 1000, timezone.utc
                ) if bronze_commit else None
            )
            producer_at = _instant(producer["event_time"])
            if committed_at is None or committed_at < producer_at:
                age_seconds = max(0.0, (current - producer_at).total_seconds())
                severity = "CRITICAL" if age_seconds > 300 else "WARNING" if age_seconds > 75 else None
                if severity:
                    key, signal = _signal("bronze_freshness", "kafka_to_bronze_dlq",
                                          severity, topic, age_seconds,
                                          300 if severity == "CRITICAL" else 75,
                                          producer["event_id"])
                    active[key] = signal

    quality = [event for event in records if event["component"] == "kafka_to_bronze_dlq"
               and event["metric_name"] == "quality_counts"
               and event["values"]["batch_input_rows"] > 0]
    quality.sort(key=lambda event: (event["event_time"], event["event_id"]))
    recent_quality = quality[-10:]
    input_rows = sum(event["values"]["batch_input_rows"] for event in recent_quality)
    if input_rows >= 100:
        invalid = sum(event["values"]["invalid_count"] for event in recent_quality)
        rate = invalid / input_rows
        severity = "CRITICAL" if rate > 0.40 else "WARNING" if rate > 0.25 else None
        if severity:
            topic = recent_quality[-1]["dimensions"].get("topic")
            key, signal = _signal("bronze_invalid_rate", "kafka_to_bronze_dlq",
                                  severity, topic, rate,
                                  0.40 if severity == "CRITICAL" else 0.25,
                                  recent_quality[-1]["event_id"])
            active[key] = signal

    nonempty_batches = {(event["run_id"], event["dimensions"]["batch_id"])
                        for event in quality}
    durations = [event for event in records if event["component"] == "kafka_to_bronze_dlq"
                 and event["metric_name"] == "batch_duration"
                 and (event["run_id"], event["dimensions"]["batch_id"]) in nonempty_batches]
    durations.sort(key=lambda event: (event["event_time"], event["event_id"]))
    recent_durations = durations[-3:]
    if len(recent_durations) == 3:
        minimum = min(event["values"]["duration_ms"] for event in recent_durations)
        severity = "CRITICAL" if minimum > 75000 else "WARNING" if minimum > 25000 else None
        if severity:
            topic = recent_durations[-1]["dimensions"].get("topic")
            key, signal = _signal("bronze_batch_duration", "kafka_to_bronze_dlq",
                                  severity, topic, minimum,
                                  75000 if severity == "CRITICAL" else 25000,
                                  recent_durations[-1]["event_id"])
            active[key] = signal

    silver = _latest(records, "bronze_to_silver_incremental", "run_counts")
    if silver:
        mismatch = abs(silver["values"]["input_rows"] - silver["values"]["output_rows"])
        if mismatch > 0:
            topic = silver["dimensions"].get("topic")
            key, signal = _signal("bronze_silver_row_mismatch",
                                  "bronze_to_silver_incremental", "CRITICAL", topic,
                                  mismatch, 0, silver["event_id"])
            active[key] = signal

    return active


def transition_alerts(
    previous: dict[str, dict[str, Any]],
    active: dict[str, dict[str, Any]],
    *,
    at: datetime,
    source_identity: str,
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    """Emit only state transitions; changing severity resolves then reopens."""

    observed_at = _timestamp(at)
    next_open: dict[str, dict[str, Any]] = {}
    transitions: list[dict[str, Any]] = []
    for fingerprint in sorted(set(previous) | set(active)):
        old = previous.get(fingerprint)
        new = active.get(fingerprint)
        if old and (new is None or old["severity"] != new["severity"]):
            transitions.append({
                "transition_id": str(uuid.uuid5(NAMESPACE, f"{fingerprint}|RESOLVED|{old['severity']}|{source_identity}")),
                "fingerprint": fingerprint, "transition": "RESOLVED",
                "severity": old["severity"], "alert_name": old["alert_name"],
                "component": old["component"], "topic": old["topic"],
                "occurred_at": observed_at,
            })
        if new:
            if old is None or old["severity"] != new["severity"]:
                opened_at = observed_at
                transitions.append({
                    "transition_id": str(uuid.uuid5(NAMESPACE, f"{fingerprint}|OPEN|{new['severity']}|{source_identity}")),
                    "fingerprint": fingerprint, "transition": "OPEN",
                    **new, "occurred_at": opened_at,
                })
            else:
                opened_at = old["opened_at"]
            next_open[fingerprint] = {**new, "opened_at": opened_at}
    return next_open, transitions
