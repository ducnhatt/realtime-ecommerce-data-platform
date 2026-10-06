"""Shared, best-effort JSONL writer for observability v1."""

from __future__ import annotations

import json
import logging
import math
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = "monitoring-event-v1"
ALLOWED_METRIC_SEMANTICS = frozenset({"delta", "gauge", "cumulative"})
ALLOWED_STATUSES = frozenset({"OK", "WARNING", "FAILED"})

_WRITE_LOCK = threading.Lock()


class MonitoringEventValidationError(ValueError):
    """Raised when a monitoring event violates the v1 contract."""


def _require_non_empty_string(name: str, value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise MonitoringEventValidationError(f"{name} must be a non-empty string")
    return value.strip()


def _utc_timestamp(value: datetime | str | None) -> str:
    if value is None:
        parsed = datetime.now(timezone.utc)
    elif isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise MonitoringEventValidationError(
                f"invalid ISO-8601 timestamp: {value}"
            ) from exc
    else:
        raise MonitoringEventValidationError(
            "timestamp must be a datetime, ISO-8601 string, or None"
        )

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise MonitoringEventValidationError("timestamp must be timezone-aware")

    return (
        parsed.astimezone(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def _validate_event_id(event_id: str | None) -> str:
    if event_id is None:
        return str(uuid.uuid4())

    candidate = _require_non_empty_string("event_id", event_id)
    try:
        uuid.UUID(candidate)
    except ValueError as exc:
        raise MonitoringEventValidationError("event_id must be a valid UUID") from exc
    return candidate


def _validate_values(values: Mapping[str, int | float]) -> dict[str, int | float]:
    if not isinstance(values, Mapping) or not values:
        raise MonitoringEventValidationError("values must be a non-empty mapping")

    validated: dict[str, int | float] = {}
    for key, value in values.items():
        metric_key = _require_non_empty_string("values key", key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise MonitoringEventValidationError(
                f"values.{metric_key} must be numeric and not bool"
            )
        if not math.isfinite(float(value)):
            raise MonitoringEventValidationError(
                f"values.{metric_key} must be finite"
            )
        validated[metric_key] = value
    return validated


def _validate_dimensions(
    dimensions: Mapping[str, str | int | bool] | None,
) -> dict[str, str | int | bool]:
    if dimensions is None:
        return {}
    if not isinstance(dimensions, Mapping):
        raise MonitoringEventValidationError("dimensions must be a mapping")

    validated: dict[str, str | int | bool] = {}
    for key, value in dimensions.items():
        dimension_key = _require_non_empty_string("dimensions key", key)
        if not isinstance(value, (str, int, bool)):
            raise MonitoringEventValidationError(
                f"dimensions.{dimension_key} must be a string, integer, or bool"
            )
        if isinstance(value, str) and not value.strip():
            raise MonitoringEventValidationError(
                f"dimensions.{dimension_key} must not be empty"
            )
        validated[dimension_key] = value
    return validated


def build_monitoring_event(
    *,
    component: str,
    metric_name: str,
    metric_semantics: str,
    status: str,
    values: Mapping[str, int | float],
    dimensions: Mapping[str, str | int | bool] | None = None,
    run_id: str | None = None,
    event_id: str | None = None,
    event_time: datetime | str | None = None,
    observed_at: datetime | str | None = None,
) -> dict[str, Any]:
    """Build and validate one monitoring-event-v1 envelope."""

    validated_component = _require_non_empty_string("component", component)
    validated_metric_name = _require_non_empty_string("metric_name", metric_name)

    if metric_semantics not in ALLOWED_METRIC_SEMANTICS:
        raise MonitoringEventValidationError(
            "metric_semantics must be one of "
            f"{sorted(ALLOWED_METRIC_SEMANTICS)}"
        )
    if status not in ALLOWED_STATUSES:
        raise MonitoringEventValidationError(
            f"status must be one of {sorted(ALLOWED_STATUSES)}"
        )
    if run_id is not None:
        run_id = _require_non_empty_string("run_id", run_id)

    return {
        "event_id": _validate_event_id(event_id),
        "event_time": _utc_timestamp(event_time),
        "observed_at": _utc_timestamp(observed_at),
        "component": validated_component,
        "metric_name": validated_metric_name,
        "metric_semantics": metric_semantics,
        "status": status,
        "run_id": run_id,
        "values": _validate_values(values),
        "dimensions": _validate_dimensions(dimensions),
        "schema_version": SCHEMA_VERSION,
    }


def write_monitoring_event(
    path: str | Path,
    *,
    component: str,
    metric_name: str,
    metric_semantics: str,
    status: str,
    values: Mapping[str, int | float],
    dimensions: Mapping[str, str | int | bool] | None = None,
    run_id: str | None = None,
    event_id: str | None = None,
    event_time: datetime | str | None = None,
    observed_at: datetime | str | None = None,
    logger: logging.Logger | None = None,
) -> bool:
    """Append one event and return False instead of blocking the data path."""

    active_logger = logger or LOGGER
    try:
        event = build_monitoring_event(
            component=component,
            metric_name=metric_name,
            metric_semantics=metric_semantics,
            status=status,
            values=values,
            dimensions=dimensions,
            run_id=run_id,
            event_id=event_id,
            event_time=event_time,
            observed_at=observed_at,
        )
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

        with _WRITE_LOCK:
            with target.open("a", encoding="utf-8", newline="\n") as handle:
                handle.write(line)
                handle.write("\n")
                handle.flush()
        return True
    except Exception as exc:  # Monitoring must remain best-effort.
        active_logger.warning(
            "MONITORING_WRITE_FAILED path=%s component=%s metric_name=%s error=%s",
            path,
            component,
            metric_name,
            exc,
        )
        return False
