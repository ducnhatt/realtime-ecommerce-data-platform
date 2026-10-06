"""Producer-specific monitoring adapter for raw_data_monitor.jsonl."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from src.monitoring.jsonl_writer import write_monitoring_event


class MonitoringRawData:
    """Emit delivery metrics only after the producer has been flushed."""

    def __init__(self, path: str | Path, topic: str) -> None:
        self.path = path
        self.topic = topic

    def log_delivery_summary(
        self,
        *,
        attempted: int,
        acknowledged: int,
        failed: int,
        outstanding: int,
        duration_ms: float,
        status: str,
        run_id: str,
        event_time: datetime,
    ) -> int:
        common = {
            "path": self.path,
            "component": "producer",
            "status": status,
            "run_id": run_id,
            "event_time": event_time,
            "dimensions": {"topic": self.topic, "mode": "realtime"},
        }
        events = (
            {
                "metric_name": "delivery_counts",
                "metric_semantics": "delta",
                "values": {
                    "attempted": attempted,
                    "acknowledged": acknowledged,
                    "failed": failed,
                },
            },
            {
                "metric_name": "delivery_outstanding",
                "metric_semantics": "gauge",
                "values": {"outstanding": outstanding},
            },
            {
                "metric_name": "delivery_duration",
                "metric_semantics": "gauge",
                "values": {"duration_ms": duration_ms},
            },
        )
        written = sum(
            write_monitoring_event(**common, **event)
            for event in events
        )
        print(
            "PRODUCER_MONITORING_SUMMARY "
            f"status={'OK' if written == len(events) else 'DEGRADED'} "
            f"run_id={run_id} events_written={written} expected={len(events)} "
            f"path={self.path}"
        )
        return written
