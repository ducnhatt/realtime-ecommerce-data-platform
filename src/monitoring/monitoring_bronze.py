"""Bronze/DLQ driver monitoring adapter."""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path

from src.monitoring.jsonl_writer import write_monitoring_event


class MonitoringBronze:
    """Emit one coherent monitoring group after both sinks commit a batch."""

    def __init__(self, path: str | Path, topic: str, application_id: str) -> None:
        self.path = path
        self.topic = topic
        self.application_id = application_id

    def _event_id(self, batch_id: int, metric_name: str) -> str:
        identity = (
            f"observability-v1:{self.application_id}:{self.topic}:"
            f"{batch_id}:{metric_name}"
        )
        return str(uuid.uuid5(uuid.NAMESPACE_URL, identity))

    def log_committed_batch(
        self,
        *,
        batch_id: int,
        batch_input_rows: int,
        valid_count: int,
        warning_count: int,
        invalid_count: int,
        duration_ms: int,
        committed_at: str,
    ) -> int:
        quality_total = valid_count + warning_count + invalid_count
        status = "OK" if quality_total == batch_input_rows else "WARNING"
        committed_at_epoch_ms = int(
            datetime.fromisoformat(committed_at.replace("Z", "+00:00")).timestamp()
            * 1000
        )
        dimensions = {
            "topic": self.topic,
            "batch_id": batch_id,
            "sink_pair": "bronze_dlq",
        }
        events = (
            {
                "metric_name": "quality_counts",
                "metric_semantics": "delta",
                "values": {
                    "batch_input_rows": batch_input_rows,
                    "valid_count": valid_count,
                    "warning_count": warning_count,
                    "invalid_count": invalid_count,
                },
            },
            {
                "metric_name": "batch_duration",
                "metric_semantics": "gauge",
                "values": {"duration_ms": duration_ms},
            },
            {
                "metric_name": "last_committed",
                "metric_semantics": "gauge",
                "values": {"committed_at_epoch_ms": committed_at_epoch_ms},
            },
        )
        written = sum(
            write_monitoring_event(
                self.path,
                component="kafka_to_bronze_dlq",
                status=status,
                run_id=self.application_id,
                event_id=self._event_id(batch_id, event["metric_name"]),
                event_time=committed_at,
                dimensions=dimensions,
                **event,
            )
            for event in events
        )
        print(
            "BRONZE_MONITORING_BATCH "
            f"status={status} application_id={self.application_id} "
            f"batch_id={batch_id} input_rows={batch_input_rows} "
            f"valid={valid_count} warning={warning_count} invalid={invalid_count} "
            f"duration_ms={duration_ms} events_written={written} "
            f"expected={len(events)} path={self.path}",
            flush=True,
        )
        return written
