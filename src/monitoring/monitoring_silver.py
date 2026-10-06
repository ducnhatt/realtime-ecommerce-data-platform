"""Bounded Bronze-to-Silver run monitoring adapter."""

from __future__ import annotations

import uuid
from pathlib import Path

from src.monitoring.jsonl_writer import write_monitoring_event


class MonitoringSilver:
    """Append a compact performance summary after an availableNow run ends."""

    def __init__(self, path: str | Path, topic: str) -> None:
        self.path = path
        self.topic = topic

    def log_run(
        self,
        *,
        run_id: str,
        input_rows: int,
        output_rows: int,
        non_empty_batches: int,
        duration_ms: float,
        last_batch_id: int,
    ) -> int:
        if input_rows != output_rows:
            status = "WARNING"
        else:
            status = "OK"
        rows_per_second = (
            output_rows * 1000.0 / duration_ms if duration_ms > 0 else 0.0
        )
        common = {
            "path": self.path,
            "component": "bronze_to_silver_incremental",
            "status": status,
            "run_id": run_id,
            "dimensions": {"topic": self.topic, "mode": "availableNow"},
        }
        events = (
            {
                "metric_name": "run_counts",
                "metric_semantics": "delta",
                "values": {
                    "input_rows": input_rows,
                    "output_rows": output_rows,
                    "non_empty_batches": non_empty_batches,
                },
            },
            {
                "metric_name": "run_performance",
                "metric_semantics": "gauge",
                "values": {
                    "duration_ms": duration_ms,
                    "rows_per_second": rows_per_second,
                },
            },
            {
                "metric_name": "checkpoint_progress",
                "metric_semantics": "gauge",
                "values": {"last_batch_id": last_batch_id},
            },
        )
        written = sum(
            write_monitoring_event(
                **common,
                event_id=str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"observability-v1:silver:{run_id}:{event['metric_name']}",
                    )
                ),
                **event,
            )
            for event in events
        )
        print(
            "SILVER_MONITORING_RUN "
            f"status={status} run_id={run_id} input_rows={input_rows} "
            f"output_rows={output_rows} non_empty_batches={non_empty_batches} "
            f"duration_ms={duration_ms:.3f} last_batch_id={last_batch_id} "
            f"events_written={written} expected={len(events)} path={self.path}",
            flush=True,
        )
        return written

    def log_failure(self, *, run_id: str, last_batch_id: int) -> bool:
        """Report a failed run without inventing committed row counts."""
        written = write_monitoring_event(
            self.path,
            component="bronze_to_silver_incremental",
            metric_name="run_failure",
            metric_semantics="delta",
            status="FAILED",
            values={"failure_count": 1},
            dimensions={
                "topic": self.topic,
                "mode": "availableNow",
                "last_batch_id": last_batch_id,
            },
            run_id=run_id,
            event_id=str(
                uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"observability-v1:silver:{run_id}:run_failure",
                )
            ),
        )
        print(
            "SILVER_MONITORING_RUN "
            f"status=FAILED run_id={run_id} last_batch_id={last_batch_id} "
            f"events_written={int(written)} expected=1 path={self.path}",
            flush=True,
        )
        return written
