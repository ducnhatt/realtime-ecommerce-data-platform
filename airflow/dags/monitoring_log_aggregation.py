"""Bounded five-minute collector for the three observability v1 JSONL sources."""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, task


LOGGER = logging.getLogger(__name__)
SCHEDULE = os.getenv("MONITORING_AGGREGATION_SCHEDULE", "*/5 * * * *")
MONITORING_ROOT = Path(os.getenv("MONITORING_DIR", "/opt/monitoring"))
REGISTRY_PATH = Path("/opt/platform-src/src/monitoring/known_invalid_events_v1.json")


with DAG(
    dag_id="monitoring_log_aggregation_v1",
    description="Incrementally collect producer, Bronze and Silver monitoring JSONL.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["ecommerce", "monitoring"],
) as dag:

    @task(execution_timeout=timedelta(minutes=2))
    def collect_monitoring() -> None:
        from src.monitoring.collector_store import collect_once

        result = collect_once(MONITORING_ROOT, REGISTRY_PATH)
        LOGGER.info(
            "MONITORING_COLLECTOR_OK changed=%s accepted_events=%s "
            "rejected_events=%s windows=%s alerts_status=%s alert_transitions=%s root=%s",
            result["changed"],
            result["accepted_events"],
            result["rejected_events"],
            result["windows"],
            result["alerts_status"],
            result["alert_transitions"],
            MONITORING_ROOT,
        )

    collect_monitoring()
