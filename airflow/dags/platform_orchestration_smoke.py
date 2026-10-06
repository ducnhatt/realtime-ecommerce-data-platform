"""Minimal DAG used only to validate the local Airflow control plane."""

from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="platform_orchestration_smoke_v1",
    description="Validate DAG parsing, scheduling, task execution, and task logs.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "smoke"],
) as dag:

    @task
    def emit_success_marker() -> None:
        print("AIRFLOW_ORCHESTRATION_SMOKE_OK")

    emit_success_marker()
