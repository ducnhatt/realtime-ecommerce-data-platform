"""Validate the isolated dbt runtime and its ClickHouse dev connection."""

import os
import logging
import subprocess
from datetime import datetime, timezone

from airflow.sdk import DAG, task


LOGGER = logging.getLogger(__name__)


with DAG(
    dag_id="dbt_debug_smoke_v1",
    description="Run dbt debug against ClickHouse dev without materializing models.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "dbt", "smoke"],
) as dag:

    @task
    def debug_dbt() -> None:
        target = "dev"
        command = [
            os.environ["DBT_EXECUTABLE"],
            "debug",
            "--project-dir",
            "/opt/dbt-project",
            "--profiles-dir",
            "/opt/dbt-project",
            "--target",
            target,
        ]
        subprocess.run(command, check=True)
        LOGGER.info("AIRFLOW_DBT_DEBUG_OK target=%s", target)

    debug_dbt()
