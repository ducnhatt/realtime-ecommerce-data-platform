"""Submit a bounded PySpark job from Airflow without Docker socket access."""

import os
import subprocess
from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="spark_submit_smoke_v1",
    description="Verify Airflow client-mode submission to the standalone Spark cluster.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "spark", "smoke"],
) as dag:

    @task
    def submit_job() -> None:
        command = [
            "spark-submit",
            "--master",
            os.environ["SPARK_MASTER_URL"],
            "--deploy-mode",
            "client",
            "--conf",
            "spark.driver.host=airflow",
            "--conf",
            "spark.driver.bindAddress=0.0.0.0",
            "--conf",
            "spark.ui.enabled=false",
            "--executor-memory",
            "512m",
            "--total-executor-cores",
            "1",
            "/opt/spark-apps/checks/smoke/airflow_spark_submit_smoke.py",
        ]
        subprocess.run(command, check=True)

    submit_job()
