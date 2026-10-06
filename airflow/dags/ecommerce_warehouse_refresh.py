"""Orchestrate the bounded Bronze-to-Gold warehouse refresh and quality gates."""

import base64
import logging
import os
import subprocess
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, task


LOGGER = logging.getLogger(__name__)
SPARK_APPS = Path("/opt/spark-apps")
CLICKHOUSE_CHECKS = Path("/opt/clickhouse/checks")
DBT_PROJECT = Path("/opt/dbt-project")
WAREHOUSE_DAG_SCHEDULE = os.getenv("WAREHOUSE_DAG_SCHEDULE", "*/30 * * * *")


def spark_submit(
    script: Path,
    packages: list[str],
    expected_marker: str,
) -> None:
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
        "--conf",
        "spark.jars.ivy=/opt/airflow/ivy",
        "--executor-memory",
        "1g",
        "--total-executor-cores",
        "2",
    ]
    if packages:
        command.extend(["--packages", ",".join(packages)])
    command.append(str(script))
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    marker_found = False
    assert process.stdout is not None
    for line in process.stdout:
        message = line.rstrip()
        LOGGER.info("SPARK_SUBMIT_OUTPUT %s", message)
        marker_found = marker_found or expected_marker in message

    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    if not marker_found:
        raise RuntimeError(f"Missing Spark success marker: {expected_marker}")


def run_clickhouse_contract(filename: str, expected_marker: str) -> None:
    sql = (CLICKHOUSE_CHECKS / filename).read_text(encoding="utf-8")
    statements = [statement.strip() for statement in sql.split(";") if statement.strip()]
    auth = base64.b64encode(
        f"{os.environ['CLICKHOUSE_USER']}:{os.environ['CLICKHOUSE_PASSWORD']}".encode(
            "utf-8"
        )
    ).decode("ascii")
    endpoint = (
        f"http://{os.environ['CLICKHOUSE_HOST']}:"
        f"{os.environ['CLICKHOUSE_HTTP_PORT']}/"
    )
    outputs: list[str] = []
    for statement in statements:
        request = urllib.request.Request(
            endpoint,
            data=statement.encode("utf-8"),
            headers={"Authorization": f"Basic {auth}"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            output = response.read().decode("utf-8").strip()
        if output:
            outputs.append(output)
            LOGGER.info("CLICKHOUSE_CONTRACT_OUTPUT %s", output)

    if not any(expected_marker in output for output in outputs):
        raise RuntimeError(f"Missing ClickHouse contract marker: {expected_marker}")


with DAG(
    dag_id="ecommerce_warehouse_refresh_v1",
    description="Bounded, quality-gated Bronze-to-Gold warehouse refresh.",
    schedule=WAREHOUSE_DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["ecommerce", "warehouse", "production"],
) as dag:

    @task(execution_timeout=timedelta(minutes=30))
    def bronze_to_silver() -> None:
        spark_submit(
            SPARK_APPS / "jobs/incremental/bronze_to_silver_incremental.py",
            [os.environ["SPARK_HADOOP_CLOUD_PACKAGE"]],
            "BRONZE_TO_SILVER_INCREMENTAL_OK",
        )

    @task(execution_timeout=timedelta(minutes=30))
    def reconcile_bronze_silver() -> None:
        spark_submit(
            SPARK_APPS / "checks/reconciliation/reconcile_bronze_silver.py",
            [os.environ["SPARK_HADOOP_CLOUD_PACKAGE"]],
            "BRONZE_SILVER_RECONCILIATION_OK",
        )

    @task(execution_timeout=timedelta(minutes=30))
    def silver_to_staging() -> None:
        spark_submit(
            SPARK_APPS / "jobs/incremental/silver_to_clickhouse_staging.py",
            [
                os.environ["SPARK_HADOOP_CLOUD_PACKAGE"],
                os.environ["CLICKHOUSE_SPARK_PACKAGE"],
                os.environ["CLICKHOUSE_JDBC_PACKAGE"],
            ],
            "SILVER_TO_CLICKHOUSE_STAGING_INCREMENTAL_OK",
        )

    @task(execution_timeout=timedelta(minutes=5))
    def validate_staging() -> None:
        run_clickhouse_contract(
            "validate_staging_schema.sql",
            "CLICKHOUSE_STAGING_CONTRACT_OK",
        )

    @task(execution_timeout=timedelta(minutes=20))
    def dbt_build_gold_prod() -> None:
        command = [
            os.environ["DBT_EXECUTABLE"],
            "build",
            "--project-dir",
            str(DBT_PROJECT),
            "--profiles-dir",
            str(DBT_PROJECT),
            "--target",
            "prod",
        ]
        subprocess.run(command, check=True)
        LOGGER.info("AIRFLOW_DBT_GOLD_BUILD_OK target=prod")

    @task(execution_timeout=timedelta(minutes=5))
    def validate_gold() -> None:
        run_clickhouse_contract(
            "validate_gold_schema.sql",
            "CLICKHOUSE_GOLD_SCHEMA_CONTRACT_OK",
        )
        run_clickhouse_contract(
            "validate_gold_data.sql",
            "CLICKHOUSE_GOLD_DATA_CONTRACT_OK",
        )

    @task(execution_timeout=timedelta(minutes=30))
    def reconcile_silver_staging_gold() -> None:
        spark_submit(
            SPARK_APPS
            / "checks/reconciliation/reconcile_silver_clickhouse_gold.py",
            [
                os.environ["SPARK_HADOOP_CLOUD_PACKAGE"],
                os.environ["CLICKHOUSE_SPARK_PACKAGE"],
                os.environ["CLICKHOUSE_JDBC_PACKAGE"],
            ],
            "SILVER_STAGING_GOLD_RECONCILIATION_OK",
        )

    (
        bronze_to_silver()
        >> reconcile_bronze_silver()
        >> silver_to_staging()
        >> validate_staging()
        >> dbt_build_gold_prod()
        >> validate_gold()
        >> reconcile_silver_staging_gold()
    )
