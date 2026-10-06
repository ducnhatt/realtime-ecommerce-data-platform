"""Read-only execution-boundary test from Airflow to ClickHouse Gold."""

import base64
import json
import os
import urllib.request
from datetime import datetime, timezone

from airflow.sdk import DAG, task


with DAG(
    dag_id="clickhouse_read_smoke_v1",
    description="Verify Airflow can execute an authenticated read-only Gold query.",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "clickhouse", "smoke"],
) as dag:

    @task
    def query_gold() -> None:
        host = os.environ["CLICKHOUSE_HOST"]
        port = int(os.environ["CLICKHOUSE_HTTP_PORT"])
        user = os.environ["CLICKHOUSE_USER"]
        password = os.environ["CLICKHOUSE_PASSWORD"]
        database = os.environ["CLICKHOUSE_GOLD_DATABASE"]

        query = f"""
            SELECT
                version() AS clickhouse_version,
                currentUser() AS clickhouse_user,
                count() AS gold_fact_count
            FROM {database}.fact_ecommerce_event
            FORMAT JSONEachRow
        """
        auth = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
        request = urllib.request.Request(
            url=f"http://{host}:{port}/",
            data=query.encode("utf-8"),
            headers={"Authorization": f"Basic {auth}"},
            method="POST",
        )

        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.loads(response.read().decode("utf-8"))

        print(
            "AIRFLOW_CLICKHOUSE_READ_OK "
            f"version={result['clickhouse_version']} "
            f"user={result['clickhouse_user']} "
            f"gold_fact_count={result['gold_fact_count']}"
        )

    query_gold()
