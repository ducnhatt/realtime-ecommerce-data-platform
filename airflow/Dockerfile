ARG AIRFLOW_BASE_IMAGE=apache/airflow:slim-3.3.2-python3.10
FROM ${AIRFLOW_BASE_IMAGE}

USER root

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

ARG PYSPARK_VERSION=4.0.4
RUN pip install --no-cache-dir "pyspark==${PYSPARK_VERSION}"

COPY --chown=airflow:root requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --requirement /tmp/requirements-dbt.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV DBT_EXECUTABLE=/home/airflow/dbt-venv/bin/dbt
