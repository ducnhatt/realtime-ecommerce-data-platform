# Spark application layout

The Spark source tree separates deployable data workloads from verification tooling.

```text
spark/
├── jobs/
│   ├── streaming/      # long-running Kafka consumers
│   ├── incremental/    # bounded available-now operational processing
│   └── recovery/       # manually invoked rebuild/recovery workloads
├── checks/
│   ├── reconciliation/ # read-only cross-system data checks
│   ├── contracts/      # deterministic schema/transformation regression checks
│   └── smoke/          # isolated infrastructure connectivity checks
└── pipeline/           # reusable transformations and shared configuration
```

## Operational jobs

- `jobs/streaming/kafka_to_bronze_dlq.py`
- `jobs/streaming/kafka_to_redis.py`
- `jobs/incremental/bronze_to_silver_incremental.py`

## On-demand recovery

- `jobs/recovery/bronze_to_silver_batch.py`

## Checks are not scheduled business pipelines

Reconciliation checks may later be scheduled as observability controls. Contract and smoke checks run during development, environment setup or regression testing. They are retained to make integration boundaries reproducible, but they are not presented as production data jobs.

All files execute with `PYTHONPATH=/opt/spark-apps`, so moving entrypoints into these folders does not change imports from the shared `pipeline` package.

