"""Small distributed computation used by the Airflow Spark-submit boundary test."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    spark = SparkSession.builder.appName("airflow-spark-submit-smoke-v1").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    result = (
        spark.range(1, 101, numPartitions=2)
        .agg(F.count("id").alias("row_count"), F.sum("id").alias("id_sum"))
        .first()
    )

    if result["row_count"] != 100 or result["id_sum"] != 5050:
        raise RuntimeError(
            "Airflow Spark-submit smoke mismatch: "
            f"row_count={result['row_count']} id_sum={result['id_sum']}"
        )

    print(
        "AIRFLOW_SPARK_SUBMIT_OK "
        f"application_id={spark.sparkContext.applicationId} "
        f"row_count={result['row_count']} id_sum={result['id_sum']} "
        f"input_partitions=2"
    )
    spark.stop()


if __name__ == "__main__":
    main()
