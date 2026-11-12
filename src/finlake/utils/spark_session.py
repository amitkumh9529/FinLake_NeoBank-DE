import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "finlake-pipeline",
    master: str = "local[2]",
    enable_delta: bool = True,
) -> SparkSession:
    """Returns active Databricks SparkSession or creates a local Delta-enabled session."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session

    # Ensure Spark workers use the exact same Python interpreter (e.g., .venv Python 3.11)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Guard against mismatched external SPARK_HOME (e.g., Spark 4.x vs PySpark 3.5.x)
    spark_home = os.environ.get("SPARK_HOME", "")
    if spark_home and "spark-4" in spark_home.lower():
        os.environ.pop("SPARK_HOME", None)

    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
        .config("spark.ui.enabled", "false")
    )

    if enable_delta:
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    return builder.getOrCreate()
