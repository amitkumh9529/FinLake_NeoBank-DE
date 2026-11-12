import os
import sys

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def reset_local_schemas(spark: SparkSession) -> None:
    """Drops all local test schemas so every test module runs in complete isolation."""
    for schema_name in ["metadata", "source", "bronze", "silver", "gold", "quarantine"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Provides an isolated, Delta-enabled local SparkSession with temp warehouse."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark_home = os.environ.get("SPARK_HOME", "")
    if spark_home and "spark-4" in spark_home.lower():
        os.environ.pop("SPARK_HOME", None)

    warehouse_dir = tmp_path_factory.mktemp("spark_warehouse").as_posix()
    derby_dir = tmp_path_factory.mktemp("derby_home").as_posix()

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("finlake-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby_dir}")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
