from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession

from finlake.config.settings import DatabaseConfig, TableConfig
from finlake.utils.logger import get_logger

logger = get_logger("finlake.ingestion.jdbc")


class PartitionedJDBCReader:
    """Reads RDBMS tables in dynamically balanced parallel partitions across Spark executors."""

    def __init__(self, spark: SparkSession, db_config: DatabaseConfig):
        self.spark = spark
        self.db_config = db_config

    def _build_where_clause(self, table_config: TableConfig, last_watermark: Optional[str]) -> str:
        if not (last_watermark and table_config.watermark_column):
            return ""
        safe_wm = str(last_watermark).strip().replace("'", "''")
        # Derby requires TIMESTAMP() cast when comparing TIMESTAMP columns with string literals
        if "derby" in self.db_config.driver.lower() and len(safe_wm) > 10:
            return f"WHERE {table_config.watermark_column} > TIMESTAMP('{safe_wm}')"
        return f"WHERE {table_config.watermark_column} > '{safe_wm}'"

    def _fetch_partition_bounds(
        self,
        source_ref: str,
        partition_col: str,
        where_clause: str,
        properties: dict,
    ) -> Optional[Tuple[int, int]]:
        """Queries dynamic MIN/MAX bounds to prevent data skew and wasted empty tasks."""
        bounds_query = (
            f"(SELECT MIN({partition_col}) AS min_val, MAX({partition_col}) AS max_val "
            f"FROM {source_ref} {where_clause}) bounds_q"
        )
        row = self.spark.read.jdbc(
            url=self.db_config.jdbc_url,
            table=bounds_query,
            properties=properties,
        ).first()

        # Positional indexing avoids driver case-sensitivity issues (e.g. Derby uppercase)
        if row is None or row[0] is None or row[1] is None:
            return None
        return int(row[0]), int(row[1])

    def read_table(
        self,
        table_config: TableConfig,
        last_watermark: Optional[str] = None,
    ) -> DataFrame:
        """Executes watermark-filtered, dynamically partitioned JDBC extraction."""
        properties = {
            "user": self.db_config.user,
            "password": self.db_config.password,
            "driver": self.db_config.driver,
            "fetchsize": "10000",
        }

        source_ref = f"{table_config.source_schema}.{table_config.source_table}"
        where_clause = self._build_where_clause(table_config, last_watermark)
        subquery = f"(SELECT * FROM {source_ref} {where_clause}) src"

        if table_config.partition_column and table_config.num_partitions > 1:
            bounds = self._fetch_partition_bounds(
                source_ref=source_ref,
                partition_col=table_config.partition_column,
                where_clause=where_clause,
                properties=properties,
            )

            if bounds is None:
                logger.info(f"No incremental records found for {source_ref} ({where_clause}).")
                empty_subquery = f"(SELECT * FROM {source_ref} WHERE 1 = 0) empty_src"
                return self.spark.read.jdbc(
                    url=self.db_config.jdbc_url,
                    table=empty_subquery,
                    properties=properties,
                )

            min_val, max_val = bounds
            if min_val == max_val:
                logger.info(f"Single key value ({min_val}) for {source_ref}; using 1 partition.")
                return self.spark.read.jdbc(
                    url=self.db_config.jdbc_url,
                    table=subquery,
                    properties=properties,
                )

            upper_bound = max_val + 1
            effective_partitions = max(1, min(table_config.num_partitions, upper_bound - min_val))
            logger.info(
                f"Reading {source_ref} across {effective_partitions} partitions "
                f"(col={table_config.partition_column}, bounds=[{min_val}, {upper_bound}))"
            )
            return self.spark.read.jdbc(
                url=self.db_config.jdbc_url,
                table=subquery,
                column=table_config.partition_column,
                lowerBound=min_val,
                upperBound=upper_bound,
                numPartitions=effective_partitions,
                properties=properties,
            )

        logger.info(f"Reading {source_ref} via single-partition JDBC.")
        return self.spark.read.jdbc(
            url=self.db_config.jdbc_url,
            table=subquery,
            properties=properties,
        )
