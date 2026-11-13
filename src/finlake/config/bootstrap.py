from pathlib import Path
from typing import Optional

from pyspark.sql import SparkSession

from finlake.config.settings import qualify_schema_name, qualify_table_name
from finlake.utils.logger import get_logger

logger = get_logger("finlake.bootstrap")


class MetadataBootstrapper:
    """Initializes Medallion schemas, metadata Delta tables, and idempotent seed configuration."""

    def __init__(self, spark: SparkSession, catalog: Optional[str] = "finlake"):
        self.spark = spark
        self.catalog = catalog.strip() if catalog and catalog.strip() else None

    def _tbl(self, schema: str, table: str) -> str:
        return qualify_table_name(self.catalog, schema, table)

    def _schema(self, schema: str) -> str:
        return qualify_schema_name(self.catalog, schema)

    def initialize_schemas(
        self,
        create_unity_catalog: bool = False,
        location_root: Optional[Path] = None,
    ) -> None:
        """Creates catalog (optional in UC) and all Medallion schemas."""
        if create_unity_catalog and self.catalog:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS {self.catalog}")

        for schema_name in ["metadata", "source", "bronze", "silver", "gold", "quarantine"]:
            fq_schema = self._schema(schema_name)
            if location_root is not None:
                schema_dir = (Path(location_root) / schema_name).as_posix()
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema} LOCATION '{schema_dir}'")
            else:
                self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")
        logger.info(f"Initialized schemas under catalog={self.catalog or 'spark_catalog'}")

    def initialize_metadata_tables(self) -> None:
        """Creates the 5 core Delta control tables in metadata schema."""
        tables_ddl = [
            f"""
            CREATE TABLE IF NOT EXISTS {self._tbl("metadata", "tables")} (
                table_id            INT NOT NULL,
                table_name          STRING NOT NULL,
                source_system       STRING NOT NULL,
                source_schema       STRING,
                source_table        STRING,
                source_path         STRING,
                target_layer        STRING NOT NULL,
                bronze_schema       STRING,
                silver_schema       STRING,
                gold_schema         STRING,
                active_flag         BOOLEAN NOT NULL,
                load_order          INT NOT NULL,
                created_at          TIMESTAMP NOT NULL
            ) USING DELTA
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self._tbl("metadata", "table_parameters")} (
                table_id            INT NOT NULL,
                parameter_name      STRING NOT NULL,
                parameter_value     STRING NOT NULL,
                created_at          TIMESTAMP NOT NULL
            ) USING DELTA
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self._tbl("metadata", "data_quality_rules")} (
                rule_id             INT NOT NULL,
                table_id            INT NOT NULL,
                rule_name           STRING NOT NULL,
                rule_expression     STRING NOT NULL,
                severity            STRING NOT NULL,
                active_flag         BOOLEAN NOT NULL,
                created_at          TIMESTAMP NOT NULL
            ) USING DELTA
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self._tbl("metadata", "table_watermarks")} (
                table_id                INT NOT NULL,
                last_watermark_value    STRING NOT NULL,
                last_updated_at         TIMESTAMP NOT NULL,
                last_run_id             BIGINT
            ) USING DELTA
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {self._tbl("metadata", "pipeline_runs")} (
                run_id              BIGINT NOT NULL,
                table_id            INT NOT NULL,
                layer               STRING NOT NULL,
                start_time          TIMESTAMP NOT NULL,
                end_time            TIMESTAMP,
                status              STRING NOT NULL,
                records_read        BIGINT,
                records_written     BIGINT,
                records_quarantined BIGINT,
                error_message       STRING
            ) USING DELTA
            PARTITIONED BY (table_id)
            """,
        ]
        for ddl in tables_ddl:
            self.spark.sql(ddl)
        logger.info("Initialized 5 metadata Delta control tables.")

    def seed_from_sql_file(self, seed_sql_path: Path) -> None:
        """Executes idempotent seed SQL script, adapting catalog prefix if running locally."""
        raw_sql = seed_sql_path.read_text(encoding="utf-8")
        prefix = f"{self.catalog}." if self.catalog else ""
        adapted_sql = raw_sql.replace("finlake.", prefix)

        lines = [line for line in adapted_sql.splitlines() if not line.strip().startswith("--")]
        cleaned_sql = "\n".join(lines)

        for stmt in [s.strip() for s in cleaned_sql.split(";") if s.strip()]:
            self.spark.sql(stmt)

        logger.info(f"Executed idempotent seed script: {seed_sql_path.name}")
