from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession

from finlake.config.settings import qualify_table_name
from finlake.utils.logger import get_logger

logger = get_logger("finlake.audit")


class AuditLogger:
    """Manages idempotent execution telemetry in metadata.pipeline_runs using parameterized SQL."""

    def __init__(self, spark: SparkSession, catalog: Optional[str] = "finlake"):
        self.spark = spark
        self.catalog = catalog.strip() if catalog and catalog.strip() else None
        self.audit_table = qualify_table_name(self.catalog, "metadata", "pipeline_runs")

    def log_start(self, run_id: int, table_id: int, layer: str) -> None:
        start_time = datetime.now(timezone.utc)
        query = f"""
            MERGE INTO {self.audit_table} t
            USING (
                SELECT :run_id AS run_id, :table_id AS table_id, :layer AS layer
            ) s
            ON t.run_id = s.run_id AND t.table_id = s.table_id AND t.layer = s.layer
            WHEN MATCHED THEN UPDATE SET
                start_time = :start_time,
                end_time = NULL,
                status = 'INPROGRESS',
                records_read = NULL,
                records_written = NULL,
                records_quarantined = NULL,
                error_message = NULL
            WHEN NOT MATCHED THEN INSERT (run_id, table_id, layer, start_time, status)
                VALUES (:run_id, :table_id, :layer, :start_time, 'INPROGRESS')
        """
        self.spark.sql(
            query,
            args={
                "run_id": int(run_id),
                "table_id": int(table_id),
                "layer": layer.strip(),
                "start_time": start_time,
            },
        )
        logger.info(f"[{layer.strip()}] Started run_id={run_id}, table_id={table_id}")

    def log_success(
        self,
        run_id: int,
        table_id: int,
        layer: str,
        records_read: int,
        records_written: int,
        records_quarantined: int = 0,
    ) -> None:
        end_time = datetime.now(timezone.utc)
        query = f"""
            UPDATE {self.audit_table}
            SET end_time = :end_time,
                status = 'SUCCESS',
                records_read = :records_read,
                records_written = :records_written,
                records_quarantined = :records_quarantined,
                error_message = NULL
            WHERE run_id = :run_id AND table_id = :table_id AND layer = :layer
        """
        self.spark.sql(
            query,
            args={
                "run_id": int(run_id),
                "table_id": int(table_id),
                "layer": layer.strip(),
                "end_time": end_time,
                "records_read": int(records_read),
                "records_written": int(records_written),
                "records_quarantined": int(records_quarantined),
            },
        )
        logger.info(
            f"[{layer.strip()}] Succeeded run_id={run_id}, table_id={table_id} "
            f"(read={records_read}, written={records_written}, quarantined={records_quarantined})"
        )

    def log_failure(self, run_id: int, table_id: int, layer: str, error_message: str) -> None:
        end_time = datetime.now(timezone.utc)
        truncated_error = error_message[:1000]
        query = f"""
            UPDATE {self.audit_table}
            SET end_time = :end_time,
                status = 'FAILED',
                error_message = :error_message
            WHERE run_id = :run_id AND table_id = :table_id AND layer = :layer
        """
        self.spark.sql(
            query,
            args={
                "run_id": int(run_id),
                "table_id": int(table_id),
                "layer": layer.strip(),
                "end_time": end_time,
                "error_message": truncated_error,
            },
        )
        logger.error(
            f"[{layer.strip()}] Failed run_id={run_id}, table_id={table_id}: {truncated_error}"
        )
