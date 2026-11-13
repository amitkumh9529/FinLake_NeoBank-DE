from typing import Dict, Optional

from pyspark.sql import SparkSession

from finlake.config.settings import qualify_table_name
from finlake.utils.logger import get_logger

logger = get_logger("finlake.watermark")


class WatermarkManager:
    """Manages monotonic incremental high-watermarks in metadata.table_watermarks."""

    def __init__(self, spark: SparkSession, catalog: Optional[str] = "finlake"):
        self.spark = spark
        self.catalog = catalog.strip() if catalog and catalog.strip() else None
        self.watermark_table = qualify_table_name(self.catalog, "metadata", "table_watermarks")

    def get_watermark(self, table_id: int) -> Optional[str]:
        """Retrieves the last persisted high-watermark value for a given table_id."""
        df = self.spark.sql(
            f"SELECT last_watermark_value FROM {self.watermark_table} WHERE table_id = :table_id",
            args={"table_id": int(table_id)},
        )
        rows = df.limit(1).collect()
        if not rows:
            return None
        return rows[0]["last_watermark_value"]

    def get_all_watermarks(self) -> Dict[int, str]:
        """Batch-loads all watermarks in a single query."""
        rows = self.spark.sql(
            f"SELECT table_id, last_watermark_value FROM {self.watermark_table}"
        ).collect()
        return {row.table_id: row.last_watermark_value for row in rows}

    def update_watermark(self, table_id: int, new_watermark: str, run_id: int) -> bool:
        """Atomically updates watermark via parameterized Delta MERGE, preventing regression."""
        if not new_watermark or not str(new_watermark).strip():
            return False

        clean_val = str(new_watermark).strip()
        current = self.get_watermark(table_id)
        if current is not None and clean_val <= current:
            logger.info(
                f"Skipped watermark update for table_id={table_id}: "
                f"candidate '{clean_val}' <= current '{current}'"
            )
            return False

        query = f"""
            MERGE INTO {self.watermark_table} t
            USING (
                SELECT :table_id AS table_id, :new_val AS new_val, :run_id AS run_id
            ) s
            ON t.table_id = s.table_id
            WHEN MATCHED AND s.new_val > t.last_watermark_value THEN UPDATE SET
                t.last_watermark_value = s.new_val,
                t.last_updated_at = current_timestamp(),
                t.last_run_id = s.run_id
            WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_updated_at, last_run_id)
                VALUES (s.table_id, s.new_val, current_timestamp(), s.run_id)
        """
        self.spark.sql(
            query,
            args={
                "table_id": int(table_id),
                "new_val": clean_val,
                "run_id": int(run_id),
            },
        )
        logger.info(f"Updated watermark for table_id={table_id} -> '{clean_val}' (run_id={run_id})")
        return True
