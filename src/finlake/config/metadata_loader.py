from typing import Dict, List, Optional

from pyspark.sql import SparkSession

from finlake.config.settings import DQRule, TableConfig, qualify_table_name
from finlake.utils.logger import get_logger

logger = get_logger("finlake.metadata")


class MetadataRegistry:
    """Single-query metadata loader hydrated and validated via Pydantic v2 models."""

    def __init__(self, spark: SparkSession, catalog: Optional[str] = "finlake"):
        self.spark = spark
        self.catalog = catalog.strip() if catalog and catalog.strip() else None

    def _tbl(self, table: str) -> str:
        return qualify_table_name(self.catalog, "metadata", table)

    def get_active_tables(
        self,
        source_system: Optional[str] = None,
        target_layer: Optional[str] = "silver",
    ) -> List[TableConfig]:
        """Loads active tables joined with parameters in a single Spark query (no N+1)."""
        filters = ["t.active_flag = TRUE"]
        args = {}
        if source_system:
            filters.append("lower(t.source_system) = lower(:source_system)")
            args["source_system"] = source_system.strip()
        if target_layer:
            filters.append("lower(t.target_layer) = lower(:target_layer)")
            args["target_layer"] = target_layer.strip()

        where_clause = " AND ".join(filters)

        query = f"""
            SELECT
                t.table_id,
                t.table_name,
                t.source_system,
                t.source_schema,
                t.source_table,
                t.source_path,
                t.target_layer,
                t.bronze_schema,
                t.silver_schema,
                t.gold_schema,
                t.active_flag,
                t.load_order,
                p.params
            FROM {self._tbl("tables")} t
            LEFT JOIN (
                SELECT
                    table_id,
                    map_from_entries(collect_list(struct(parameter_name, parameter_value))) AS params
                FROM {self._tbl("table_parameters")}
                GROUP BY table_id
            ) p ON t.table_id = p.table_id
            WHERE {where_clause}
            ORDER BY t.load_order, t.table_id
        """

        rows = self.spark.sql(query, args=args).collect()
        configs: List[TableConfig] = []

        for row in rows:
            params = row.params if row.params is not None else {}
            tgt_layer = row.target_layer.lower()
            default_load_type = "FULL" if tgt_layer == "gold" else "MERGE"

            cfg = TableConfig(
                table_id=row.table_id,
                table_name=row.table_name,
                source_system=row.source_system.lower(),
                source_schema=row.source_schema,
                source_table=row.source_table,
                source_path=row.source_path,
                target_layer=tgt_layer,
                bronze_schema=row.bronze_schema or "bronze",
                silver_schema=row.silver_schema or "silver",
                gold_schema=row.gold_schema or "gold",
                load_type=params.get("load_type", default_load_type),
                primary_key=params.get("primary_key"),
                watermark_column=params.get("watermark_column"),
                partition_column=params.get("partition_column"),
                num_partitions=int(params.get("num_partitions", 4)),
                load_order=row.load_order,
                active_flag=row.active_flag,
            )
            configs.append(cfg)

        logger.info(
            f"Loaded {len(configs)} active tables (source_system={source_system}, target_layer={target_layer})"
        )
        return configs

    def get_dq_rules(self, table_id: int) -> List[DQRule]:
        """Loads active Data Quality rules for a specific table_id."""
        query = f"""
            SELECT rule_id, table_id, rule_name, rule_expression, severity, active_flag
            FROM {self._tbl("data_quality_rules")}
            WHERE table_id = :table_id AND active_flag = TRUE
            ORDER BY rule_id
        """
        rows = self.spark.sql(query, args={"table_id": int(table_id)}).collect()
        return [
            DQRule(
                rule_id=row.rule_id,
                table_id=row.table_id,
                rule_name=row.rule_name,
                rule_expression=row.rule_expression,
                severity=row.severity,
                active_flag=row.active_flag,
            )
            for row in rows
        ]

    def get_all_dq_rules(self) -> Dict[int, List[DQRule]]:
        """Batch-loads all active Data Quality rules grouped by table_id in a single query."""
        query = f"""
            SELECT rule_id, table_id, rule_name, rule_expression, severity, active_flag
            FROM {self._tbl("data_quality_rules")}
            WHERE active_flag = TRUE
            ORDER BY table_id, rule_id
        """
        rows = self.spark.sql(query).collect()
        grouped: Dict[int, List[DQRule]] = {}
        for row in rows:
            rule = DQRule(
                rule_id=row.rule_id,
                table_id=row.table_id,
                rule_name=row.rule_name,
                rule_expression=row.rule_expression,
                severity=row.severity,
                active_flag=row.active_flag,
            )
            grouped.setdefault(row.table_id, []).append(rule)
        return grouped
