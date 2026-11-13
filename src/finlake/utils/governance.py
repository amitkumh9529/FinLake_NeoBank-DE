from typing import List, Optional

from pyspark.sql import SparkSession

from finlake.config.settings import qualify_schema_name, qualify_table_name
from finlake.utils.logger import get_logger

logger = get_logger("finlake.governance")

MASK_PAN_SQL_EXPR = (
    "CASE WHEN pan_number IS NULL THEN NULL ELSE CONCAT('XXXXXX', RIGHT(pan_number, 4)) END"
)
MASK_EMAIL_SQL_EXPR = (
    "CASE WHEN email IS NULL THEN NULL "
    "WHEN NOT CONTAINS(email, '@') THEN '***@masked' "
    "ELSE CONCAT('***@', ELEMENT_AT(SPLIT(email, '@'), 2)) END"
)
MASK_PHONE_SQL_EXPR = (
    "CASE WHEN phone_number IS NULL THEN NULL ELSE CONCAT('XXXXXX', RIGHT(phone_number, 4)) END"
)


class UnityCatalogGovernance:
    """Applies Unity Catalog PII masking, tagging, RBAC grants, and Delta retention cleanup."""

    def __init__(self, spark: SparkSession, catalog: Optional[str] = "finlake"):
        self.spark = spark
        self.catalog = catalog.strip() if catalog and catalog.strip() else None

    def build_governance_statements(
        self,
        analyst_group: str = "bi_analysts",
        compliance_group: str = "compliance_team",
    ) -> List[str]:
        """Generates parameterized Unity Catalog UDF, column-masking, tagging, and RBAC SQL."""
        meta = qualify_schema_name(self.catalog, "metadata")
        silver_cust = qualify_table_name(self.catalog, "silver", "customers")
        gold_c360 = qualify_table_name(self.catalog, "gold", "dim_customer_360")
        gold_kpi = qualify_table_name(self.catalog, "gold", "fct_daily_bank_kpis")
        gold_schema = qualify_schema_name(self.catalog, "gold")

        statements = [
            f"""
            CREATE OR REPLACE FUNCTION {meta}.mask_pan(pan_number STRING)
            RETURNS STRING
            RETURN CASE
                WHEN is_account_group_member('{compliance_group}') THEN pan_number
                WHEN pan_number IS NULL THEN NULL
                ELSE CONCAT('XXXXXX', RIGHT(pan_number, 4))
            END
            """,
            f"""
            CREATE OR REPLACE FUNCTION {meta}.mask_email(email STRING)
            RETURNS STRING
            RETURN CASE
                WHEN is_account_group_member('{compliance_group}') THEN email
                WHEN email IS NULL THEN NULL
                WHEN NOT CONTAINS(email, '@') THEN '***@masked'
                ELSE CONCAT('***@', ELEMENT_AT(SPLIT(email, '@'), 2))
            END
            """,
            f"""
            CREATE OR REPLACE FUNCTION {meta}.mask_phone(phone_number STRING)
            RETURNS STRING
            RETURN CASE
                WHEN is_account_group_member('{compliance_group}') THEN phone_number
                WHEN phone_number IS NULL THEN NULL
                ELSE CONCAT('XXXXXX', RIGHT(phone_number, 4))
            END
            """,
        ]

        if self.spark.catalog.tableExists(silver_cust):
            statements.extend(
                [
                    f"ALTER TABLE {silver_cust} ALTER COLUMN pan_number SET MASK {meta}.mask_pan",
                    f"ALTER TABLE {silver_cust} ALTER COLUMN email SET MASK {meta}.mask_email",
                    f"ALTER TABLE {silver_cust} ALTER COLUMN phone_number SET MASK {meta}.mask_phone",
                    f"ALTER TABLE {silver_cust} SET TAGS ('pii' = 'true', 'data_classification' = 'confidential')",
                ]
            )

        if self.spark.catalog.tableExists(gold_c360):
            statements.extend(
                [
                    f"ALTER TABLE {gold_c360} ALTER COLUMN pan_number SET MASK {meta}.mask_pan",
                    f"ALTER TABLE {gold_c360} ALTER COLUMN email SET MASK {meta}.mask_email",
                    f"ALTER TABLE {gold_c360} ALTER COLUMN phone_number SET MASK {meta}.mask_phone",
                    f"ALTER TABLE {gold_c360} SET TAGS ('pii' = 'masked', 'domain' = 'customer_analytics')",
                ]
            )

        if self.spark.catalog.tableExists(gold_kpi):
            statements.append(f"ALTER TABLE {gold_kpi} SET TAGS ('domain' = 'executive_kpi')")

        if self.catalog:
            statements.extend(
                [
                    f"GRANT USE CATALOG ON CATALOG {self.catalog} TO `{analyst_group}`",
                    f"GRANT USE SCHEMA ON SCHEMA {gold_schema} TO `{analyst_group}`",
                    f"GRANT SELECT ON SCHEMA {gold_schema} TO `{analyst_group}`",
                ]
            )
        return statements

    def apply_unity_catalog_governance(
        self,
        analyst_group: str = "bi_analysts",
        compliance_group: str = "compliance_team",
    ) -> int:
        """Executes Unity Catalog governance statements when running under Unity Catalog."""
        if not self.catalog:
            logger.info("Skipping Unity Catalog SQL grants/masks in local spark_catalog mode.")
            return 0

        statements = self.build_governance_statements(
            analyst_group=analyst_group,
            compliance_group=compliance_group,
        )
        executed = 0
        for stmt in statements:
            self.spark.sql(stmt)
            executed += 1
        logger.info(f"Applied {executed} Unity Catalog governance rules under {self.catalog}.")
        return executed

    def run_retention_maintenance(
        self,
        retention_hours: int = 168,
        schemas: Optional[List[str]] = None,
    ) -> int:
        """Executes Delta VACUUM across Medallion schemas to reclaim stale file storage."""
        if retention_hours < 168:
            raise ValueError(
                "retention_hours must be >= 168 (7 days) to protect Delta Lake Time Travel."
            )

        target_schemas = schemas or ["bronze", "silver", "gold", "quarantine"]
        vacuumed = 0

        for schema_name in target_schemas:
            fq_schema = qualify_schema_name(self.catalog, schema_name)
            try:
                tables = self.spark.catalog.listTables(fq_schema)
            except Exception:
                continue

            for tbl in tables:
                if tbl.isTemporary:
                    continue
                fq_tbl = qualify_table_name(self.catalog, schema_name, tbl.name)
                self.spark.sql(f"VACUUM {fq_tbl} RETAIN {int(retention_hours)} HOURS")
                vacuumed += 1

        logger.info(
            f"Completed Delta VACUUM maintenance on {vacuumed} tables (retain={retention_hours}h)."
        )
        return vacuumed
