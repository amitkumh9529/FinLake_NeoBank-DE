# Databricks notebook source
# MAGIC %md
# MAGIC # FinLake: Phase 0 - Metadata Registry Validation & Bootstrap
# MAGIC Ensures Unity Catalog schemas and Delta control tables are initialized and validates
# MAGIC active metadata records via Pydantic v2 before ingestion begins.

# COMMAND ----------

from finlake.config.bootstrap import MetadataBootstrapper
from finlake.config.metadata_loader import MetadataRegistry
from finlake.config.settings import EnvironmentSettings
from finlake.utils.logger import get_logger

logger = get_logger("finlake.notebook.bootstrap")
env = EnvironmentSettings()

dbutils.widgets.text("catalog_name", env.finlake_catalog or "finlake_dev")
catalog_name = dbutils.widgets.get("catalog_name")

bootstrapper = MetadataBootstrapper(spark, catalog=catalog_name)
bootstrapper.initialize_schemas(create_unity_catalog=True)
bootstrapper.initialize_metadata_tables()

registry = MetadataRegistry(spark, catalog=catalog_name)
active_tables = registry.get_active_tables(target_layer="silver")

source_systems = sorted({t.source_system for t in active_tables})
dbutils.jobs.taskValues.set(key="active_table_count", value=len(active_tables))
dbutils.jobs.taskValues.set(key="active_source_systems", value=source_systems)

logger.info(
    f"Bootstrap verified for catalog={catalog_name}: "
    f"{len(active_tables)} active Silver entities across {source_systems}."
)
