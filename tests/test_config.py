import pytest
from pydantic import ValidationError

from finlake.config.settings import (
    DatabaseConfig,
    EnvironmentSettings,
    TableConfig,
    qualify_schema_name,
    qualify_table_name,
)


def test_environment_settings_defaults():
    env = EnvironmentSettings()
    assert env.finlake_env == "dev"
    assert env.finlake_catalog == "finlake_dev"
    assert env.smtp_port == 587


def test_database_config_jdbc_url():
    db = DatabaseConfig(
        host="sql.finlake.internal",
        port=1433,
        database="banking",
        user="admin",
        password="secret",
    )
    assert db.jdbc_url == "jdbc:sqlserver://sql.finlake.internal:1433;database=banking"


def test_table_config_requires_pk_for_merge():
    with pytest.raises(ValidationError) as exc_info:
        TableConfig(
            table_id=1,
            table_name="customers",
            source_system="sqlserver",
            source_schema="banking",
            source_table="customers",
            target_layer="silver",
            load_type="MERGE",
            primary_key=None,
        )
    assert "primary_key is mandatory" in str(exc_info.value)


def test_table_config_allows_no_pk_for_full():
    cfg = TableConfig(
        table_id=4,
        table_name="branches",
        source_system="sqlserver",
        source_schema="banking",
        source_table="branches",
        target_layer="silver",
        load_type="FULL",
        primary_key=None,
    )
    assert cfg.table_name == "branches"
    assert cfg.num_partitions == 4


def test_table_config_validates_source_coordinates():
    with pytest.raises(ValidationError) as exc_sql:
        TableConfig(
            table_id=1,
            table_name="customers",
            source_system="sqlserver",
            target_layer="silver",
            load_type="FULL",
        )
    assert "source_schema and source_table are required" in str(exc_sql.value)

    with pytest.raises(ValidationError) as exc_blob:
        TableConfig(
            table_id=5,
            table_name="credit_bureau_reports",
            source_system="blob",
            target_layer="silver",
            load_type="APPEND",
        )
    assert "source_path is required" in str(exc_blob.value)


def test_qualify_names():
    assert qualify_table_name("finlake", "silver", "customers") == "finlake.silver.customers"
    assert qualify_table_name(None, "silver", "customers") == "silver.customers"
    assert qualify_table_name("", "silver", "customers") == "silver.customers"
    assert qualify_schema_name("finlake", "metadata") == "finlake.metadata"
    assert qualify_schema_name(None, "metadata") == "metadata"
