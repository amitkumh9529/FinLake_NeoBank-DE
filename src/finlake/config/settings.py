from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def qualify_table_name(catalog: Optional[str], schema: str, table: str) -> str:
    """Formats a 3-part Unity Catalog FQN or 2-part local Spark table name."""
    if catalog and catalog.strip():
        return f"{catalog.strip()}.{schema}.{table}"
    return f"{schema}.{table}"


def qualify_schema_name(catalog: Optional[str], schema: str) -> str:
    """Formats a 2-part Unity Catalog schema or 1-part local database name."""
    if catalog and catalog.strip():
        return f"{catalog.strip()}.{schema}"
    return schema


class EnvironmentSettings(BaseSettings):
    """Application-level configuration loaded from environment variables or .env file."""

    finlake_env: Literal["dev", "stage", "prod"] = "dev"
    finlake_catalog: Optional[str] = "finlake_dev"
    secret_scope: str = "finlake-scope"

    source_volume_path: str = "/Volumes/finlake/source/volume"
    checkpoint_base_path: str = "/Volumes/finlake/source/volume/_checkpoints"
    schema_base_path: str = "/Volumes/finlake/source/volume/_schema"

    smtp_host: str = "smtp.gmail.com"
    smtp_port: int = 587
    alert_sender_email: str = "alerts@finlake.internal"
    alert_recipient_email: str = "data-eng@finlake.internal"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class DatabaseConfig(BaseModel):
    """Validated JDBC connection settings for Core Banking RDBMS."""

    host: str = Field(..., min_length=1)
    port: int = Field(default=1433, ge=1, le=65535)
    database: str = Field(..., min_length=1)
    user: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)
    driver: str = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    url_override: Optional[str] = None

    @property
    def jdbc_url(self) -> str:
        if self.url_override:
            return self.url_override
        return f"jdbc:sqlserver://{self.host}:{self.port};database={self.database}"


class DQRule(BaseModel):
    """Validated Data Quality expectation rule definition."""

    rule_id: int
    table_id: int
    rule_name: str = Field(..., min_length=1)
    rule_expression: str = Field(..., min_length=1)
    severity: Literal["CRITICAL", "WARNING"] = "CRITICAL"
    active_flag: bool = True


class TableConfig(BaseModel):
    """Validated metadata specification for an ingested table."""

    table_id: int
    table_name: str = Field(..., min_length=1)
    source_system: Literal["sqlserver", "blob", "silver"]
    source_schema: Optional[str] = None
    source_table: Optional[str] = None
    source_path: Optional[str] = None
    target_layer: Literal["bronze", "silver", "gold"]
    bronze_schema: Optional[str] = "bronze"
    silver_schema: Optional[str] = "silver"
    gold_schema: Optional[str] = "gold"
    quarantine_schema: str = "quarantine"
    load_type: Literal["FULL", "APPEND", "MERGE"] = "MERGE"
    primary_key: Optional[str] = None
    watermark_column: Optional[str] = None
    partition_column: Optional[str] = None
    num_partitions: int = Field(default=4, ge=1, le=64)
    load_order: int = Field(default=100, ge=1)
    active_flag: bool = True

    @field_validator("primary_key")
    @classmethod
    def validate_primary_key_for_merge(cls, v: Optional[str], info) -> Optional[str]:
        if info.data.get("load_type") == "MERGE" and not v:
            raise ValueError("primary_key is mandatory when load_type is MERGE")
        return v

    @model_validator(mode="after")
    def validate_source_coordinates(self) -> "TableConfig":
        if self.source_system == "sqlserver":
            if not self.source_schema or not self.source_table:
                raise ValueError("source_schema and source_table are required for sqlserver source")
        elif self.source_system == "blob":
            if not self.source_path:
                raise ValueError("source_path is required for blob source")
        return self
