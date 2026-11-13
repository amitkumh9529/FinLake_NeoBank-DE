from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from finlake.config.bootstrap import MetadataBootstrapper
from finlake.utils.audit import AuditLogger
from finlake.utils.governance import (
    MASK_EMAIL_SQL_EXPR,
    MASK_PAN_SQL_EXPR,
    MASK_PHONE_SQL_EXPR,
    UnityCatalogGovernance,
)
from finlake.utils.notifier import PipelineNotifier
from tests.conftest import reset_local_schemas

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_SQL_PATH = PROJECT_ROOT / "sql" / "02_seed_metadata.sql"
WORKFLOW_YML_PATH = PROJECT_ROOT / "resources" / "finlake_workflow.yml"


@pytest.fixture(scope="module")
def gov_env(spark, tmp_path_factory):
    """Bootstraps schemas, partitioned pipeline_runs, and sample Silver/Gold Delta tables."""
    reset_local_schemas(spark)
    schema_root = tmp_path_factory.mktemp("finlake_gov_meta")
    bootstrapper = MetadataBootstrapper(spark, catalog=None)
    bootstrapper.initialize_schemas(create_unity_catalog=False, location_root=schema_root)
    bootstrapper.initialize_metadata_tables()
    bootstrapper.seed_from_sql_file(SEED_SQL_PATH)

    spark.createDataFrame(
        [(1, "ABCDE1234F", "alice@finlake.bank", "9876543210")],
        ["customer_id", "pan_number", "email", "phone_number"],
    ).write.format("delta").mode("overwrite").saveAsTable("silver.customers")

    spark.createDataFrame(
        [(1, "ABCDE1234F", "alice@finlake.bank", "9876543210")],
        ["customer_id", "pan_number", "email", "phone_number"],
    ).write.format("delta").mode("overwrite").saveAsTable("gold.dim_customer_360")

    spark.createDataFrame(
        [("2025-01-01", 10)],
        ["metric_date", "daily_txn_count"],
    ).write.format("delta").mode("overwrite").saveAsTable("gold.fct_daily_bank_kpis")

    return bootstrapper


def test_pipeline_runs_is_partitioned_by_table_id(spark, gov_env):
    """Verifies pipeline_runs uses table_id partitioning for parallel ingestion safety."""
    detail = spark.sql("DESCRIBE DETAIL metadata.pipeline_runs").first()
    assert detail["format"] == "delta"
    assert list(detail["partitionColumns"]) == ["table_id"]


def test_notifier_html_reports_all_states_and_prevents_xss(spark, gov_env):
    """Validates SUCCESS, DLQ warning, FAILED (with XSS escaping), and 0-row failure guard."""
    audit = AuditLogger(spark, catalog=None)
    notifier = PipelineNotifier(spark, catalog=None)

    # 1. Healthy Run (8001)
    audit.log_start(8001, 1, "Bronze")
    audit.log_success(8001, 1, "Bronze", records_read=500, records_written=500)
    audit.log_start(8001, 7, "Gold")
    audit.log_success(8001, 7, "Gold", records_read=500, records_written=500)

    status_ok, kpis_ok, _ = notifier.collect_run_metrics(8001)
    subj_ok, html_ok = notifier.generate_html_summary(8001, environment="prod")
    assert status_ok == "SUCCESS"
    assert subj_ok == "[PROD] FinLake Pipeline [SUCCESS] - Run ID 8001"
    assert kpis_ok["total_written"] == 1000
    assert "customers" in html_ok

    # 2. DLQ Warning Run (8002)
    audit.log_start(8002, 1, "Silver")
    audit.log_success(
        8002, 1, "Silver", records_read=100, records_written=95, records_quarantined=5
    )
    status_warn, kpis_warn, _ = notifier.collect_run_metrics(8002)
    subj_warn, html_warn = notifier.generate_html_summary(8002, environment="stage")
    assert status_warn == "SUCCESS_WITH_WARNINGS"
    assert "SUCCESS_WITH_WARNINGS" in subj_warn
    assert "Data Quality Quarantine (DLQ) Notice" in html_warn
    assert "quarantine.customers" in html_warn

    # 3. Failed Run with HTML/XSS injection payload (8003)
    audit.log_start(8003, 2, "Silver")
    audit.log_failure(8003, 2, "Silver", "<script>alert('xss')</script> failed")
    status_fail, _, _ = notifier.collect_run_metrics(8003)
    subj_fail, html_fail = notifier.generate_html_summary(8003, environment="dev")
    assert status_fail == "FAILED"
    assert "[DEV] FinLake Pipeline [FAILED]" in subj_fail
    assert "<script>" not in html_fail
    assert "&lt;script&gt;" in html_fail

    # 4. Zero-row early failure guard (8999)
    status_zero, kpis_zero, _ = notifier.collect_run_metrics(8999)
    _, html_zero = notifier.generate_html_summary(8999)
    assert status_zero == "FAILED"
    assert kpis_zero["total_steps"] == 0
    assert "Zero pipeline execution steps were logged" in html_zero


def test_notifier_smtp_email_dispatch_with_mock(spark, gov_env):
    """Validates TLS SMTP email dispatch and recipient validation."""
    notifier = PipelineNotifier(spark, catalog=None)

    with pytest.raises(ValueError, match="At least one recipient"):
        notifier.send_email("Subj", "<p>Body</p>", "smtp.test", 587, "a@b.com", "pw", [])

    mock_smtp_cls = MagicMock()
    mock_server = mock_smtp_cls.return_value.__enter__.return_value

    with patch("finlake.utils.notifier.smtplib.SMTP", mock_smtp_cls):
        notifier.send_email(
            subject="FinLake Alert",
            html_body="<h1>OK</h1>",
            smtp_host="smtp.finlake.internal",
            smtp_port=587,
            sender_email="alerts@finlake.internal",
            sender_password="app-password",
            recipients=["oncall@finlake.internal"],
        )

    mock_smtp_cls.assert_called_once_with("smtp.finlake.internal", 587, timeout=30)
    mock_server.starttls.assert_called_once()
    mock_server.login.assert_called_once_with("alerts@finlake.internal", "app-password")
    mock_server.send_message.assert_called_once()


def test_pii_masking_expressions_are_ansi_and_null_safe(spark):
    """Proves PII masking SQL expressions work under strict ANSI mode with NULL/malformed values."""
    prev_ansi = spark.conf.get("spark.sql.ansi.enabled", "false")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        df = spark.createDataFrame(
            [
                ("ABCDE1234F", "alice@finlake.bank", "9876543210"),
                (None, "corrupted_email_no_at", None),
                ("SHORT", None, "1234"),
            ],
            ["pan_number", "email", "phone_number"],
        )
        masked = df.selectExpr(
            f"{MASK_PAN_SQL_EXPR} AS m_pan",
            f"{MASK_EMAIL_SQL_EXPR} AS m_email",
            f"{MASK_PHONE_SQL_EXPR} AS m_phone",
        ).collect()

        assert masked[0]["m_pan"] == "XXXXXX234F"
        assert masked[0]["m_email"] == "***@finlake.bank"
        assert masked[0]["m_phone"] == "XXXXXX3210"

        assert masked[1]["m_pan"] is None
        assert masked[1]["m_email"] == "***@masked"
        assert masked[1]["m_phone"] is None

        assert masked[2]["m_email"] is None
    finally:
        spark.conf.set("spark.sql.ansi.enabled", prev_ansi)


def test_unity_governance_statements_and_delta_vacuum(spark, gov_env):
    """Verifies generated UC governance DDL and executes real Delta VACUUM maintenance."""
    gov_local = UnityCatalogGovernance(spark, catalog=None)
    assert gov_local.apply_unity_catalog_governance() == 0

    stmts = gov_local.build_governance_statements()
    # 3 UDFs + 4 silver.customers + 4 gold.dim_customer_360 + 1 gold.fct_daily_bank_kpis = 12
    assert len(stmts) == 12
    assert any("SET MASK" in s for s in stmts)
    assert any("SET TAGS" in s for s in stmts)

    with pytest.raises(ValueError, match="retention_hours must be >= 168"):
        gov_local.run_retention_maintenance(retention_hours=24)

    vacuumed = gov_local.run_retention_maintenance(retention_hours=168, schemas=["silver", "gold"])
    assert vacuumed == 3


def test_databricks_workflow_dag_and_notebooks_valid():
    """Validates workflow YAML DAG structure, SLA health checks, and notebook paths on disk."""
    cfg = yaml.safe_load(WORKFLOW_YML_PATH.read_text(encoding="utf-8"))
    job = cfg["resources"]["jobs"]["finlake_daily_pipeline"]

    assert job["max_concurrent_runs"] == 1
    assert job["health"]["rules"][0]["metric"] == "RUN_DURATION_SECONDS"

    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert set(tasks.keys()) == {
        "bootstrap_metadata",
        "ingest_sqlserver",
        "ingest_blob_autoloader",
        "bronze_to_silver",
        "silver_to_gold",
        "apply_unity_governance",
        "pipeline_audit_summary",
    }

    # Verify parallel ingestion fan-out and fan-in
    assert tasks["ingest_sqlserver"]["depends_on"] == [{"task_key": "bootstrap_metadata"}]
    assert tasks["ingest_blob_autoloader"]["depends_on"] == [{"task_key": "bootstrap_metadata"}]
    assert {d["task_key"] for d in tasks["bronze_to_silver"]["depends_on"]} == {
        "ingest_sqlserver",
        "ingest_blob_autoloader",
    }

    # Verify final alerting runs on ALL_DONE
    assert tasks["pipeline_audit_summary"]["run_if"] == "ALL_DONE"

    # Verify every notebook path referenced in YAML exists in notebooks/
    resources_dir = WORKFLOW_YML_PATH.parent
    for t in job["tasks"]:
        nb_rel = t["notebook_task"]["notebook_path"]
        assert (resources_dir / nb_rel).resolve().exists(), f"Missing notebook: {nb_rel}"
