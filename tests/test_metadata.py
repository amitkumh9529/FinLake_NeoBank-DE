from pathlib import Path

import pytest

from finlake.config.bootstrap import MetadataBootstrapper
from finlake.config.metadata_loader import MetadataRegistry
from finlake.quality.validator import DataQualityValidator
from finlake.utils.audit import AuditLogger
from finlake.utils.watermark import WatermarkManager
from tests.conftest import reset_local_schemas

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_SQL_PATH = PROJECT_ROOT / "sql" / "02_seed_metadata.sql"


@pytest.fixture(scope="module")
def metadata_env(spark, tmp_path_factory):
    """Bootstraps real Delta Lake metadata tables in a temporary warehouse."""
    reset_local_schemas(spark)
    schema_root = tmp_path_factory.mktemp("finlake_meta")
    bootstrapper = MetadataBootstrapper(spark, catalog=None)
    bootstrapper.initialize_schemas(create_unity_catalog=False, location_root=schema_root)
    bootstrapper.initialize_metadata_tables()
    bootstrapper.seed_from_sql_file(SEED_SQL_PATH)
    return bootstrapper


def test_seed_metadata_idempotency(spark, metadata_env):
    """Verifies re-running seed SQL preserves live watermarks and prevents duplicates."""
    wm = WatermarkManager(spark, catalog=None)
    assert wm.get_watermark(1) == "1900-01-01 00:00:00"

    # Simulate production progress on customer watermark
    assert wm.update_watermark(1, "2025-06-15 08:30:00", run_id=5001) is True

    # Re-run seed SQL (simulating a CI/CD deployment)
    metadata_env.seed_from_sql_file(SEED_SQL_PATH)

    # Watermark MUST NOT be reset back to 1900-01-01
    assert wm.get_watermark(1) == "2025-06-15 08:30:00"
    assert spark.table("metadata.tables").count() == 11
    assert spark.table("metadata.data_quality_rules").count() == 9


def test_metadata_registry_loads_and_hydrates_tables(spark, metadata_env):
    """Verifies single-query hydration, load_order sorting, and JDBC partition config."""
    registry = MetadataRegistry(spark, catalog=None)

    silver_tables = registry.get_active_tables(target_layer="silver")
    assert len(silver_tables) == 6
    assert [t.table_name for t in silver_tables] == [
        "customers",
        "accounts",
        "transactions",
        "branches",
        "credit_bureau_reports",
        "payment_gateway_logs",
    ]

    # Verify JDBC parallel partition config on transactions
    txn_cfg = next(t for t in silver_tables if t.table_name == "transactions")
    assert txn_cfg.load_type == "APPEND"
    assert txn_cfg.partition_column == "txn_id"
    assert txn_cfg.num_partitions == 8

    # Verify source_system filter
    sql_tables = registry.get_active_tables(source_system="sqlserver", target_layer="silver")
    assert len(sql_tables) == 4
    blob_tables = registry.get_active_tables(source_system="blob", target_layer="silver")
    assert len(blob_tables) == 2

    # Verify Gold targets hydrate cleanly without Pydantic ValidationError
    gold_tables = registry.get_active_tables(target_layer="gold")
    assert len(gold_tables) == 5
    assert all(t.load_type == "FULL" for t in gold_tables)


def test_seeded_dq_rules_execute_validly_in_spark(spark, metadata_env):
    """Validates every seeded SQL rule expression executes against real Spark DataFrames."""
    registry = MetadataRegistry(spark, catalog=None)
    rules_by_table = registry.get_all_dq_rules()
    cust_rules_single = registry.get_dq_rules(1)

    assert len(cust_rules_single) == 3
    assert set(rules_by_table.keys()) == {1, 2, 3, 5, 6}

    # Test Table 1 (customers) rule expressions including BADPAN anomaly & WARNING email
    cust_df = spark.createDataFrame(
        [
            (1001, "ABCDE1234F", "alice@finlake.bank", "VERIFIED"),
            (1002, "BADPAN1", "bob@finlake.bank", "PENDING"),
            (1003, "ABCDE1234G", "corrupted_email", "PENDING"),
        ],
        ["customer_id", "pan_number", "email", "kyc_status"],
    )
    cust_validator = DataQualityValidator(rules_by_table[1])
    valid_cust, bad_cust = cust_validator.validate_and_split(cust_df)
    assert valid_cust.count() == 2
    assert bad_cust.count() == 1
    reasons = bad_cust.first()["quarantine_reasons"]
    assert "FAIL_valid_pan_format" in reasons
    warn_row = valid_cust.filter("customer_id = 1003").first()
    assert "WARN_valid_email_format" in warn_row["_dq_warnings"]

    # Test Table 2 (accounts) negative balance & currency rules
    acc_df = spark.createDataFrame(
        [
            (500001, 2500.0, "INR"),
            (500002, -120.5, "INR"),
        ],
        ["account_id", "balance", "currency"],
    )
    valid_acc, bad_acc = DataQualityValidator(rules_by_table[2]).validate_and_split(acc_df)
    assert valid_acc.count() == 1
    assert bad_acc.count() == 1

    # Test Table 5 (credit_bureau_reports) & Table 6 (payment_gateway_logs) rules
    bureau_df = spark.createDataFrame([(1001, 720), (1002, 950)], ["customer_id", "credit_score"])
    valid_b, bad_b = DataQualityValidator(rules_by_table[5]).validate_and_split(bureau_df)
    assert valid_b.count() == 1 and bad_b.count() == 1

    gw_df = spark.createDataFrame([(30001, 150), (30002, 0)], ["txn_id", "processing_time_ms"])
    valid_g, bad_g = DataQualityValidator(rules_by_table[6]).validate_and_split(gw_df)
    assert valid_g.count() == 1 and bad_g.count() == 1


def test_watermark_monotonicity_and_edge_cases(spark, metadata_env):
    """Ensures older/empty watermarks cannot overwrite newer watermarks."""
    wm = WatermarkManager(spark, catalog=None)
    assert wm.get_watermark(999) is None
    assert wm.update_watermark(2, "", run_id=6000) is False

    assert wm.update_watermark(2, "2025-07-01 10:00:00", run_id=6001) is True
    assert wm.get_watermark(2) == "2025-07-01 10:00:00"

    # Attempt regression to earlier timestamp
    assert wm.update_watermark(2, "2024-01-01 00:00:00", run_id=6002) is False
    assert wm.get_watermark(2) == "2025-07-01 10:00:00"

    all_wm = wm.get_all_watermarks()
    assert len(all_wm) == 5


def test_audit_logger_idempotent_retry_and_layer_isolation(spark, metadata_env):
    """Verifies task retry idempotency, parameterized quote safety, and Bronze/Silver isolation."""
    audit = AuditLogger(spark, catalog=None)

    # Start Bronze and simulate retry (second log_start for same run_id, table_id, layer)
    audit.log_start(run_id=9001, table_id=1, layer="Bronze")
    audit.log_start(run_id=9001, table_id=1, layer="Bronze")
    audit.log_success(
        run_id=9001, table_id=1, layer="Bronze", records_read=100, records_written=100
    )

    # Start Silver on same run_id + table_id and fail with apostrophe in error
    audit.log_start(run_id=9001, table_id=1, layer="Silver")
    audit.log_failure(
        run_id=9001,
        table_id=1,
        layer="Silver",
        error_message="O'Connor constraint failed on pan_number",
    )

    runs_df = spark.table("metadata.pipeline_runs").filter("run_id = 9001 AND table_id = 1")
    rows = {r.layer: r for r in runs_df.collect()}

    assert len(rows) == 2  # No duplicate Bronze row from retry!
    assert rows["Bronze"].status == "SUCCESS"
    assert rows["Silver"].status == "FAILED"
    assert rows["Silver"].error_message == "O'Connor constraint failed on pan_number"
