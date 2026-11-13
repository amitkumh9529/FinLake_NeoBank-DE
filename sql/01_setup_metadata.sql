-- =============================================================================
-- FinLake Unity Catalog & Metadata Registry Setup
-- Enables Delta Auto-Optimize & isolates Quarantine (DLQ) governance schema
-- =============================================================================

CREATE CATALOG IF NOT EXISTS finlake;

CREATE SCHEMA IF NOT EXISTS finlake.metadata;
CREATE SCHEMA IF NOT EXISTS finlake.source;
CREATE SCHEMA IF NOT EXISTS finlake.bronze;
CREATE SCHEMA IF NOT EXISTS finlake.silver;
CREATE SCHEMA IF NOT EXISTS finlake.gold;
CREATE SCHEMA IF NOT EXISTS finlake.quarantine;

-- Landing volume for external partner feeds (Credit Bureau & Payment Gateways)
CREATE VOLUME IF NOT EXISTS finlake.source.volume;

-- 1. Logical Table Registry
CREATE TABLE IF NOT EXISTS finlake.metadata.tables (
    table_id            INT NOT NULL,
    table_name          STRING NOT NULL,
    source_system       STRING NOT NULL,   -- 'sqlserver' / 'blob' / 'silver'
    source_schema       STRING,
    source_table        STRING,
    source_path         STRING,
    target_layer        STRING NOT NULL,   -- 'bronze' / 'silver' / 'gold'
    bronze_schema       STRING,
    silver_schema       STRING,
    gold_schema         STRING,
    active_flag         BOOLEAN NOT NULL,
    load_order          INT NOT NULL,
    created_at          TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 2. Processing Parameters (load_type, primary_key, watermark_column, partition_column, num_partitions)
CREATE TABLE IF NOT EXISTS finlake.metadata.table_parameters (
    table_id            INT NOT NULL,
    parameter_name      STRING NOT NULL,
    parameter_value     STRING NOT NULL,
    created_at          TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 3. Data Quality & Expectation Rules
CREATE TABLE IF NOT EXISTS finlake.metadata.data_quality_rules (
    rule_id             INT NOT NULL,
    table_id            INT NOT NULL,
    rule_name           STRING NOT NULL,
    rule_expression     STRING NOT NULL,
    severity            STRING NOT NULL,   -- 'CRITICAL' (Quarantine) / 'WARNING'
    active_flag         BOOLEAN NOT NULL,
    created_at          TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 4. Incremental High-Watermarks
CREATE TABLE IF NOT EXISTS finlake.metadata.table_watermarks (
    table_id                INT NOT NULL,
    last_watermark_value    STRING NOT NULL,
    last_updated_at         TIMESTAMP NOT NULL,
    last_run_id             BIGINT
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- 5. Execution Audit Logging (Partitioned by table_id for safe parallel Bronze/Silver writes)
CREATE TABLE IF NOT EXISTS finlake.metadata.pipeline_runs (
    run_id              BIGINT NOT NULL,
    table_id            INT NOT NULL,
    layer               STRING NOT NULL,   -- 'Bronze', 'Silver', 'Gold'
    start_time          TIMESTAMP NOT NULL,
    end_time            TIMESTAMP,
    status              STRING NOT NULL,   -- 'INPROGRESS', 'SUCCESS', 'FAILED'
    records_read        BIGINT,
    records_written     BIGINT,
    records_quarantined BIGINT,
    error_message       STRING
) USING DELTA
PARTITIONED BY (table_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
