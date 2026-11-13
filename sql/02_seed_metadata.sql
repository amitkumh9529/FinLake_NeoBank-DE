-- =============================================================================
-- FinLake Idempotent Metadata Seeding (Safe for Repeated CI/CD Runs)
-- Never overwrites active production high-watermarks!
-- =============================================================================

-- 1. Idempotent MERGE for Logical Table Registry
MERGE INTO finlake.metadata.tables t
USING (
    SELECT * FROM VALUES
    (1, 'customers', 'sqlserver', 'banking', 'customers', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 1),
    (2, 'accounts', 'sqlserver', 'banking', 'accounts', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 2),
    (3, 'transactions', 'sqlserver', 'banking', 'transactions', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 3),
    (4, 'branches', 'sqlserver', 'banking', 'branches', NULL, 'silver', 'bronze', 'silver', NULL, TRUE, 4),
    (5, 'credit_bureau_reports', 'blob', NULL, NULL, '/Volumes/finlake/source/volume/credit_bureau_reports/', 'silver', 'bronze', 'silver', NULL, TRUE, 5),
    (6, 'payment_gateway_logs', 'blob', NULL, NULL, '/Volumes/finlake/source/volume/payment_gateway_logs/', 'silver', 'bronze', 'silver', NULL, TRUE, 6),
    (7, 'dim_customer_360', 'silver', NULL, NULL, NULL, 'gold', NULL, 'silver', 'gold', TRUE, 1),
    (8, 'fct_daily_bank_kpis', 'silver', NULL, NULL, NULL, 'gold', NULL, 'silver', 'gold', TRUE, 2),
    (9, 'dim_branch_performance', 'silver', NULL, NULL, NULL, 'gold', NULL, 'silver', 'gold', TRUE, 3),
    (10, 'fct_risk_customer_summary', 'silver', NULL, NULL, NULL, 'gold', NULL, 'silver', 'gold', TRUE, 4),
    (11, 'fct_txn_channel_summary', 'silver', NULL, NULL, NULL, 'gold', NULL, 'silver', 'gold', TRUE, 5)
    AS src(table_id, table_name, source_system, source_schema, source_table, source_path, target_layer, bronze_schema, silver_schema, gold_schema, active_flag, load_order)
) s
ON t.table_id = s.table_id
WHEN MATCHED THEN UPDATE SET
    t.table_name = s.table_name,
    t.source_system = s.source_system,
    t.source_schema = s.source_schema,
    t.source_table = s.source_table,
    t.source_path = s.source_path,
    t.target_layer = s.target_layer,
    t.bronze_schema = s.bronze_schema,
    t.silver_schema = s.silver_schema,
    t.gold_schema = s.gold_schema,
    t.active_flag = s.active_flag,
    t.load_order = s.load_order
WHEN NOT MATCHED THEN INSERT (
    table_id, table_name, source_system, source_schema, source_table, source_path,
    target_layer, bronze_schema, silver_schema, gold_schema, active_flag, load_order, created_at
) VALUES (
    s.table_id, s.table_name, s.source_system, s.source_schema, s.source_table, s.source_path,
    s.target_layer, s.bronze_schema, s.silver_schema, s.gold_schema, s.active_flag, s.load_order, current_timestamp()
);

-- 2. Idempotent MERGE for Table Processing Parameters (including Parallel JDBC partitioning)
MERGE INTO finlake.metadata.table_parameters t
USING (
    SELECT * FROM VALUES
    -- Customers (Parallel JDBC + Incremental MERGE)
    (1, 'load_type', 'MERGE'),
    (1, 'primary_key', 'customer_id'),
    (1, 'watermark_column', 'updated_at'),
    (1, 'partition_column', 'customer_id'),
    (1, 'num_partitions', '4'),
    -- Accounts (Parallel JDBC + Incremental MERGE)
    (2, 'load_type', 'MERGE'),
    (2, 'primary_key', 'account_id'),
    (2, 'watermark_column', 'updated_at'),
    (2, 'partition_column', 'account_id'),
    (2, 'num_partitions', '4'),
    -- Transactions (High-Volume Parallel JDBC + Incremental APPEND)
    (3, 'load_type', 'APPEND'),
    (3, 'primary_key', 'txn_id'),
    (3, 'watermark_column', 'txn_timestamp'),
    (3, 'partition_column', 'txn_id'),
    (3, 'num_partitions', '8'),
    -- Branches (Small Reference Dimension - FULL Load)
    (4, 'load_type', 'FULL'),
    (4, 'primary_key', 'branch_code'),
    (4, 'num_partitions', '1'),
    -- Credit Bureau Reports (Blob Auto Loader + Incremental MERGE)
    (5, 'load_type', 'MERGE'),
    (5, 'primary_key', 'customer_id'),
    (5, 'watermark_column', 'bureau_pull_date'),
    -- Payment Gateway Logs (Blob Auto Loader + Incremental APPEND)
    (6, 'load_type', 'APPEND'),
    (6, 'primary_key', 'txn_id'),
    (6, 'watermark_column', 'processed_timestamp')
    AS src(table_id, parameter_name, parameter_value)
) s
ON t.table_id = s.table_id AND t.parameter_name = s.parameter_name
WHEN MATCHED THEN UPDATE SET t.parameter_value = s.parameter_value
WHEN NOT MATCHED THEN INSERT (table_id, parameter_name, parameter_value, created_at)
VALUES (s.table_id, s.parameter_name, s.parameter_value, current_timestamp());

-- 3. Idempotent MERGE for Data Quality Rules
MERGE INTO finlake.metadata.data_quality_rules t
USING (
    SELECT * FROM VALUES
    (101, 1, 'valid_pan_format', "pan_number IS NOT NULL AND pan_number rlike '^[A-Z]{5}[0-9]{4}[A-Z]{1}$'", 'CRITICAL', TRUE),
    (102, 1, 'valid_email_format', "email IS NOT NULL AND email LIKE '%@%.%'", 'WARNING', TRUE),
    (103, 1, 'valid_kyc_status', "kyc_status IN ('VERIFIED', 'PENDING', 'REJECTED')", 'CRITICAL', TRUE),
    (201, 2, 'non_negative_balance', "balance >= 0", 'CRITICAL', TRUE),
    (202, 2, 'valid_currency', "currency IN ('INR', 'USD', 'EUR', 'GBP')", 'CRITICAL', TRUE),
    (301, 3, 'positive_txn_amount', "amount > 0", 'CRITICAL', TRUE),
    (302, 3, 'valid_txn_type', "txn_type IN ('DEBIT', 'CREDIT')", 'CRITICAL', TRUE),
    (501, 5, 'valid_credit_score_range', "credit_score BETWEEN 300 AND 900", 'CRITICAL', TRUE),
    (601, 6, 'positive_processing_time', "processing_time_ms > 0", 'CRITICAL', TRUE)
    AS src(rule_id, table_id, rule_name, rule_expression, severity, active_flag)
) s
ON t.rule_id = s.rule_id
WHEN MATCHED THEN UPDATE SET
    t.table_id = s.table_id,
    t.rule_name = s.rule_name,
    t.rule_expression = s.rule_expression,
    t.severity = s.severity,
    t.active_flag = s.active_flag
WHEN NOT MATCHED THEN INSERT (rule_id, table_id, rule_name, rule_expression, severity, active_flag, created_at)
VALUES (s.rule_id, s.table_id, s.rule_name, s.rule_expression, s.severity, s.active_flag, current_timestamp());

-- 4. Initialize Watermarks ONLY if not already present (Preserves Live Production State!)
MERGE INTO finlake.metadata.table_watermarks t
USING (
    SELECT * FROM VALUES
    (1, '1900-01-01 00:00:00', 0),
    (2, '1900-01-01 00:00:00', 0),
    (3, '1900-01-01 00:00:00', 0),
    (5, '1900-01-01', 0),
    (6, '1900-01-01 00:00:00', 0)
    AS src(table_id, last_watermark_value, last_run_id)
) s
ON t.table_id = s.table_id
WHEN NOT MATCHED THEN INSERT (table_id, last_watermark_value, last_updated_at, last_run_id)
VALUES (s.table_id, s.last_watermark_value, current_timestamp(), s.last_run_id);
