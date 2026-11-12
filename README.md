# FinLake: Enterprise Financial Lakehouse Platform

![CI/CD Pipeline](https://github.com/finlake/finlake/actions/workflows/ci_cd.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-Unity%20Catalog-orange)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-blueviolet)

## Project Overview
**FinLake** is an enterprise-grade, metadata-driven financial data lakehouse built on **Databricks**, **Unity Catalog**, and **Delta Lake**. Designed to unify core banking RDBMS entities, high-velocity payment gateway telemetry, and external credit bureau reports (tested at **1.55M+ realistic records**), it replaces manual siloed ETL with a self-healing **Medallion Architecture**.

---

## Architecture & Workflow DAG

```text
                [ 00_bootstrap_metadata ] (Unity Catalog + Pydantic Validation)
                            │
            ┌───────────────┴───────────────┐
            ▼                               ▼
[ 01a_ingest_sqlserver ]        [ 01b_ingest_blob_autoloader ]
(Dynamic Partitioned JDBC)      (Auto Loader + Schema Evolution)
            └───────────────┬───────────────┘
                            ▼
                [ 02_run_conformance ]
  (Standardize • Window Dedup • NULL-Safe DQ Gate • Quarantine DLQ • SCD1 Merge)
                            │
                            ▼
                [ 03_run_gold_marts ]
      (5 Delta Lake Gold Marts • Liquid Clustering • OPTIMIZE)
                            │
                            ▼
              [ 04_apply_governance ]
 (Unity Catalog PII Masking • Tags • RBAC Grants • Delta VACUUM 168h)
                            │ (ALL_DONE)
                            ▼
             [ 05_run_audit_summary ]
       (SLA Telemetry • DLQ Callouts • Customized HTML Email)
```

---

## Key Engineering Highlights
- **Metadata-Driven Orchestration**: Single-query metadata hydration (`MetadataRegistry`) validated via **Pydantic v2** schemas, supporting `FULL`, idempotent insert-only `APPEND`, and SCD Type 1 `MERGE` strategies.
- **Skew-Free Partitioned JDBC & Streaming**: Dynamic `MIN/MAX` partition bounds calculation prevents executor skew, while Databricks Auto Loader (`cloudFiles`) captures rescued data and schema evolution.
- **NULL-Safe Data Quality & Quarantine (DLQ)**: Separates `CRITICAL` vs `WARNING` rules using three-valued SQL logic guards, routing invalid records to `finlake.quarantine.*` without halting core pipelines.
- **Delta Lake Gold Marts & BI Serving**: Materializes `dim_customer_360`, `fct_daily_bank_kpis` (rolling 7-day windows), `dim_branch_performance`, `fct_risk_customer_summary`, and `fct_txn_channel_summary` (`p95_processing_time_ms`) in Delta Lake with **Liquid Clustering**, serving [FinLake_Dashboard.lvdash.json](05_Dashboard/FinLake_Dashboard.lvdash.json).
- **Unity Catalog Governance & Observability**: ANSI-safe dynamic PII column masks (`mask_pan`, `mask_email`, `mask_phone`), partitioned `pipeline_runs` audit logging, and branded HTML execution emails.
- **DevOps & CI/CD**: Wheel packaging (`uv build`), Ruff linting, PySpark/Delta integration tests, and automated **Databricks Asset Bundle (DAB)** deployments via GitHub Actions.

---

## Repository Structure
```text
finlake/
├── databricks.yml              # Databricks Asset Bundle (dev / stage / prod + whl)
├── pyproject.toml              # Dependencies, uv & Ruff/Pytest config
├── .python-version             # Pinned Python 3.11 (DBR 14.3+ compatible)
├── .env.example                # Local environment variable template
├── generate_data.py            # Vectorized 1.55M-row realistic banking data generator
├── .github/workflows/          # GitHub Actions CI/CD (Ruff + Pytest + DAB deploy)
├── resources/                  # Databricks Workflow DAG (finlake_workflow.yml)
├── sql/                        # Unity Catalog DDLs, idempotent MERGE seeds & PII masking
├── src/finlake/                # Modular PySpark engine
│   ├── config/                 # Pydantic models, Bootstrapper & MetadataRegistry
│   ├── ingestion/              # Dynamic PartitionedJDBCReader, AutoLoader & Bronze engine
│   ├── quality/                # NULL-safe DataQualityValidator & DLQ router
│   ├── transformations/        # Silver conformance/MERGE & Gold Delta marts
│   └── utils/                  # Audit, Watermark, Governance, Notifier, Spark & Logger
├── notebooks/                  # Thin Databricks Workflow execution notebooks (00 - 05)
├── 05_Dashboard/               # Databricks Lakeview AI/BI Dashboard JSON
└── tests/                      # Pytest unit & full E2E Delta Lake integration tests
```

---

## Quickstart & Local Verification

### 1. Setup Environment & Dependencies
```bash
cp .env.example .env
uv sync --extra dev
```

### 2. Generate 1.55M+ Realistic Banking Dataset
```bash
uv run python generate_data.py
```

### 3. Run Linting, Formatting, Tests & Wheel Build
```bash
uv run ruff check src/ tests/ notebooks/
uv run ruff format --check src/ tests/ notebooks/
uv run pytest tests/ -v --cov=src/finlake --cov-report=term-missing
uv build --wheel
```

### 4. Deploy via Databricks Asset Bundles
```bash
databricks bundle validate -t prod
databricks bundle deploy -t prod
databricks bundle run finlake_daily_pipeline -t prod
```
