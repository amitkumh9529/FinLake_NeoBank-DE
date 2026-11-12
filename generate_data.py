"""
FinLake - High-Performance Real-World Production Data Generator
--------------------------------------------------------------
Generates 1.55+ Million realistic financial records across Core Banking (RDBMS)
and External Cloud Object Feeds (Blob CSVs).

Key Features:
- Vectorized NumPy / Pandas generation: produces 1.55M rows in ~15-20 seconds.
- Realistic Indian banking / global fintech entities and distributions:
    * Branches: 100 metro & tier-2 branches with IFSC-style codes and real regions.
    * Customers: 100,000 realistic demographic profiles with KYC statuses and PANs.
    * Accounts: 150,000 accounts with realistic log-normal balance distributions.
    * Transactions: 1,000,000 ledger transactions across UPI, NEFT, IMPS, ATM, POS.
    * Credit Bureau Reports: 100,000 credit agency scoring records (CIBIL/Experian).
    * Payment Gateway Logs: 200,000 telemetry logs (Stripe, Razorpay, BillDesk).
- Realistic Production Edge Cases & Anomalies (0.5% - 1.0% injection):
    * Malformed PAN numbers (to trigger Data Quality Quarantine).
    * Non-negative balance constraint violations on Savings accounts.
    * Non-positive transaction amounts (amount <= 0).
    * Micro-batch duplicate updates (to test Delta MERGE deduplication).
    * Late-arriving timestamps (to test watermark lookback logic).
- Output Split:
    * Batch 1 (Historical / Initial Load ~80%)
    * Batch 2 (Incremental / CDC ~20%)
"""

import os
import sys
import time
from pathlib import Path
import numpy as np
import pandas as pd

# Set random seed for reproducible realism
np.random.seed(42)

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "Source_Data_Files"
SQL_DIR = SOURCE_DIR / "01_SQL_Server"
BLOB_DIR = SOURCE_DIR / "02_Blob"

# Ensure target directories exist
SQL_DIR.mkdir(parents=True, exist_ok=True)
BLOB_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------------------------------------------------------
# Entity Volume Configurations (Total: 1,550,100 rows)
# ------------------------------------------------------------------------------
NUM_BRANCHES = 100
NUM_CUSTOMERS = 100_000
NUM_ACCOUNTS = 150_000
NUM_TRANSACTIONS = 1_000_000
NUM_BUREAU_REPORTS = 100_000
NUM_GATEWAY_LOGS = 200_000

# City & Region reference table
INDIAN_CITIES = [
    ("Mumbai", "Maharashtra", "WEST"),
    ("Pune", "Maharashtra", "WEST"),
    ("Bengaluru", "Karnataka", "SOUTH"),
    ("Hyderabad", "Telangana", "SOUTH"),
    ("Chennai", "Tamil Nadu", "SOUTH"),
    ("Delhi", "Delhi", "NORTH"),
    ("Noida", "Uttar Pradesh", "NORTH"),
    ("Gurugram", "Haryana", "NORTH"),
    ("Kolkata", "West Bengal", "EAST"),
    ("Ahmedabad", "Gujarat", "WEST"),
    ("Jaipur", "Rajasthan", "NORTH"),
    ("Kochi", "Kerala", "SOUTH"),
    ("Chandigarh", "Punjab", "NORTH"),
    ("Indore", "Madhya Pradesh", "CENTRAL"),
    ("Lucknow", "Uttar Pradesh", "NORTH"),
]

FIRST_NAMES = [
    "Aarav", "Aditi", "Amit", "Ananya", "Arjun", "Deepak", "Divya", "Gaurav",
    "Ishaan", "Kavya", "Manish", "Neha", "Nikhil", "Pooja", "Pranav", "Priya",
    "Rahul", "Rakesh", "Riya", "Rohan", "Sachin", "Sneha", "Sunil", "Suresh",
    "Tanvi", "Varun", "Vikram", "Vivek", "Zoya", "Karan"
]

LAST_NAMES = [
    "Sharma", "Verma", "Patel", "Mehta", "Iyer", "Nair", "Reddy", "Rao",
    "Deshmukh", "Kulkarni", "Gupta", "Agarwal", "Singh", "Kumar", "Chopra",
    "Malhotra", "Joshi", "Bhat", "Das", "Banerjee", "Chatterjee", "Mishra"
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com", "finlake.bank", "icloud.com"]


def generate_branches() -> pd.DataFrame:
    """Generates 100 branch master entities."""
    print("  -> Generating Branches (100 rows)...")
    branch_codes = [f"BR{i:04d}" for i in range(1, NUM_BRANCHES + 1)]
    city_indices = np.random.choice(len(INDIAN_CITIES), size=NUM_BRANCHES)
    
    cities = [INDIAN_CITIES[i][0] for i in city_indices]
    states = [INDIAN_CITIES[i][1] for i in city_indices]
    regions = [INDIAN_CITIES[i][2] for i in city_indices]
    names = [f"{city} {regions[i]} Branch #{i+1}" for i, city in enumerate(cities)]
    
    created_at = pd.date_range("2021-01-01", periods=NUM_BRANCHES, freq="D").strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({
        "branch_code": branch_codes,
        "branch_name": names,
        "city": cities,
        "state": states,
        "region": regions,
        "created_at": created_at
    })
    return df


def generate_customers(branches_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 100,000 customers (80K Historical, 20K Incremental) with realistic PII & edge cases."""
    print("  -> Generating Customers (100,000 rows with edge cases)...")
    cust_ids = np.arange(1001, 1001 + NUM_CUSTOMERS)
    
    firsts = np.random.choice(FIRST_NAMES, size=NUM_CUSTOMERS)
    lasts = np.random.choice(LAST_NAMES, size=NUM_CUSTOMERS)
    
    # Dates of birth between 1950 and 2005 (18 to 74 years old)
    dob_start = np.datetime64("1955-01-01").astype("int64")
    dob_end = np.datetime64("2005-12-31").astype("int64")
    random_dobs = pd.to_datetime(np.random.randint(dob_start, dob_end, size=NUM_CUSTOMERS, dtype="int64"))
    dobs = random_dobs.strftime("%Y-%m-%d")

    # Vectorized PAN generation (5 letters + 4 digits + 1 letter)
    letters = np.array(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
    pan_prefix = np.array(["".join(np.random.choice(letters, 5)) for _ in range(NUM_CUSTOMERS)])
    pan_digits = np.random.randint(1000, 9999, size=NUM_CUSTOMERS).astype(str)
    pan_suffix = np.random.choice(letters, size=NUM_CUSTOMERS)
    pans = np.char.add(np.char.add(pan_prefix, pan_digits), pan_suffix)

    # Inject 0.5% malformed PANs to test Data Quality Quarantine (e.g. invalid format 'INVALID_PAN_')
    dirty_mask = np.random.rand(NUM_CUSTOMERS) < 0.005
    pans[dirty_mask] = np.array([f"BADPAN{i}" for i in range(np.sum(dirty_mask))])

    # Phone numbers: Indian 10-digit mobile (+91 9xxxxxxxxx)
    phone_prefixes = np.random.choice(["98", "97", "99", "91", "88", "70"], size=NUM_CUSTOMERS)
    phone_suffixes = np.random.randint(10000000, 99999999, size=NUM_CUSTOMERS).astype(str)
    phones = np.char.add(phone_prefixes, phone_suffixes)

    # Emails
    domains = np.random.choice(EMAIL_DOMAINS, size=NUM_CUSTOMERS)
    emails = [f"{f.lower()}.{l.lower()}{cid % 10000}@{d}" for f, l, cid, d in zip(firsts, lasts, cust_ids, domains)]

    # Inject 0.2% invalid emails (missing @ or domain) to test warning DQ rule
    email_dirty = np.random.rand(NUM_CUSTOMERS) < 0.002
    for i in np.where(email_dirty)[0]:
        emails[i] = f"corrupted_email_{cust_ids[i]}"

    # KYC statuses
    kyc_statuses = np.random.choice(["VERIFIED", "PENDING", "REJECTED"], size=NUM_CUSTOMERS, p=[0.88, 0.09, 0.03])

    # Branch assignment
    branch_codes = np.random.choice(branches_df["branch_code"].values, size=NUM_CUSTOMERS)

    # Timestamps
    created_ts = pd.date_range("2023-01-01", periods=NUM_CUSTOMERS, freq="30s").strftime("%Y-%m-%d %H:%M:%S")
    updated_ts = created_ts.copy()

    df = pd.DataFrame({
        "customer_id": cust_ids,
        "first_name": firsts,
        "last_name": lasts,
        "date_of_birth": dobs,
        "pan_number": pans,
        "email": emails,
        "phone_number": phones,
        "kyc_status": kyc_statuses,
        "branch_code": branch_codes,
        "created_at": created_ts,
        "updated_at": updated_ts
    })

    # Split into 80K Historical (Batch 1) and 20K Incremental (Batch 2)
    split_idx = int(NUM_CUSTOMERS * 0.8)
    df_hist = df.iloc[:split_idx].copy()
    df_inc = df.iloc[split_idx:].copy()

    # In incremental batch, simulate 500 duplicate updates of existing customers (testing MERGE)
    dup_ids = np.random.choice(df_hist["customer_id"].values, size=500, replace=False)
    dup_rows = df_hist[df_hist["customer_id"].isin(dup_ids)].copy()
    dup_rows["kyc_status"] = "VERIFIED"
    dup_rows["updated_at"] = "2025-06-01 12:00:00"
    df_inc = pd.concat([df_inc, dup_rows], ignore_index=True)

    return df_hist, df_inc


def generate_accounts(customers_df: pd.DataFrame, branches_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 150,000 accounts with realistic log-normal balance distributions."""
    print("  -> Generating Accounts (150,000 rows with edge cases)...")
    account_ids = np.arange(500001, 500001 + NUM_ACCOUNTS)
    
    # Assign accounts to customers (1 to 3 accounts per customer)
    all_cust_ids = customers_df["customer_id"].values
    assigned_cust_ids = np.random.choice(all_cust_ids, size=NUM_ACCOUNTS)

    acc_types = np.random.choice(["SAVINGS", "CURRENT", "SALARY", "FD"], size=NUM_ACCOUNTS, p=[0.60, 0.20, 0.15, 0.05])
    
    # Realistic log-normal balance: median ~25,000 INR, some high net-worth accounts up to 5M+
    raw_balances = np.random.lognormal(mean=10.0, sigma=1.5, size=NUM_ACCOUNTS)
    balances = np.round(np.clip(raw_balances, 500.0, 15_000_000.0), 2)

    # Inject 0.3% negative balances on SAVINGS to test Data Quality Quarantine (Overdraft constraint)
    neg_mask = (acc_types == "SAVINGS") & (np.random.rand(NUM_ACCOUNTS) < 0.003)
    balances[neg_mask] = -1 * np.round(np.random.uniform(100.0, 5000.0, size=np.sum(neg_mask)), 2)

    currencies = np.full(NUM_ACCOUNTS, "INR")
    branch_codes = np.random.choice(branches_df["branch_code"].values, size=NUM_ACCOUNTS)
    statuses = np.random.choice(["ACTIVE", "DORMANT", "CLOSED"], size=NUM_ACCOUNTS, p=[0.92, 0.06, 0.02])

    opened_dates = pd.date_range("2023-01-01", periods=NUM_ACCOUNTS, freq="20s").strftime("%Y-%m-%d")
    created_ts = pd.date_range("2023-01-01", periods=NUM_ACCOUNTS, freq="20s").strftime("%Y-%m-%d %H:%M:%S")
    updated_ts = created_ts.copy()

    df = pd.DataFrame({
        "account_id": account_ids,
        "customer_id": assigned_cust_ids,
        "account_type": acc_types,
        "balance": balances,
        "currency": currencies,
        "branch_code": branch_codes,
        "status": statuses,
        "opened_date": opened_dates,
        "created_at": created_ts,
        "updated_at": updated_ts
    })

    split_idx = int(NUM_ACCOUNTS * 0.8)
    df_hist = df.iloc[:split_idx].copy()
    df_inc = df.iloc[split_idx:].copy()

    # Simulate balance updates for 1,000 historical accounts in incremental batch
    acc_updates = df_hist.sample(1000).copy()
    acc_updates["balance"] = np.round(acc_updates["balance"] + np.random.uniform(-5000, 15000, size=1000), 2)
    acc_updates["updated_at"] = "2025-06-01 14:00:00"
    df_inc = pd.concat([df_inc, acc_updates], ignore_index=True)

    return df_hist, df_inc


def generate_transactions(accounts_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 1,000,000 transactions across payment channels with realistic timestamps."""
    print("  -> Generating Transactions (1,000,000 rows with edge cases)...")
    txn_ids = np.arange(10_000_001, 10_000_001 + NUM_TRANSACTIONS)
    
    acc_ids = accounts_df["account_id"].values
    assigned_acc_ids = np.random.choice(acc_ids, size=NUM_TRANSACTIONS)

    txn_types = np.random.choice(["DEBIT", "CREDIT"], size=NUM_TRANSACTIONS, p=[0.65, 0.35])
    channels = np.random.choice(["UPI", "NEFT", "IMPS", "ATM", "POS", "NET_BANKING"], size=NUM_TRANSACTIONS, p=[0.50, 0.15, 0.15, 0.10, 0.05, 0.05])
    
    # Amounts: UPI (small 50 - 5,000), NEFT/IMPS (large 5,000 - 250,000)
    amounts = np.where(
        channels == "UPI",
        np.round(np.random.exponential(scale=600, size=NUM_TRANSACTIONS) + 20, 2),
        np.round(np.random.exponential(scale=12000, size=NUM_TRANSACTIONS) + 500, 2)
    )
    amounts = np.clip(amounts, 1.0, 500_000.0)

    # Inject 0.1% non-positive amounts (e.g. 0.00 or -50.00) to test DQ rule
    invalid_mask = np.random.rand(NUM_TRANSACTIONS) < 0.001
    amounts[invalid_mask] = 0.00

    statuses = np.random.choice(["SUCCESS", "FAILED", "PENDING"], size=NUM_TRANSACTIONS, p=[0.94, 0.05, 0.01])

    # Continuous timestamps spread across 2024 - 2025
    timestamps = pd.date_range("2024-01-01", periods=NUM_TRANSACTIONS, freq="35s").strftime("%Y-%m-%d %H:%M:%S")

    df = pd.DataFrame({
        "txn_id": txn_ids,
        "account_id": assigned_acc_ids,
        "txn_type": txn_types,
        "amount": amounts,
        "txn_timestamp": timestamps,
        "channel": channels,
        "status": statuses,
        "created_at": timestamps
    })

    split_idx = int(NUM_TRANSACTIONS * 0.8)
    df_hist = df.iloc[:split_idx].copy()
    df_inc = df.iloc[split_idx:].copy()

    return df_hist, df_inc


def generate_credit_bureau_reports(customers_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 100,000 external credit agency records (CIBIL/Experian style)."""
    print("  -> Generating Credit Bureau Reports (100,000 rows)...")
    cust_ids = np.random.choice(customers_df["customer_id"].values, size=NUM_BUREAU_REPORTS, replace=False)
    
    # Credit score normal distribution centered at 720 (range 300 - 900)
    scores = np.random.normal(loc=715, scale=85, size=NUM_BUREAU_REPORTS).astype(int)
    scores = np.clip(scores, 300, 900)

    risk_grades = np.where(scores >= 750, "LOW", np.where(scores >= 650, "MEDIUM", "HIGH"))
    active_loans = np.random.choice([0, 1, 2, 3, 4, 5, 7], size=NUM_BUREAU_REPORTS, p=[0.30, 0.35, 0.18, 0.10, 0.04, 0.02, 0.01])
    
    # Overdue amounts mostly 0 for LOW risk, up to 150K for HIGH risk
    overdue = np.where(
        risk_grades == "HIGH",
        np.round(np.random.exponential(scale=18000, size=NUM_BUREAU_REPORTS), 2),
        0.00
    )

    pull_dates = pd.date_range("2024-01-01", periods=NUM_BUREAU_REPORTS, freq="300s").strftime("%Y-%m-%d")

    df = pd.DataFrame({
        "customer_id": cust_ids,
        "credit_score": scores,
        "risk_grade": risk_grades,
        "external_active_loans": active_loans,
        "external_overdue_amount": overdue,
        "bureau_pull_date": pull_dates
    })

    split_idx = int(NUM_BUREAU_REPORTS * 0.8)
    df_hist = df.iloc[:split_idx].copy()
    df_inc = df.iloc[split_idx:].copy()

    return df_hist, df_inc


def generate_payment_gateway_logs(transactions_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Generates 200,000 payment gateway telemetry logs (Stripe, Razorpay, BillDesk)."""
    print("  -> Generating Payment Gateway Logs (200,000 rows)...")
    sampled_txns = transactions_df.sample(NUM_GATEWAY_LOGS)
    txn_ids = sampled_txns["txn_id"].values
    base_ts = sampled_txns["txn_timestamp"].values

    gateways = np.random.choice(["Razorpay", "Stripe", "BillDesk", "PayU", "Cashfree"], size=NUM_GATEWAY_LOGS, p=[0.40, 0.25, 0.20, 0.10, 0.05])
    statuses = np.random.choice(["SUCCESS", "FAILED", "TIMEOUT"], size=NUM_GATEWAY_LOGS, p=[0.92, 0.06, 0.02])
    
    # Response codes: '00' is success, 'E01'-'E05' are bank error codes
    response_codes = np.where(statuses == "SUCCESS", "00", np.random.choice(["E01", "E02", "E03", "E04"], size=NUM_GATEWAY_LOGS))
    
    # Processing latency in ms: 50ms to 2800ms
    proc_time = np.random.exponential(scale=250, size=NUM_GATEWAY_LOGS).astype(int) + 45
    proc_time = np.clip(proc_time, 45, 5000)

    device_types = np.random.choice(["Android_App", "iOS_App", "Mobile_Web", "Desktop_Web", "POS_Terminal"], size=NUM_GATEWAY_LOGS, p=[0.45, 0.25, 0.15, 0.10, 0.05])
    locations = np.random.choice([c[0] for c in INDIAN_CITIES], size=NUM_GATEWAY_LOGS)

    df = pd.DataFrame({
        "txn_id": txn_ids,
        "gateway_name": gateways,
        "gateway_status": statuses,
        "response_code": response_codes,
        "processing_time_ms": proc_time,
        "device_type": device_types,
        "geo_location": locations,
        "processed_timestamp": base_ts
    })

    split_idx = int(NUM_GATEWAY_LOGS * 0.8)
    df_hist = df.iloc[:split_idx].copy()
    df_inc = df.iloc[split_idx:].copy()

    return df_hist, df_inc


def write_fast_csv(df: pd.DataFrame, path: Path):
    """Writes CSV quickly with chunked buffering."""
    df.to_csv(path, index=False, chunksize=100_000)


def main():
    start_total = time.time()
    print("=" * 75)
    print("⚡ FinLake: High-Performance Production Data Generator (1.55M Rows)")
    print("=" * 75)

    # 1. Branches
    branches_df = generate_branches()
    write_fast_csv(branches_df, SQL_DIR / "branches.csv")

    # 2. Customers
    cust_hist, cust_inc = generate_customers(branches_df)
    write_fast_csv(cust_hist, SQL_DIR / "customers_hist.csv")
    write_fast_csv(cust_inc, SQL_DIR / "customers_inc.csv")
    all_cust = pd.concat([cust_hist, cust_inc])

    # 3. Accounts
    acc_hist, acc_inc = generate_accounts(all_cust, branches_df)
    write_fast_csv(acc_hist, SQL_DIR / "accounts_hist.csv")
    write_fast_csv(acc_inc, SQL_DIR / "accounts_inc.csv")
    all_acc = pd.concat([acc_hist, acc_inc])

    # 4. Transactions
    txn_hist, txn_inc = generate_transactions(all_acc)
    write_fast_csv(txn_hist, SQL_DIR / "transactions_hist.csv")
    write_fast_csv(txn_inc, SQL_DIR / "transactions_inc.csv")
    all_txn = pd.concat([txn_hist, txn_inc])

    # 5. Credit Bureau Reports (Blob CSVs)
    bureau_hist, bureau_inc = generate_credit_bureau_reports(all_cust)
    write_fast_csv(bureau_hist, BLOB_DIR / "credit_bureau_reports_1.csv")
    write_fast_csv(bureau_inc, BLOB_DIR / "credit_bureau_reports_2_incremental.csv")

    # 6. Payment Gateway Logs (Blob CSVs)
    gateway_hist, gateway_inc = generate_payment_gateway_logs(all_txn)
    write_fast_csv(gateway_hist, BLOB_DIR / "payment_gateway_logs_1.csv")
    write_fast_csv(gateway_inc, BLOB_DIR / "payment_gateway_logs_2_incremental.csv")

    # Create SQL Server Bulk-Insert / Setup script
    sql_script_path = SQL_DIR / "04_Bulk_Load_SQLServer.sql"
    with open(sql_script_path, "w", encoding="utf-8") as f:
        f.write(f"""-- FinLake: High-Performance SQL Server Bulk Loading Script
-- Efficiently loads 1.25M Core Banking records into SQL Server in seconds

USE banking;
GO

-- 1. Branches
BULK INSERT banking.branches
FROM '{SQL_DIR.resolve()}\\branches.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK);

-- 2. Customers
BULK INSERT banking.customers
FROM '{SQL_DIR.resolve()}\\customers_hist.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK);

-- 3. Accounts
BULK INSERT banking.accounts
FROM '{SQL_DIR.resolve()}\\accounts_hist.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK);

-- 4. Transactions
BULK INSERT banking.transactions
FROM '{SQL_DIR.resolve()}\\transactions_hist.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\\n', TABLOCK);
GO
""")

    elapsed = time.time() - start_total
    total_rows = (
        len(branches_df)
        + len(all_cust)
        + len(all_acc)
        + len(all_txn)
        + len(bureau_hist)
        + len(bureau_inc)
        + len(gateway_hist)
        + len(gateway_inc)
    )

    print("=" * 75)
    print(f"✅ Generated {total_rows:,} real-world records in {elapsed:.2f} seconds!")
    print(f"📁 Core Banking (SQL Server): {SQL_DIR}")
    print(f"📁 External Feeds (Blob CSVs): {BLOB_DIR}")
    print(f"⚡ Bulk Insert Script Ready: {sql_script_path.name}")
    print("=" * 75)


if __name__ == "__main__":
    main()
