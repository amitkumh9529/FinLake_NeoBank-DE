-- =============================================================================
-- FinLake Unity Catalog Governance: ANSI-Safe Dynamic PII Masking & RBAC
-- Protects sensitive banking attributes (PAN, Email, Phone) in Silver & Gold
-- =============================================================================

-- 1. Masking Function: PAN Number (NULL-safe & ANSI-safe)
CREATE OR REPLACE FUNCTION finlake.metadata.mask_pan(pan_number STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('compliance_team') THEN pan_number
    WHEN pan_number IS NULL THEN NULL
    ELSE CONCAT('XXXXXX', RIGHT(pan_number, 4))
END;

-- 2. Masking Function: Email Address (Guards against malformed emails without '@')
CREATE OR REPLACE FUNCTION finlake.metadata.mask_email(email STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('compliance_team') THEN email
    WHEN email IS NULL THEN NULL
    WHEN NOT CONTAINS(email, '@') THEN '***@masked'
    ELSE CONCAT('***@', ELEMENT_AT(SPLIT(email, '@'), 2))
END;

-- 3. Masking Function: Phone Number (NULL-safe)
CREATE OR REPLACE FUNCTION finlake.metadata.mask_phone(phone_number STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('compliance_team') THEN phone_number
    WHEN phone_number IS NULL THEN NULL
    ELSE CONCAT('XXXXXX', RIGHT(phone_number, 4))
END;

-- 4. Apply Column Masks to Silver Customers & Gold Customer 360
ALTER TABLE finlake.silver.customers ALTER COLUMN pan_number SET MASK finlake.metadata.mask_pan;
ALTER TABLE finlake.silver.customers ALTER COLUMN email SET MASK finlake.metadata.mask_email;
ALTER TABLE finlake.silver.customers ALTER COLUMN phone_number SET MASK finlake.metadata.mask_phone;

ALTER TABLE finlake.gold.dim_customer_360 ALTER COLUMN pan_number SET MASK finlake.metadata.mask_pan;
ALTER TABLE finlake.gold.dim_customer_360 ALTER COLUMN email SET MASK finlake.metadata.mask_email;
ALTER TABLE finlake.gold.dim_customer_360 ALTER COLUMN phone_number SET MASK finlake.metadata.mask_phone;

-- 5. Unity Catalog Governance Tags & Least-Privilege Analytical Grants
ALTER TABLE finlake.silver.customers SET TAGS ('pii' = 'true', 'data_classification' = 'confidential');
ALTER TABLE finlake.gold.dim_customer_360 SET TAGS ('pii' = 'masked', 'domain' = 'customer_analytics');
ALTER TABLE finlake.gold.fct_daily_bank_kpis SET TAGS ('domain' = 'executive_kpi');

GRANT USE CATALOG ON CATALOG finlake TO `bi_analysts`;
GRANT USE SCHEMA ON SCHEMA finlake.gold TO `bi_analysts`;
GRANT SELECT ON SCHEMA finlake.gold TO `bi_analysts`;
