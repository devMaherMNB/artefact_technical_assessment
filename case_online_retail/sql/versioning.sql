-- ============================================================
-- DATA VERSIONING STRATEGY — Online Retail Pipeline
-- ============================================================
-- Approach: Daily snapshot table
-- Rationale: Captures full state of staging data per pipeline run.
--            Simpler than SCD Type 2 (no schema changes to dims).
--            DATE key enforces one snapshot per day — idempotent.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS staging_online_retail;

CREATE TABLE IF NOT EXISTS staging_online_retail.raw_transactions_archive (
    invoice_no      VARCHAR(20),
    stock_code      VARCHAR(20),
    description     VARCHAR(255),
    quantity        INTEGER,
    invoice_date    VARCHAR(50),
    unit_price      NUMERIC(10,2),
    customer_id     VARCHAR(20),
    country         VARCHAR(100),
    load_timestamp  TIMESTAMP DEFAULT NOW(),
    batch_id        UUID DEFAULT gen_random_uuid(),
    snapshot_id     UUID NOT NULL,
    snapshot_date   DATE NOT NULL
);

-- To create a snapshot after each ingest run:
-- INSERT INTO staging_online_retail.raw_transactions_archive
-- SELECT *, gen_random_uuid() as snapshot_id, CURRENT_DATE as snapshot_date
-- FROM staging_online_retail.raw_transactions

-- To retrieve a specific day's snapshot:
SELECT * FROM staging_online_retail.raw_transactions_archive
WHERE snapshot_date = '2026-02-22';
