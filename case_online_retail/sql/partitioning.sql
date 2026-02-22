-- ============================================================
-- PARTITIONING STRATEGY — Online Retail fact_sales
-- ============================================================
-- Status: Design reference — not applied to current schema
-- Reason: Declarative partitioning requires table rebuild;
--         not practical mid-pipeline for an existing table.
--         In a greenfield project, this would be applied at
--         table creation time.
-- ============================================================

-- CHOSEN COLUMN: date_id (YYYYMMDD integer)
-- STRATEGY: RANGE partitioning by year
--
-- RATIONALE:
--   - ~90% of retail analytical queries filter by time range
--     (e.g. "sales this month", "YoY comparison")
--   - RANGE on date_id enables partition pruning — Postgres
--     only scans the relevant year partition, not all 534k rows
--   - date_id as integer (YYYYMMDD) is naturally ordered,
--     making RANGE the correct strategy vs LIST or HASH
--
-- REJECTED ALTERNATIVES:
--   - LIST on country: too many distinct values, uneven distribution
--   - HASH on customer_id: even distribution but no range pruning benefit
--
-- PRODUCTION IMPLEMENTATION:
-- (would replace current fact_sales CREATE TABLE in schema.sql)

CREATE TABLE dw_online_retail.fact_sales (
    sales_id        INTEGER GENERATED ALWAYS AS IDENTITY,
    invoice_no      VARCHAR(20),
    customer_id     INTEGER REFERENCES dw_online_retail.dim_customers(customer_id),
    product_id      INTEGER REFERENCES dw_online_retail.dim_products(product_id),
    date_id         INTEGER NOT NULL REFERENCES dw_online_retail.dim_date(date_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    total_value     NUMERIC(12,2) NOT NULL,
    load_timestamp  TIMESTAMP DEFAULT NOW(),
    batch_id        UUID DEFAULT gen_random_uuid(),
    PRIMARY KEY (sales_id, date_id)
) PARTITION BY RANGE (date_id);

CREATE TABLE dw_online_retail.fact_sales_2010
    PARTITION OF dw_online_retail.fact_sales
    FOR VALUES FROM (20100101) TO (20110101);

CREATE TABLE dw_online_retail.fact_sales_2011
    PARTITION OF dw_online_retail.fact_sales
    FOR VALUES FROM (20110101) TO (20120101);

CREATE TABLE dw_online_retail.fact_sales_default
    PARTITION OF dw_online_retail.fact_sales
    DEFAULT;
