CREATE SCHEMA IF NOT EXISTS dw_online_retail;

-- Dimension Tables First
CREATE TABLE IF NOT EXISTS dw_online_retail.dim_customers (
    customer_id     SERIAL PRIMARY KEY,
    raw_customer_id VARCHAR(20) UNIQUE,  
    country         VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dw_online_retail.dim_products (
    product_id      SERIAL PRIMARY KEY,
    stock_code      VARCHAR(20) UNIQUE,
    description     VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dw_online_retail.dim_date (
    date_id INT PRIMARY KEY,           
    full_date DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- Fact Table
CREATE TABLE IF NOT EXISTS dw_online_retail.fact_sales (
    sales_id        SERIAL PRIMARY KEY,
    invoice_no      VARCHAR(20),
    customer_id     INTEGER REFERENCES dw_online_retail.dim_customers(customer_id),
    product_id      INTEGER REFERENCES dw_online_retail.dim_products(product_id),
    date_id         INTEGER REFERENCES dw_online_retail.dim_date(date_id),
    quantity        INTEGER NOT NULL,
    unit_price      NUMERIC(10,2) NOT NULL,
    total_value     NUMERIC(12,2) NOT NULL,
    load_timestamp  TIMESTAMP DEFAULT NOW(),
    batch_id        UUID DEFAULT gen_random_uuid()
);

