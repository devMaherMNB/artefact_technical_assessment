CREATE SCHEMA IF NOT EXISTS supply_chain;

-- Time Dimension
CREATE TABLE supply_chain.dim_time (
    date_id INT PRIMARY KEY,           -- Format: YYYYMMDD (20240115)
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

-- Product Dimension
CREATE TABLE supply_chain.dim_products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200) NOT NULL,
    product_category VARCHAR(100) NOT NULL,
    sku VARCHAR(50) UNIQUE NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Supplier Dimension
CREATE TABLE supply_chain.dim_suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200) NOT NULL,
    supplier_country VARCHAR(100) NOT NULL,
    contact_email VARCHAR(150),
    phone VARCHAR(50),
    rating NUMERIC(3,2),  -- e.g., 4.5 out of 5
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Warehouse Dimension
CREATE TABLE supply_chain.dim_warehouses (
    warehouse_id SERIAL PRIMARY KEY,
    warehouse_name VARCHAR(200) NOT NULL,
    warehouse_location VARCHAR(200) NOT NULL,
    warehouse_country VARCHAR(100) NOT NULL,
    capacity_sqft INT,
    manager_name VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Order Dimension
CREATE TABLE supply_chain.dim_orders (
    order_id SERIAL PRIMARY KEY,
    order_date DATE NOT NULL,
    customer_id VARCHAR(50),
    order_status VARCHAR(50) NOT NULL,  -- 'completed', 'pending', 'cancelled'
    order_priority VARCHAR(20),         -- 'high', 'medium', 'low'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact Table (Shipments)
CREATE TABLE supply_chain.fact_shipments (
    shipment_id SERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    supplier_id INT NOT NULL,
    warehouse_id INT NOT NULL,
    order_id INT NOT NULL,
    date_id INT NOT NULL,
    
    -- Denormalized for performance
    shipment_date DATE NOT NULL,
    
    -- Measures
    quantity INT NOT NULL CHECK (quantity > 0),
    shipment_value NUMERIC(12,2) NOT NULL CHECK (shipment_value >= 0),
    shipping_time_hours INT NOT NULL CHECK (shipping_time_hours >= 0),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Keys
    FOREIGN KEY (product_id) REFERENCES supply_chain.dim_products(product_id),
    FOREIGN KEY (supplier_id) REFERENCES supply_chain.dim_suppliers(supplier_id),
    FOREIGN KEY (warehouse_id) REFERENCES supply_chain.dim_warehouses(warehouse_id),
    FOREIGN KEY (order_id) REFERENCES supply_chain.dim_orders(order_id),
    FOREIGN KEY (date_id) REFERENCES supply_chain.dim_time(date_id)
);

-- Performance Indexes
CREATE INDEX idx_shipments_date ON supply_chain.fact_shipments(shipment_date);
CREATE INDEX idx_shipments_product ON supply_chain.fact_shipments(product_id);
CREATE INDEX idx_shipments_supplier ON supply_chain.fact_shipments(supplier_id);
CREATE INDEX idx_shipments_warehouse ON supply_chain.fact_shipments(warehouse_id);
CREATE INDEX idx_products_category ON supply_chain.dim_products(product_category);
