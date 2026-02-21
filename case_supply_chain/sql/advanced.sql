-- 1. Stored procedure to update shipment records for shipping delays
CREATE OR REPLACE PROCEDURE supply_chain.adjust_shipping_delay(
    p_shipment_id INT,
    p_added_hours INT
)
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE supply_chain.fact_shipments
    SET shipping_time_hours = shipping_time_hours + p_added_hours
    WHERE shipment_id = p_shipment_id;
END;
$$;

-- 2. View for consolidated summary of shipment and product performance
CREATE OR REPLACE VIEW supply_chain.vw_shipment_summary AS
SELECT
    dp.product_category,
    SUM(fs.quantity) AS total_quantity,
    ROUND(AVG(fs.shipping_time_hours), 2) AS avg_shipping_time_hours,
    ROUND(SUM(fs.shipment_value), 2) AS total_shipment_value,
    COUNT(fs.shipment_id) AS shipment_count
FROM supply_chain.fact_shipments fs
JOIN supply_chain.dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.product_category;

-- 3. Identify suppliers with a significant increase or decrease in shipment values compared to the previous year (Should be revised)
WITH supplier_annual_values AS (
    SELECT
        ds.supplier_name,
        SUM(fs.shipment_value) AS annual_value,
        dt.year
    FROM supply_chain.fact_shipments fs
    JOIN supply_chain.dim_suppliers ds ON fs.supplier_id = ds.supplier_id
    JOIN supply_chain.dim_time dt ON fs.date_id = dt.date_id
    GROUP BY ds.supplier_name, dt.year
)
SELECT 
    supplier_name,
    year,
    ROUND(annual_value, 2) AS current_year_value,
    ROUND(LAG(annual_value) OVER (PARTITION BY supplier_name ORDER BY year), 2) AS previous_year_value,
    ROUND(annual_value - LAG(annual_value) OVER (PARTITION BY supplier_name ORDER BY year), 2) AS net_change,
    ROUND(
        ((annual_value - LAG(annual_value) OVER (PARTITION BY supplier_name ORDER BY year)) / 
        NULLIF(LAG(annual_value) OVER (PARTITION BY supplier_name ORDER BY year), 0)) * 100, 
    2) AS percentage_change
FROM supplier_annual_values
ORDER BY supplier_name, year;

-- 4. Audit trigger for shipment changes
-- Create an audit table
CREATE TABLE IF NOT EXISTS supply_chain.shipment_audit_log (
    audit_id      SERIAL PRIMARY KEY,
    shipment_id   INT,
    operation     VARCHAR(10),       
    old_value     NUMERIC(12,2),      
    new_value     NUMERIC(12,2),     
    changed_at    TIMESTAMP DEFAULT NOW(), 
    changed_by    VARCHAR(100) DEFAULT CURRENT_USER
);

-- Then the trigger function
CREATE OR REPLACE FUNCTION supply_chain.log_shipment_changes()
RETURNS TRIGGER AS $
BEGIN
    IF TG_OP = 'DELETE' THEN
        INSERT INTO supply_chain.shipment_audit_log
            (shipment_id, operation, old_value, new_value)
        VALUES (OLD.shipment_id, 'DELETE', OLD.shipment_value, NULL);
        RETURN OLD;

    ELSIF TG_OP = 'UPDATE' THEN
        INSERT INTO supply_chain.shipment_audit_log
            (shipment_id, operation, old_value, new_value)
        VALUES (OLD.shipment_id, 'UPDATE', OLD.shipment_value, NEW.shipment_value);
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$ LANGUAGE plpgsql;

-- Attach the trigger to the fact table
DROP TRIGGER IF EXISTS trg_shipment_audit ON supply_chain.fact_shipments;
CREATE TRIGGER trg_shipment_audit
AFTER UPDATE OR DELETE ON supply_chain.fact_shipments
FOR EACH ROW EXECUTE FUNCTION supply_chain.log_shipment_changes();