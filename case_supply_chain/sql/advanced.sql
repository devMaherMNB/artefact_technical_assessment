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

-- 3. Identify suppliers with a significant increase or decrease in shipment values compared to the previous year
-- Window function??