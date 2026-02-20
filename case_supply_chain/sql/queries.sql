-- 1. Find the total quantity shipped for each product category
SELECT 
    dp.product_category, 
    SUM(fs.quantity) AS total_quantity
FROM supply_chain.fact_shipments fs 
JOIN supply_chain.dim_products dp ON fs.product_id = dp.product_id 
GROUP BY dp.product_category 
ORDER BY total_quantity DESC;

-- 2. Identify warehouses with the most efficient shipping processes based on average shipping times
SELECT 
    dw.warehouse_name,
    ROUND(AVG(fs.shipping_time_hours), 2) AS avg_shipping_time_hours
FROM supply_chain.fact_shipments fs
JOIN supply_chain.dim_warehouses dw ON fs.warehouse_id = dw.warehouse_id
GROUP BY dw.warehouse_name
ORDER BY avg_shipping_time_hours ASC;

-- 3. Calculate the total value of shipments for each supplier
SELECT 
    ds.supplier_name, 
    ROUND(SUM(fs.shipment_value), 2) AS total_shipment_value
FROM supply_chain.fact_shipments fs
JOIN supply_chain.dim_suppliers ds ON fs.supplier_id = ds.supplier_id
GROUP BY ds.supplier_name
ORDER BY total_shipment_value DESC;

-- 4. Retrieve the top 5 products with the highest total shipment quantities
SELECT 
    dp.product_name, 
    SUM(fs.quantity) AS total_quantity
FROM supply_chain.fact_shipments fs
JOIN supply_chain.dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.product_name
ORDER BY total_quantity DESC
LIMIT 5;

-- 5. Create a report that shows the distribution of shipment values for each product category
SELECT 
    dp.product_category, 
    MIN(fs.shipment_value) AS min_shipment_value,
    MAX(fs.shipment_value) AS max_shipment_value,
    ROUND(AVG(fs.shipment_value), 2) AS avg_shipment_value,
    ROUND(SUM(fs.shipment_value), 2) AS total_shipment_value,
    COUNT(fs.shipment_id) AS shipment_count
FROM supply_chain.fact_shipments fs
JOIN supply_chain.dim_products dp ON fs.product_id = dp.product_id
GROUP BY dp.product_category
ORDER BY total_shipment_value DESC;
