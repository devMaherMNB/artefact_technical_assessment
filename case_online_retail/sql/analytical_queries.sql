-- Query 1: Top 10 products by total revenue
SELECT 
    p.stock_code, 
    p.description, 
    SUM(f.total_value) as total_revenue
FROM dw_online_retail.fact_sales f
JOIN dw_online_retail.dim_products p ON f.product_id = p.product_id
GROUP BY p.stock_code, p.description
ORDER BY total_revenue DESC
LIMIT 10;

-- Query 2: Monthly revenue trend
SELECT 
    d.year, 
    d.month, 
    d.month_name, 
    SUM(f.total_value) as monthly_revenue
FROM dw_online_retail.fact_sales f
JOIN dw_online_retail.dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year ASC, d.month ASC;

-- Query 3: Revenue by country
SELECT 
    c.country, 
    SUM(f.total_value) as country_revenue
FROM dw_online_retail.fact_sales f
JOIN dw_online_retail.dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY country_revenue DESC;

-- Query 4: Top 10 customers by spend (excluding UNKNOWN)
SELECT 
    c.raw_customer_id, 
    SUM(f.total_value) as total_spend
FROM dw_online_retail.fact_sales f
JOIN dw_online_retail.dim_customers c ON f.customer_id = c.customer_id
WHERE c.raw_customer_id != 'UNKNOWN'
GROUP BY c.raw_customer_id
ORDER BY total_spend DESC
LIMIT 10;

-- Query 5: Sales vs Returns summary
SELECT 
    SUM(CASE WHEN quantity > 0 THEN total_value ELSE 0 END) as gross_sales,
    SUM(CASE WHEN quantity < 0 THEN ABS(total_value) ELSE 0 END) as total_returns,
    SUM(total_value) as net_revenue
FROM dw_online_retail.fact_sales;
