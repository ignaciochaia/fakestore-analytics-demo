-- Analytic views for FakeStore sales data

CREATE OR REPLACE VIEW fact_sales_line AS
SELECT
    (c.id::text || '-' || ci.product_id::text) AS line_id,
    c.id AS cart_id,
    c.cart_date,
    c.customer_id,
    cu.username,
    cu.city,
    ci.product_id,
    p.title AS product_name,
    p.category,
    p.price,
    ci.quantity,
    (p.price * ci.quantity) AS line_revenue
FROM carts c
JOIN cart_items ci ON ci.cart_id = c.id
JOIN products p ON p.id = ci.product_id
LEFT JOIN customers cu ON cu.id = c.customer_id;

CREATE OR REPLACE VIEW mart_sales_daily AS
SELECT
    DATE(cart_date) AS sales_date,
    COUNT(DISTINCT cart_id) AS orders,
    SUM(quantity) AS units_sold,
    SUM(line_revenue) AS revenue
FROM fact_sales_line
GROUP BY DATE(cart_date);

CREATE OR REPLACE VIEW mart_sales_by_category AS
SELECT
    category,
    SUM(quantity) AS units_sold,
    SUM(line_revenue) AS revenue
FROM fact_sales_line
GROUP BY category;

CREATE OR REPLACE VIEW mart_top_customers AS
SELECT
    customer_id,
    username,
    COUNT(DISTINCT cart_id) AS orders,
    SUM(line_revenue) AS total_revenue
FROM fact_sales_line
GROUP BY customer_id, username;
