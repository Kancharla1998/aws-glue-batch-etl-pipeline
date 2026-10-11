-- Data quality validation for retail_db.orders (run in Amazon Athena)

-- 1. Total rows (reconcile against "Clean rows" in Glue logs)
SELECT COUNT(*) AS total_rows
FROM retail_db.orders;

-- 2. Rows per day (confirms each batch landed in the right partition)
SELECT order_date, COUNT(*) AS orders
FROM retail_db.orders
GROUP BY order_date
ORDER BY order_date DESC;

-- 3. Defect check for one day (every value should be 0)
SELECT
  COUNT_IF(customer_id IS NULL OR customer_id = '') AS missing_customer,
  COUNT_IF(quantity <= 0)                           AS bad_quantity,
  COUNT(*) - COUNT(DISTINCT order_id)               AS duplicates
FROM retail_db.orders
WHERE order_date = '2026-10-12';

-- 4. Status standardization (expect exactly 4 lowercase values)
SELECT order_status, COUNT(*) AS orders
FROM retail_db.orders
WHERE order_date = '2026-10-12'
GROUP BY order_status;