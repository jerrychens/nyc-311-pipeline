-- Query to test performance AFTER index/partition optimization
-- Assumes index on load_date and possibly partitioned tables
EXPLAIN ANALYZE
SELECT complaint_type, COUNT(*) 
FROM stg_nyc_311_partitioned 
WHERE load_date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY complaint_type
ORDER BY COUNT(*) DESC
LIMIT 10;
