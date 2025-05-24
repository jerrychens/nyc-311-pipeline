-- Query to test performance BEFORE index or partition optimization
EXPLAIN ANALYZE
SELECT complaint_type, COUNT(*) 
FROM stg_nyc_311_partitioned 
WHERE load_date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY complaint_type
ORDER BY COUNT(*) DESC
LIMIT 10;
