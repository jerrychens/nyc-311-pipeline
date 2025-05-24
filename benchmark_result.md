# Query Performance Benchmark

## Query: Top Complaint Types (monthly)

```sql
SELECT complaint_type, COUNT(*) 
FROM stg_nyc_311_partitioned 
WHERE load_date BETWEEN '2025-05-01' AND '2025-05-31'
GROUP BY complaint_type
ORDER BY COUNT(*) DESC
LIMIT 10;
```

| Version        | Planning Time | Execution Time | Index Used |
|----------------|---------------|----------------|-------------|
| Before Index   | 65 ms         | 4.3 sec        | ❌ Seq Scan |
| After Index    | 3.2 ms        | 0.12 sec       | ✅ Index Scan on load_date |
