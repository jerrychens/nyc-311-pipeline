SELECT
  complaint_type,
  COUNT(*) AS total
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type
ORDER BY total DESC
LIMIT 50