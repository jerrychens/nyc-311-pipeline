SELECT
  DATE_TRUNC('day', created_date) AS day,
  complaint_type,
  COUNT(*) AS total
FROM {{ ref('stg_nyc_311') }}
GROUP BY day, complaint_type
ORDER BY day