SELECT
  complaint_type,
  COUNT(*) AS total_complaints,
  MIN(created_date) AS first_seen,
  MAX(created_date) AS last_seen
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type