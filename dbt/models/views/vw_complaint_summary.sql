SELECT
  complaint_type,
  DATE(created_date) AS complaint_day,
  COUNT(*) AS complaint_count
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type, complaint_day