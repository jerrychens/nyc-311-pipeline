SELECT
  created_date,
  DATE(created_date) AS complaint_date,
  EXTRACT(HOUR FROM created_date) AS hour,
  EXTRACT(DOW FROM created_date) AS weekday
FROM {{ ref('stg_nyc_311') }}