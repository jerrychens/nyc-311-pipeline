SELECT
  latitude,
  longitude,
  COUNT(*) AS complaint_count
FROM {{ ref('stg_nyc_311') }}
GROUP BY latitude, longitude