-- tests/check_longitude.sql

SELECT *
FROM {{ ref('stg_nyc_311') }}
WHERE longitude IS NULL OR longitude < -180 OR longitude > 180