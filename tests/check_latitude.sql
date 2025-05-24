-- tests/check_latitude.sql

SELECT *
FROM {{ ref('stg_nyc_311') }}
WHERE latitude IS NULL OR latitude < -90 OR latitude > 90

