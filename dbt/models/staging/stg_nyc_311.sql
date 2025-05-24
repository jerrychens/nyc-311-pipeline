{{ config(materialized='view') }}

SELECT
  CAST(unique_key AS TEXT) AS unique_key,
  CAST(created_date AS TIMESTAMP) AS created_date,
  LOWER(complaint_type) AS complaint_type,
  descriptor,
  CAST(latitude AS DOUBLE PRECISION) AS latitude,
  CAST(longitude AS DOUBLE PRECISION) AS longitude
FROM {{ source('public', 'nyc_311') }}
WHERE created_date IS NOT NULL