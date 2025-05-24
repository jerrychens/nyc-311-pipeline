-- models/marts/dim_time_incremental.sql
{{ config(
    materialized='incremental',
    unique_key='created_date'
) }}

SELECT
  created_date,
  DATE(created_date) AS complaint_date,
  EXTRACT(HOUR FROM created_date) AS hour,
  EXTRACT(DOW FROM created_date) AS weekday
FROM {{ ref('stg_nyc_311') }}

{% if is_incremental() %}
-- 僅處理新的資料
WHERE created_date > (SELECT MAX(created_date) FROM {{ this }})
{% endif %}