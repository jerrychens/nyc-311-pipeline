-- create_partitioned_staging.sql
-- 建立母表（若不存在）
CREATE TABLE IF NOT EXISTS stg_nyc_311_partitioned (
    unique_key   BIGINT,
    created_date TIMESTAMP,
    complaint_type VARCHAR(100),
    descriptor   VARCHAR(100),
    latitude     DOUBLE PRECISION,
    longitude    DOUBLE PRECISION,
    load_date    DATE
) PARTITION BY RANGE (load_date);

-- 建立 index 在母表上（不會自動繼承至 partition）
CREATE INDEX IF NOT EXISTS idx_stg_nyc_311_load_date
  ON stg_nyc_311_partitioned (load_date);

-- Child partition 建立範本（實際在 DAG 裡面用變數執行）
-- 下面的 :start_date / :end_date、:partition_name 會由 Python 動態帶入
CREATE TABLE IF NOT EXISTS {{ partition_name }}
  PARTITION OF stg_nyc_311_partitioned
  FOR VALUES FROM ('{{ start_date }}') TO ('{{ end_date }}');

-- 建立 index 在 child partition
CREATE INDEX IF NOT EXISTS idx_{{ partition_name }}_load_date
  ON {{ partition_name }} (load_date);