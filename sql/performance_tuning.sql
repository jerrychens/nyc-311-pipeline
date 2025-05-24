-- 建立查詢加速用的 index（僅需執行一次）
\echo Creating index on created_date...
CREATE INDEX IF NOT EXISTS idx_nyc311_created_date
ON nyc_311(created_date);

\echo Creating index on complaint_type...
CREATE INDEX IF NOT EXISTS idx_nyc311_complaint_type
ON nyc_311(complaint_type);

-- 可選：聯合查詢時使用的複合索引
\echo Creating index on (complaint_type, created_date)...
CREATE INDEX IF NOT EXISTS idx_nyc311_type_date
ON nyc_311(complaint_type, created_date);

-- 最後加一行讓你看出有執行成功
SELECT '✅ performance_tuning.sql executed' AS status;