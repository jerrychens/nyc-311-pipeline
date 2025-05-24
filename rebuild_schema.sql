-- 🔁 安全重建 nyc_311 資料表與 views

BEGIN;

-- 0️⃣ 刪除 views（若你已知道 view 名稱）
DROP VIEW IF EXISTS vw_top_complaints;
DROP VIEW IF EXISTS vw_complaint_trend;
DROP VIEW IF EXISTS vw_complaint_summary;
DROP VIEW IF EXISTS stg_nyc_311;

-- 1️⃣ 將原始表格重新命名
ALTER TABLE nyc_311 RENAME TO nyc_311_old;

-- 2️⃣ 建立新的 schema 精簡化的資料表
CREATE TABLE nyc_311 (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP,
    complaint_type VARCHAR(100),
    descriptor VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

-- 3️⃣ 將舊表的資料轉換後插入新表格
INSERT INTO nyc_311
SELECT
    unique_key::BIGINT,
    created_date::TIMESTAMP,
    complaint_type,
    descriptor,
    latitude::DOUBLE PRECISION,
    longitude::DOUBLE PRECISION
FROM nyc_311_old;

-- 4️⃣ 重建 stg_nyc_311 view
CREATE VIEW stg_nyc_311 AS
SELECT *
FROM nyc_311;

-- 5️⃣ 重建其他 views
CREATE VIEW vw_complaint_summary AS
SELECT complaint_type, COUNT(*) AS total
FROM stg_nyc_311
GROUP BY complaint_type;

CREATE VIEW vw_complaint_trend AS
SELECT DATE_TRUNC('month', created_date) AS month, COUNT(*) AS total
FROM stg_nyc_311
GROUP BY month;

CREATE VIEW vw_top_complaints AS
SELECT complaint_type, descriptor, COUNT(*) AS total
FROM stg_nyc_311
GROUP BY complaint_type, descriptor
ORDER BY total DESC
LIMIT 50;

-- 6️⃣ 確認無誤後，刪除舊表
DROP TABLE nyc_311_old;

COMMIT;