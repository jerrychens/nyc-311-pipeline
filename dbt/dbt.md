# 📦 dbt 快速入門套件 for NYC 311 資料專案（含 staging 範本）

## 🧠 什麼是 dbt？
dbt（Data Build Tool）是一個用 SQL 撰寫資料轉換邏輯的工具。
它讓你可以：

- 將原始資料（例如 Airflow 寫入的 `nyc_311`）轉換成清理過的模型
- 透過 `ref()` 建立模型依賴關係（像 DAG）
- 撰寫測試與描述文件來確保資料品質
- 輸出報表用的 view 給 BI 工具或 downstream 分析

---
## 🧱 staging 是什麼？為什麼需要？

| 功能               | 說明 |
|--------------------|------|
| 欄位命名一致化     | 將 `CamelCase` 或空格欄位轉為 `snake_case` |
| 型別標準化         | 例如 `text` → `timestamp`, `varchar` → `double` |
| 篩選資料           | 去除 `null`, `不合法資料`, 只保留必要欄位 |
| 提供下游一致輸入   | 所有 `dim`、`view` 都從 staging 開始 `ref()` |

📁 建議放在 `models/staging/` 資料夾中

---
## 🐳 docker-compose.yml 加入 dbt container
```yaml
services:
  dbt:
    image: ghcr.io/dbt-labs/dbt-postgres:1.7.7
    volumes:
      - ./dbt:/usr/app
    working_dir: /usr/app
    environment:
      DBT_PROFILES_DIR: /usr/app
    depends_on:
      - postgres
    entrypoint: ["tail", "-f", "/dev/null"]
```

---
## 📄 staging model 範本：stg_nyc_311.sql（models/staging/）
```sql
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
```

---
## 🧪 schema.yml（放在 models/staging/schema.yml）
```yaml
version: 2

models:
  - name: stg_nyc_311
    description: "Staging table for raw NYC 311 data"
    columns:
      - name: unique_key
        tests:
          - not_null
          - unique
      - name: created_date
        tests:
          - not_null
```

---
## 🧱 dim model 與 view 的差異

| 類型 | 說明 | 使用場景 |
|------|------|------------|
| `dim model` | 維度表，通常 materialized 成實體資料表（table） | 基礎資料建模（e.g., 客戶、地點、時間）|
| `view` | SQL 查詢邏輯，不 materialized，實際使用時才查 | 即時查詢、聚合趨勢分析或 BI 查詢 |

---
## 📁 三個 dim models（放在 models/marts/）

### 1. dim_complaint_type.sql
```sql
SELECT
  complaint_type,
  COUNT(*) AS total_complaints,
  MIN(created_date) AS first_seen,
  MAX(created_date) AS last_seen
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type
```

### 2. dim_location.sql
```sql
SELECT
  latitude,
  longitude,
  COUNT(*) AS complaint_count
FROM {{ ref('stg_nyc_311') }}
GROUP BY latitude, longitude
```

### 3. dim_time.sql
```sql
{{ config(materialized='table') }}

SELECT
  created_date,
  DATE(created_date) AS complaint_date,
  EXTRACT(HOUR FROM created_date) AS hour,
  EXTRACT(DOW FROM created_date) AS weekday
FROM {{ ref('stg_nyc_311') }}
```

---
## 📁 三個 views（放在 models/views/）

### 1. vw_complaint_summary.sql
```sql
{{ config(materialized='view') }}

SELECT
  complaint_type,
  DATE(created_date) AS complaint_day,
  COUNT(*) AS complaint_count
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type, complaint_day
```

### 2. vw_complaint_trend.sql
```sql
{{ config(materialized='view') }}

SELECT
  DATE_TRUNC('day', created_date) AS day,
  complaint_type,
  COUNT(*) AS total
FROM {{ ref('stg_nyc_311') }}
GROUP BY day, complaint_type
ORDER BY day
```

### 3. vw_top_complaints.sql
```sql
{{ config(materialized='view') }}

SELECT
  complaint_type,
  COUNT(*) AS total
FROM {{ ref('stg_nyc_311') }}
GROUP BY complaint_type
ORDER BY total DESC
LIMIT 10
```