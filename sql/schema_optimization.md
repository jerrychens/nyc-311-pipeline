# NYC 311 ETL Schema Optimization Guide

這份文件說明了什麼情況下我們需要重新設計資料表的 schema，以及如何根據本專案的需求進行優化，提升資料處理效能與一致性。

---

## ✅ 什麼情況下要重新設計 Table Schema？

| 情境 | 原因 |
|------|------|
| 使用 `TEXT` 或 `VARCHAR` 預設型別 | 無法預測欄位長度，影響效能和資料一致性 |
| 大量資料寫入或查詢變慢 | 缺少最佳化欄位型別與索引 |
| 欄位型別與實際用途不符（例如用 TEXT 存日期） | 不利於排序與運算 |
| 欄位名稱模糊或不規範 | 降低可讀性與維護性 |
| 預期會進行 join / group by | 型別一致性與精確設計會降低錯誤與查詢成本 |

---

## 🎯 建議 Schema 設計（以本專案為例）

| 欄位名稱 | 建議型別 | 原因 |
|----------|-----------|------|
| `unique_key` | `BIGINT` | 這是唯一編號，且是整數。效能更好 |
| `created_date` | `TIMESTAMP` | 符合時間格式，可用於篩選、索引、排序 |
| `complaint_type` | `VARCHAR(100)` | 文字型別但長度有限，可索引、較省空間 |
| `descriptor` | `VARCHAR(100)` | 同上 |
| `latitude` / `longitude` | `DOUBLE PRECISION` | 需要保留小數，適合做地理運算 |

---

## 🧪 檢查 unique_key 是否能轉為 BIGINT

```sql
SELECT unique_key FROM nyc_311 WHERE unique_key !~ '^\d+$' LIMIT 10;
```

如果查不到結果（0 rows）：

代表所有 unique_key 都是純數字，可以 安全地轉為 BIGINT，通常有更好的效能與儲存效率。

---

✅ Optimize schema 前要注意什麼？

✅ 1. 檢查是否有相依物件

在 DROP 原始資料表前，先確認是否有：
	•	View（pg_views）
	•	Materialized View
	•	Foreign Key constraints
	•	Indexes（不會阻止 DROP，但會連帶被刪除）

你可以先查出有哪些 view 依賴它，之後drop view：
```sql
SELECT dependent_view.relname AS view_name
FROM pg_depend
JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
JOIN pg_class AS dependent_view ON pg_rewrite.ev_class = dependent_view.oid
JOIN pg_class AS base_table ON pg_depend.refobjid = base_table.oid
WHERE base_table.relname = 'nyc_311';
```

```sql
SELECT viewname, schemaname
FROM pg_views
WHERE definition ILIKE '%nyc_311%';
```

---

## 🛠️ 建表語法

```sql
CREATE TABLE IF NOT EXISTS nyc_311 (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP,
    complaint_type VARCHAR(100),
    descriptor VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
```


詳細請見 `sql/rebuild_schema.sql`

```bash
docker cp sql/rebuild_schema.sql 311-postgres-1:/tmp/
docker exec -it 311-postgres-1 psql -U airflow -d airflow -f /tmp/rebuild_schema.sql
```

---

## ✏️ 修改 `load_to_postgres()`（資料型別轉換）

```python
@task()
def load_to_postgres(json_str):
    df = pd.read_json(json_str)

    # 🔁 確保型別一致性
    df['unique_key'] = pd.to_numeric(df['unique_key'], errors='coerce').astype('Int64')
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS nyc_311 (
            unique_key BIGINT PRIMARY KEY,
            created_date TIMESTAMP,
            complaint_type VARCHAR(100),
            descriptor VARCHAR(100),
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION
        );
    """)
    conn.commit()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO nyc_311 (unique_key, created_date, complaint_type, descriptor, latitude, longitude)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (unique_key) DO NOTHING;
        """, (
            row['unique_key'],
            row['created_date'],
            row['complaint_type'],
            row['descriptor'],
            row['latitude'],
            row['longitude']
        ))

    conn.commit()
    cur.close()
    conn.close()
```

---

## 🔄 若需轉換既有表格資料

```sql
CREATE TABLE nyc_311_v2 (
    unique_key BIGINT PRIMARY KEY,
    created_date TIMESTAMP,
    complaint_type VARCHAR(100),
    descriptor VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);

INSERT INTO nyc_311_v2
SELECT
    unique_key::BIGINT,
    created_date::TIMESTAMP,
    complaint_type,
    descriptor,
    latitude::DOUBLE PRECISION,
    longitude::DOUBLE PRECISION
FROM nyc_311;

DROP TABLE nyc_311;
ALTER TABLE nyc_311_v2 RENAME TO nyc_311;
```

---

## 🧩 小提醒

- 若使用 dbt，也建議在 schema.yml 中定義正確型別。
- 可將建表語法與 schema 說明記錄在 README 或 SQL migrations 中，以利團隊協作與後續維運。
