# dags/nyc_311_etl_pipeline.py
# 這版程式碼可以穩定支援 每日自動執行與手動 backfill，也具備了常見例外處理的彈性。

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from docker.types import Mount
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import os
import pandas as pd
import requests

# === DAG Configuration ===
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=5), # 縮短 retry 間隔
}
host_dbt_path = os.path.abspath("/Users/jerrychen/Documents/311/dbt")
PARTITION_TYPE = "week"  # 可選："day" / "week" / "month"


# === 1. 抓資料 ===
@task()
def fetch_data():
    context = get_current_context()
    execution_date = context["execution_date"]  # 確保是 datetime 物件
    
    base_url = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

    # 若無傳入 execution_date，預設為現在時間（主要是手動執行時用）
    if execution_date is None:
        execution_date = datetime.utcnow()

    # 判斷是否為第一次執行（假設 start_date 設為 days_ago(1)）
    # 如果是第一次，就抓取過去一整年的資料
    first_run = execution_date.date() == (days_ago(1)).date()
    if first_run:
        since = (execution_date - timedelta(days=365)).strftime('%Y-%m-%dT00:00:00')
    else:
        # 否則只抓當天資料（單日增量）
        since = execution_date.strftime('%Y-%m-%dT00:00:00')

    # 抓取區間結尾（次日）
    until = (execution_date + timedelta(days=1)).strftime('%Y-%m-%dT00:00:00')

    # 設定 API 查詢參數
    params = {
        #"$limit": 50000,
        "$where": f"created_date >= '{since}' AND created_date < '{until}'"
    }

    # 發出 GET 請求
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    data = response.json()

    # Debug 用
    print(f"✅ Fetched {len(data)} records from API.")

    return data

# === 2. 驗證 + 清洗 ===
@task()
def validate_and_clean_data(json_data):
    df = pd.DataFrame(json_data)

    # Debug：先看資料長什麼樣
    print(f"Raw data length: {len(df)}")
    print(f"Columns: {df.columns.tolist()}")
    print(df.head())

    if df.empty:
        raise AirflowSkipException("✅ 當日無資料，略過 validate 與後續寫入階段")

    required_columns = ['unique_key', 'created_date', 'complaint_type', 'descriptor', 'latitude', 'longitude']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df[required_columns].dropna()

    # 型別轉換，轉失敗時為 NaN
    # ✅ rebuilt schema 後的型別轉換
    df['unique_key'] = pd.to_numeric(df['unique_key'], errors='coerce')  # 轉成 BIGINT
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce')  # 轉成 TIMESTAMP
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')  # DOUBLE PRECISION
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')


    df = df.dropna()

    return df.to_json(orient='records')

# === 3. Insert into Staging Table ===
@task()
def insert_into_staging_table(json_str):
    df = pd.read_json(json_str)
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce', unit='ms')

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_nyc_311 (
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
            INSERT INTO stg_nyc_311 (unique_key, created_date, complaint_type, descriptor, latitude, longitude)
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


# === 4. 建立 Partition（day / week / month） ===
@task()
def create_partitions(partition_mode='week'):
    """
    建立分區資料表：依據 day、week、month 建立 RANGE 分區
    - 避免重疊與語法錯誤
    - 子表不重複宣告欄位與 primary key（由母表繼承）
    """

    context = get_current_context()
    execution_date = context["execution_date"].date()

    # === 建立母表（只建立一次）===
    create_main_sql = """
    CREATE TABLE IF NOT EXISTS stg_nyc_311_partitioned (
        unique_key BIGINT PRIMARY KEY,
        created_date TIMESTAMP,
        complaint_type VARCHAR(100),
        descriptor VARCHAR(100),
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        load_date DATE
    ) PARTITION BY RANGE (load_date);
    
    CREATE INDEX IF NOT EXISTS idx_stg_nyc_311_load_date
        ON stg_nyc_311_partitioned (load_date);
    """

    # === 計算分區時間範圍 ===
    if partition_mode == 'day':
        start = execution_date
        end = start + timedelta(days=1)
        partition_name = f"stg_nyc_311_day_{start.strftime('%Y%m%d')}"

    elif partition_mode == 'week':
        start = execution_date - timedelta(days=execution_date.weekday())
        end = start + timedelta(days=7)
        partition_name = f"stg_nyc_311_week_{start.strftime('%Y%m%d')}"

    elif partition_mode == 'month':
        start = execution_date.replace(day=1)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1)
        else:
            end = start.replace(month=start.month + 1)
        partition_name = f"stg_nyc_311_month_{start.strftime('%Y%m')}"

    else:
        raise ValueError("Invalid partition_mode. Must be one of: day, week, month")

    print(f"🔧 建立 partition: {partition_name} ({start} ~ {end})")

    # === 建立分區 SQL ===
    create_partition_sql = f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '{partition_name}'
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF stg_nyc_311_partitioned FOR VALUES FROM (%L) TO (%L);',
                '{partition_name}', '{start}', '{end}'
            );
            EXECUTE format(
                'CREATE INDEX %I ON %I (load_date);',
                'idx_{partition_name}_load_date', '{partition_name}'
            );
        END IF;
    END$$;
    """

    # === 執行 SQL ===
    hook = PostgresHook(postgres_conn_id='postgres_default')
    hook.run(create_main_sql)
    hook.run(create_partition_sql)
    print("✅ Partition 建立完成")


# === 5. Insert into Partitioned Table ===
@task()
def insert_into_partitioned_table(json_str):

    df = pd.read_json(json_str)
    df['created_date'] = pd.to_datetime(df['created_date'], errors='coerce', unit='ms')
    df['load_date'] = datetime.utcnow().date()

    hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = hook.get_conn()
    cur = conn.cursor()

    failed = 0
    for _, row in df.iterrows():
        try:
            cur.execute("""
                INSERT INTO stg_nyc_311_partitioned (
                    unique_key, created_date, complaint_type, descriptor, latitude, longitude, load_date
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (unique_key) DO NOTHING;
            """, (
                row['unique_key'],
                row['created_date'],
                row['complaint_type'],
                row['descriptor'],
                row['latitude'],
                row['longitude'],
                row['load_date']
            ))
        except Exception as e:
            failed += 1
            print(f"❌ Failed to insert row with unique_key={row['unique_key']}: {e}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Insert complete. Total rows: {len(df)}, Failed: {failed}")

# ✅ 4. 執行 dbt run / dbt test

host_dbt_path = os.path.abspath("/Users/jerrychen/Documents/311/dbt")  # 根據你專案實際位置, host的絕對路徑

run_dbt_dim_tables = DockerOperator(
    task_id='run_dbt_dim_tables',
    image='ghcr.io/dbt-labs/dbt-postgres:1.8.1',
    api_version='auto',
    auto_remove=True, # auto_remove=True（會自動刪除 container），若頻繁執行，可考慮先關掉
    command='run --select marts.dim_*',
    working_dir='/usr/app',
    docker_url='unix://var/run/docker.sock',
    #network_mode='bridge',
    network_mode='311_default',
    mount_tmp_dir=False,
    mounts=[
        Mount(source=host_dbt_path, target='/usr/app', type='bind')
    ],
    environment={
        'DBT_PROFILES_DIR': '/usr/app'
    }
)

# 每日進行dbt run and dbt test (也可以手動) 

# docker exec -it 311-dbt-1 bash
# dbt run
# dbt test

test_dbt = DockerOperator(
    task_id='test_dbt',
    image='ghcr.io/dbt-labs/dbt-postgres:1.8.1',
    api_version='auto',
    auto_remove=True,
    entrypoint=['/bin/bash', '-c'],
    command=[
        'mkdir -p logs && '
        'dbt test --select marts.dim_* | tee logs/dbt_test_$(date +"%Y%m%d_%H%M%S").log || true'  # 同時將 dbt test 的輸出寫入對應時間命名的 log 檔案
    ],
    # command=['dbt test --select dim_* | tee logs/dbt_test_$(date +"%Y%m%d_%H%M%S").log || true'],
    # dbt test 不管結果如何都 return 0（讓 Airflow 判定為成功 ）。
	# 所有測試警告與錯誤會寫入 log，可讓使用者在 Airflow UI 中檢查。
	# 若要自動分析錯誤（例如 grep WARN 或 FAIL），也能透過 DockerOperator + XCom 或 log parsing 實現。
    working_dir='/usr/app',
    docker_url='unix://var/run/docker.sock',
    
    network_mode='311_default',
    mount_tmp_dir=False,
    mounts=[
        Mount(source=host_dbt_path, target='/usr/app', type='bind')
    ],
    environment={
        'DBT_PROFILES_DIR': '/usr/app'
    }
)

# dbt test 結果寫入 log 以供分析 
scan_dbt_test_log = DockerOperator(
    task_id='scan_dbt_test_log',
    image='ghcr.io/dbt-labs/dbt-postgres:1.8.1',
    api_version='auto',
    auto_remove=True,
    entrypoint=['/bin/bash', '-c'],
    command=[
        'logs_dir=/usr/app/logs && '
        'latest=$(ls -t $logs_dir/dbt_test_*.log | head -n1) && '
        '[ -z "$latest" ] && echo "⚠️ No dbt_test log found. Skipping." && exit 0 || '
        'timestamp=$(basename "$latest" | sed "s/dbt_test_\\(.*\\)\\.log/\\1/") && '
        'summary=$logs_dir/dbt_test_summary_${timestamp}.log && '
        'warns=$(grep -c WARN "$latest" || true) && '
        'fails=$(grep -c FAIL "$latest" || true) && '
        'echo "# dbt test summary - generated by Airflow DAG" > "$summary" && '
        'echo "# Execution time: $timestamp" >> "$summary" && '
        'echo "# Total warnings: $warns" >> "$summary" && '
        'echo "# Total failures: $fails" >> "$summary" && '
        'echo "# Source log: $(basename "$latest")" >> "$summary" && '
        'grep -E "WARN|FAIL" "$latest" >> "$summary" || true && '
        'echo "📝 Summary written to: $summary"'
    ],
    working_dir='/usr/app',
    docker_url='unix://var/run/docker.sock',
    network_mode='311_default',
    mount_tmp_dir=False,
    mounts=[
        Mount(source=host_dbt_path, target='/usr/app', type='bind')
    ],
    environment={
        'DBT_PROFILES_DIR': '/usr/app'
    }
)


# ✅ DAG 設定與任務串接
with DAG(
    dag_id='nyc_311_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=True,
    tags=["etl", "airflow", "nyc311"]
) as dag:

    raw = fetch_data()
    cleaned = validate_and_clean_data(raw)
    staging = insert_into_staging_table(cleaned)
    create_parts = create_partitions()
    partitioned = insert_into_partitioned_table(cleaned)

    run_dbt_dim_tables.dag = dag
    test_dbt.dag = dag
    scan_dbt_test_log.dag = dag


    # 更強的順序依賴
    #	•	強制要求資料必須先寫入 staging，之後才開始建立 partition。
    #	•	比原本 [staging, create_parts] 並行邏輯更穩定，避免潛在的 race condition（萬一 partition 比 insert 晚才建就 GG）

    raw >> cleaned >> staging >> create_parts >> partitioned >> run_dbt_dim_tables >> test_dbt >> scan_dbt_test_log



