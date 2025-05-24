#!/bin/bash

# run_backfill.sh
# 用法： ./run_backfill.sh 2025-05-01 2025-05-13

START_DATE=$1
END_DATE=$2

# 檢查輸入
if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "❌ 請輸入起訖日期"
  echo "用法： ./run_backfill.sh YYYY-MM-DD YYYY-MM-DD"
  exit 1
fi

echo "🚀 開始 backfill：$START_DATE 到 $END_DATE"

docker compose run --rm airflow-scheduler \
  airflow dags backfill nyc_311_etl_pipeline \
  --start-date "$START_DATE" \
  --end-date "$END_DATE"

echo "✅ backfill 完成"