# 🗽 NYC 311 ETL Pipeline with Airflow, dbt, and PostgreSQL

This is a modern data engineering project that builds an end-to-end ETL pipeline using **Apache Airflow**, **dbt**, **PostgreSQL**, and **Docker** — all orchestrated in a fully containerized environment.

📌 Use case: Ingest and model NYC 311 complaint data for analytical and monitoring purposes.

---

## 📊 Project Overview

| Component    | Description |
|-------------|-------------|
| **Airflow DAG** | Ingests 311 service request data from NYC Open Data API, validates and transforms it |
| **PostgreSQL** | Stores raw and modeled data with partitioning and indexing for performance |
| **dbt models** | Generates clean dimensional tables (`dim_complaint_type`, etc.) and analytical views |
| **Validation & Logging** | dbt tests + partitioned staging + incremental loads |
| **Docker** | All components run in isolated containers (Airflow, dbt, PostgreSQL) |

---

## 🚀 Quickstart (One-Click Setup)

1. Install **Docker Desktop**
2. Clone this repository and create a `.env` file:

```bash
git clone https://github.com/<your-username>/nyc-311-pipeline.git
cd nyc-311-pipeline
cp .env.example .env
```

3. Initialize & launch the full pipeline:

```bash
make init     # Initializes Airflow and pulls required images
make start    # Starts all services (webserver, scheduler, dbt, postgres)
```

4. Open Airflow UI at http://localhost:8080  
   (Login: `airflow / airflow`)

---

## 📅 Running Backfill

To backfill historical data between a date range:

```bash
make backfill START=2025-05-01 END=2025-05-10
```

---

## 🧱 Folder Structure

```
.
├── dags/                     # Airflow DAGs
├── dbt/                      # dbt project (models, tests, profiles.yml)
├── benchmark/                # Performance test SQLs and markdown
├── logs/                     # Airflow logs
├── docker-compose.yml        # Docker config
├── Makefile                  # Easy CLI commands
├── init.sh                   # Auto-bootstrap script
└── README.md
```

---

## 🧪 Data Quality & Testing

- ✅ **dbt test** results are stored in logs and summarized after each run
- 🧼 Records are validated before insertion via Airflow
- 🔍 Partitioning is done by `load_date`, with optional `day`, `week`, or `month` mode

---

## 📈 Performance Benchmark (with/without Index)

Check [`benchmark/benchmark_result.md`](./benchmark/benchmark_result.md) for before/after query latency comparisons using various indexing strategies.

---

## 📦 Deployment Environment

- Apache Airflow 2.7.2 (LocalExecutor)
- PostgreSQL 13
- dbt-postgres 1.8.1
- Docker Compose (v3.7)

---

## 🛠️ Makefile Commands

| Command             | Description                        |
|---------------------|------------------------------------|
| `make init`         | Initialize Airflow setup           |
| `make start`        | Start all containers               |
| `make stop`         | Stop all services                  |
| `make backfill`     | Run historical backfill via DAG    |
| `make logs`         | Tail Airflow logs                  |
| `make clean`        | Remove all containers/volumes      |

---

## 🧾 License

MIT License © 2025 Jerry Chen

---

> Built with ❤️ for learning, testing, and data-driven storytelling.
