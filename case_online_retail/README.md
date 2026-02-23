# Online Retail Sales — Data Engineering Pipeline

## Overview

An end-to-end data engineering pipeline built on the UCI Online Retail dataset (~541k transactions).
The pipeline ingests raw CSV data, cleans and validates it through a medallion architecture, and loads
it into a star schema data warehouse optimised for analytical queries. Orchestration is handled by
Apache Airflow, with all services containerised via Docker Compose. 

## Architecture

┌─────────────────────────────┐
│ Bronze: staging_online_retail │ Raw, exact copy of source. No cleaning.
│ raw_transactions             │ TRUNCATE + reload on every run (idempotent).
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ Silver: silver_online_retail │ Cleaned data with natural keys.
│ products / customers /       │ Decouples transform from load.
│ transactions                 │ Enables pipeline resumability.
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ Gold: dw_online_retail       │ Star schema with surrogate keys,
│ dim_products / dim_customers │ FK constraints, indexes, and
│ dim_date / fact_sales        │ partitioning by date_id.
└─────────────────────────────┘
│
▼
┌─────────────────────────────┐
│ Monitor                      │ 5 DQ checks: row count, null values,
│                              │ negative totals, date range, staging gap.
└─────────────────────────────┘

**Tools:** Python 3.11 · Pandas · SQLAlchemy · PostgreSQL 15 · Apache Airflow 2.7.1 · Docker Compose

## Tech Stack


| Layer            | Tool                 |
| ---------------- | -------------------- |
| Language         | Python 3.11          |
| Data processing  | Pandas, SQLAlchemy   |
| Database         | PostgreSQL 15        |
| Orchestration    | Apache Airflow 2.7.1 |
| Containerisation | Docker Compose       |


## How to Run

### Prerequisites

- Docker Desktop running

### Setup

```powershell
# 1. Start infrastructure
docker-compose up -d

# 2. Install dependencies
docker-compose exec app pip install -r requirements.txt
```

> Schema is initialised automatically on the first DAG run — no manual SQL step required.

### Run Pipeline (Manual)

```powershell
docker-compose exec app python case_online_retail/src/ingest.py
docker-compose exec app python case_online_retail/src/transform.py
docker-compose exec app python case_online_retail/src/load.py
docker-compose exec app python case_online_retail/src/monitor.py
```

### Run via Airflow (Scheduled)

- **Airflow UI:** available at `http://localhost:8080` (Credentials: `admin` / `admin`)
- **DAG:** `retail_etl_dag` — runs `@daily`
- **To trigger manually:** Unpause the DAG and click "Trigger DAG"

### Run Tests

```powershell
docker-compose exec app pytest -p no:postgresql case_online_retail/tests/ -v
```

## File Structure

case_online_retail/  
├── README.md                    ← overview, how to run, architecture  
├── dags/  
│   └── retail_etl_dag.py        ← Airflow DAG (5 tasks)  
├── metadata/  
│   └── data_catalog.json        ← Bronze/Silver/Gold documentation  
├── sql/  
│   ├── schema.sql               ← all 3 schemas + tables + indexes  
│   ├── analytical_queries.sql   ← 5 business queries  
│   ├── partitioning.sql         ← partitioning design + DDL reference  
│   └── versioning.sql           ← snapshot strategy + INSERT  
├── src/  
│   ├── ingest.py                ← CSV → staging (Bronze)  
│   ├── transform.py             ← staging → Silver (clean + write)  
│   ├── load.py                  ← Silver → Gold (surrogate keys + DW)  
│   └── monitor.py               ← DQ checks  
└── tests/  
    └── test_online_retail.py    ← 5 unit tests (no DB required)

## Data Quality Summary

- **Raw Rows:** 541,909
- **Duplicates Dropped:** 5,268
- **Nulls Handled:** 135,037 (customer_id), 1,454 (description)
- **Loaded to Warehouse:** 534,125

## Design Decisions

Full design rationale is documented in the data catalog and inline code comments.

Summary:

- **Medallion architecture (Bronze/Silver/Gold)** — Silver layer decouples transform from load, enabling pipeline resumability without re-reading 541k rows from staging if load fails
- **Pandas over Spark** — 541k rows is trivial for in-memory processing; Spark adds JVM overhead with zero benefit at this scale
- **Sentinel values over row drops** — customer_id nulls filled with 'UNKNOWN' to preserve 135k rows of revenue data while maintaining FK integrity
- **Exact deduplication only** — drop_duplicates() on all columns drops 5,268 true duplicates; logical duplicates (same invoice + product) are intentionally kept as valid repeat line items

## Further Improvements

- Add indexes on Silver layer tables to speed up pd.read_sql() queries in load.py
- Implement SCD Type 2 on dim_products to track historical price changes per product
- Integrate Great Expectations or dbt tests for declarative data quality validation
- Deploy to AWS (S3 for raw storage + MWAA for Airflow + Redshift as warehouse)
- Add PII masking for customer_id values before loading to Gold layer
- Replace chunked INSERT in load.py with PostgreSQL COPY command for faster fact loading
- Wire monitor.py alerts to Slack webhook or email via smtplib for production alerting
- Add materialized views for caching and versioning wired as a DAG Task

