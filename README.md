# NYC Taxi ETL

Downloads **NYC TLC** (Taxi & Limousine Commission) trip files and loads tables you can query in SQL.

It is a **batch pipeline**: each run processes **one month** of data on demand, then exits — not real-time streaming.

The flow is **ETL** (Extract → Transform → Load) using the **medallion architecture** — a pattern with three quality stages: **bronze** (raw copy on disk), **silver** (cleaned trips), **gold** (analytics-ready tables). Architecture detail: [`docs/DESIGN.md`](docs/DESIGN.md).

![System overview](docs/diagrams/system_overview.png)

| Read this if you want… | Where |
|------------------------|-------|
| Run it now | [Quick start](#quick-start) |
| Why it exists | [Problem](#problem-we-solve) |
| See dirty vs clean rows | [Example walkthrough](#example-walkthrough) |
| Architecture, validation, warehouse, columns | [`docs/DESIGN.md`](docs/DESIGN.md) |

---

## Quick start

From project root (`nyc-taxi-etl/`). Python 3.10+; network required on first download.

**`download_data.py`** writes raw files under `data/bronze/` (the medallion **bronze** layer). **`run_pipeline.py`** does that too, then loads **silver** and **gold** tables into the **warehouse** — a single SQLite or DuckDB database file (`warehouse/taxi.db`).

### Where outputs go

| What gets created | Where to find it |
|-------------------|------------------|
| **Raw trip file** — unchanged TLC Parquet for one month | `data/bronze/trips/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` |
| **Zone lookup** — borough/zone names for location IDs (downloaded once) | `data/bronze/reference/taxi_zone_lookup.csv` |
| **Cleaned trips** (`silver_trips`) and **rejected rows** (`rejects_quarantine`) | `warehouse/taxi.db` |
| **Analytics tables** — `fact_rides`, `dim_date`, `dim_location` (**gold** layer; star schema) | same file |
| **Run history** — row counts, status, validation results | `pipeline_runs` in same file |
| **Text log** for this run | `logs/pipeline_*.log` |

DuckDB: an optional in-process SQL engine (alternative to SQLite). Set `backend = "duckdb"` and `path = "warehouse/taxi.duckdb"` in `config.toml` — same table names, different file.

### One-time setup

```bash
cd nyc-taxi-etl
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### Run pipeline (download + transform)

```bash
source .venv/bin/activate
python scripts/run_pipeline.py --year 2024 --month 5
```

**Expect (~4 min, SQLite, 2024-05):** `status=success ... silver=3663844 gold=3663844`

Re-run with bronze already on disk:

```bash
python scripts/run_pipeline.py --year 2024 --month 5 --skip-download
```

Download bronze only:

```bash
python scripts/download_data.py --year 2024 --month 5
```

**`run_pipeline.py` flags:** `--year`, `--month` (required); `--skip-download` (use existing bronze).

### Verify

```bash
sqlite3 warehouse/taxi.db "SELECT status, year, month, silver_rows, gold_rows, quarantine_rows
FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;"
```

```bash
sqlite3 warehouse/taxi.db "SELECT validation_json FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;"
```

```bash
sqlite3 warehouse/taxi.db "SELECT COUNT(*) FROM fact_rides WHERE year=2024 AND month=5;"
```

`validation_json` stores **validation gate** results (did this month's data pass quality checks?) at the end of each run — for inspection only, not read back by the pipeline. Format: [`docs/DESIGN.md` — `validation_json`](docs/DESIGN.md#validation_json).

### Optional: DuckDB

```toml
[warehouse]
backend = "duckdb"
path = "warehouse/taxi.duckdb"
```

### Optional: Airflow

Not required for local runs — the CLI is enough. **Airflow** is an optional job scheduler with a web UI. A **DAG** (Directed Acyclic Graph) is its name for a workflow — here, one task that runs the same pipeline script.

1. Install Airflow in a separate venv (Python 3.10–3.13)
2. Start with `./scripts/start_airflow.sh`
3. Trigger DAG `taxi_monthly_etl` in the web UI (port **8080**)

Full install and usage: [`docs/DESIGN.md` — Airflow](docs/DESIGN.md#airflow-optional).

---

## Problem we solve

NYC publishes **millions of taxi trips per month** as **Parquet** files (a compressed columnar format — efficient for large tables). Useful, but not ready for reporting:

- **Dirty rows** — null pickup, negative fares, dropoff before pickup
- **Wrong shape** — wide trip files, not organized for dashboards
- **No quality gate** — no check that a whole month's data is sane before building analytics tables
- **No run audit** — reruns need status, counts, and validation history

This project saves **bronze** raw copies, **cleans** valid trips into **silver**, builds a **gold** **star schema** (one fact table `fact_rides` joined to dimension tables `dim_date` and `dim_location` — the usual layout for BI and SQL reports), **fails the run** when **partition** validation breaks (checks on the full year/month, not individual rows), and records every run in `pipeline_runs` and `logs/`.

**Not in scope:** real-time streaming, a public API, or Airflow inside core ETL code.

---

## Example walkthrough

Real **2024-05** runs: ~**3.72M** bronze → ~**3.66M** silver → ~**60k** quarantined. Three fictional rows show what fails and which layer each row reaches.

Row-cleaning rules and partition gates: [`docs/DESIGN.md` — Validation gates](docs/DESIGN.md#validation-gates).

### Step 1 — Bronze (raw input, no cleaning)

File: `data/bronze/trips/year=2024/month=05/yellow_tripdata_2024-05.parquet`  
Ingest copies Parquet and adds `_source_file`, `_ingested_at`, `_run_id`. **No cleaning.**

| Trip | VendorID | tpep_pickup_datetime | tpep_dropoff_datetime | trip_distance | PULocationID | DOLocationID | fare_amount | Verdict |
|------|----------|----------------------|------------------------|---------------|--------------|--------------|-------------|---------|
| **A** | 1 | 2024-05-15 14:32:10 | 2024-05-15 14:48:55 | 3.2 | 161 | 229 | 14.5 | **Clean** |
| **B** | 2 | 2024-05-15 09:10:00 | 2024-05-15 09:25:00 | 2.1 | 162 | 230 | **-5.0** | **Dirty** — `fare_amount < 0` |
| **C** | 1 | 2024-05-15 18:00:00 | **2024-05-15 17:45:00** | 1.0 | 161 | 229 | 12.0 | **Dirty** — dropoff before pickup |

### Step 2 — Silver (clean trips vs quarantine)

Rows that fail cleaning rules go to **quarantine** (`rejects_quarantine`) instead of silver.

**Trip A → `silver_trips`:**

| vendor_id | tpep_pickup_datetime | pulocation_id | dolocation_id | fare_amount | trip_duration_minutes | pickup_hour | year | month |
|-----------|----------------------|---------------|---------------|-------------|----------------------|-------------|------|-------|
| 1 | 2024-05-15 14:32:10 | 161 | 229 | 14.5 | 16 | 14 | 2024 | 5 |

**Dirty rows → `rejects_quarantine`:**

| Trip | fare_amount | tpep_dropoff_datetime | reject_reason |
|------|-------------|------------------------|---------------|
| **B** | -5.0 | 09:25:00 | `invalid_pickup,dropoff,fare,distance,or_time_order` |
| **C** | 12.0 | 17:45:00 (before pickup) | same label |

**Validate silver** (checks the whole month — **partition** gate): min row count, max null-pickup %. Fail → pipeline stops; gold not updated.

### Step 3 — Gold (star schema for reporting)

Zone names from `taxi_zone_lookup.csv`.

**`dim_location`:**

| location_id | borough | zone_name |
|-------------|---------|-----------|
| 161 | Manhattan | Midtown Center |
| 229 | Queens | LaGuardia Airport |

**`dim_date`** (from Trip A pickup): `date_key = YYYYMMDD × 100 + hour` → `2024051514`

| date_key | pickup_date | hour | day_of_week |
|----------|-------------|------|-------------|
| 2024051514 | 2024-05-15 | 14 | 3 (Wed) |

**`fact_rides`** (Trip A only; B and C were quarantined):

| date_key | pickup_location_id | dropoff_location_id | fare_amount | trip_duration_minutes |
|----------|-------------------|----------------------|-------------|----------------------|
| 2024051514 | 161 → Midtown | 229 → LaGuardia | 14.5 | 16 |

**Validate gold:** min row count, no orphan **foreign keys** (FKs) — every `location_id` in `fact_rides` must exist in `dim_location`. Fail → run marked **failed**.

### Step 4 — Run audit

| run_id | year | month | status | bronze_rows | silver_rows | quarantine_rows | gold_rows |
|--------|------|-------|--------|-------------|-------------|-----------------|-----------|
| `9cf2cb2f-…` | 2024 | 5 | success | 3723833 | 3663844 | 59989 | 3663844 |

Example analytics on gold:

```sql
SELECT d.pickup_date, d.hour, l.zone_name, COUNT(*) AS trips, AVG(f.fare_amount) AS avg_fare
FROM fact_rides f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_location l ON f.pickup_location_id = l.location_id
WHERE f.year = 2024 AND f.month = 5
GROUP BY 1, 2, 3
ORDER BY trips DESC
LIMIT 10;
```
