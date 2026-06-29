# NYC Taxi ETL

## Problem we solve

[NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) (Taxi & Limousine Commission) publishes **millions of yellow taxi trips per month** as **Parquet** files — TLC's compressed columnar format, one file per month (e.g. `yellow_tripdata_2024-05.parquet`). **Yellow** means the classic street-hail cab fleet; TLC also publishes green cab, **FHV** (for-hire vehicles, e.g. Uber/Lyft), and other datasets separately. This project uses **yellow** trips only.

That raw data is useful but not ready for reporting:

- **Dirty rows** — null pickup times, negative fares, dropoff before pickup
- **Wrong shape** — one wide trip file per month; not split into a **fact** table (trip numbers) plus **dimension** tables (date, location) that you join for reports
- **Opaque locations** — pickup/dropoff are numeric zone IDs, not borough or neighborhood names
- **No quality gate** — no check that a whole **partition** (one year/month of data) looks sane before analytics run
- **No run audit** — reruns need status, row counts, and a record of what passed or failed

**Not in scope:** real-time streaming, a public API, or running **Airflow** (optional job scheduler) inside core ETL code.

---

## How this project solves it

A **batch pipeline** — processes **one month** on demand, then exits (not real-time streaming). Each run:

1. **Bronze** — save an immutable raw copy on disk (nothing overwritten)
2. **Silver** — keep valid trips; send bad rows to **quarantine** (a separate table for rejects)
3. **Gold** — build a **star schema**: a standard analytics table layout with one central **fact** table (`fact_rides` — one row per trip, numeric fields like fare and duration) and **dimension** tables (`dim_date`, `dim_location` — descriptive attributes you join to the fact table). Fixes the wrong-shape problem above.
4. **Validate** — **quality gates**: fail the run if the month's data breaks thresholds (row counts, null rates, broken joins)
5. **Audit** — record counts, status, and validation results in `pipeline_runs` and `logs/`

This is **ETL** (Extract → Transform → Load) using the **medallion architecture** — the bronze / silver / gold stages above. Architecture detail: [`docs/DESIGN.md`](docs/DESIGN.md).

![System overview](docs/diagrams/system_overview.png)

| Read this if you want… | Where |
|------------------------|-------|
| See the problem on example rows | [Example walkthrough](#example-walkthrough) |
| Run it now | [Quick start](#quick-start) |
| Architecture, validation, warehouse, columns | [`docs/DESIGN.md`](docs/DESIGN.md) |

---

## Example walkthrough

One month (**2024-05**) through each stage. Real run row counts: ~**3.72M** trips in bronze → ~**3.66M** valid in silver → ~**60k** rejected to quarantine (`bronze_rows` ≈ `silver_rows` + `quarantine_rows` for that month). Three fictional trips show the dirty-data problems above and how each layer responds.

Per-row cleaning rules and month-level gates: [`docs/DESIGN.md` — Validation gates](docs/DESIGN.md#validation-gates).

### Step 1 — Bronze (raw input, no cleaning)

File: `data/bronze/trips/year=2024/month=05/yellow_tripdata_2024-05.parquet`  
Ingest copies Parquet and adds `_source_file`, `_ingested_at`, `_run_id`. **No cleaning.**

Below: **three fictional example trips** (not copied from the file). Column names match real TLC bronze data; we only show fields the pipeline cares about. TLC files have many more columns — see [`docs/DESIGN.md`](docs/DESIGN.md#technical-reference). **Verdict** is for this walkthrough only — it is **not** a column in the source data; it labels whether each trip would pass silver cleaning.

| Trip | VendorID | tpep_pickup_datetime | tpep_dropoff_datetime | trip_distance | PULocationID | DOLocationID | fare_amount | Verdict (doc only) |
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

**Validate silver** — a **partition gate** (checks the full year/month, not one row at a time): min row count, max null-pickup %. Fail → pipeline stops; gold not updated.

### Step 3 — Gold (star schema)

Gold answers the **wrong shape** problem from above. A **star schema** splits analytics data into:

- **`fact_rides`** (**fact** table) — one row per trip; measures such as `fare_amount`, `trip_duration_minutes`
- **`dim_date`**, **`dim_location`** (**dimension** tables) — attributes you join to the fact table (when the trip happened; borough/zone names)

The name comes from how these tables look in a diagram (fact in the center, dimensions around it). Zone names come from `taxi_zone_lookup.csv` — this also fixes opaque location IDs.

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

### Step 4 — Audit (what happened on this run)

Every run writes a row to `pipeline_runs` — status, row counts, and validation results — plus a text log under `logs/`.

| run_id | year | month | status | bronze_rows | silver_rows | quarantine_rows | gold_rows |
|--------|------|-------|--------|-------------|-------------|-----------------|-----------|
| `9cf2cb2f-…` | 2024 | 5 | success | 3723833 | 3663844 | 59989 | 3663844 |

Example analytics on gold — the end goal the problem section describes:

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

---

## Quick start

From project root (`nyc-taxi-etl/`). Python 3.10+; network required on first download.

**`download_data.py`** writes raw files under `data/bronze/`. **`run_pipeline.py`** runs the full pipeline — bronze through gold plus validation and audit — into the **warehouse**: a single **SQLite** (built-in file database, default) or **DuckDB** file at `warehouse/taxi.db`.

### Where outputs go

| What gets created | Where to find it |
|-------------------|------------------|
| **Raw trip file** — unchanged TLC Parquet for one month | `data/bronze/trips/year=YYYY/month=MM/yellow_tripdata_YYYY-MM.parquet` |
| **Zone lookup** — borough/zone names for location IDs (downloaded once) | `data/bronze/reference/taxi_zone_lookup.csv` |
| **Cleaned trips** (`silver_trips`) and **rejected rows** (`rejects_quarantine`) | `warehouse/taxi.db` |
| **Analytics tables** — `fact_rides`, `dim_date`, `dim_location` (gold star schema: fact + dimensions) | same file |
| **Run history** — row counts, status, validation results | `pipeline_runs` in same file |
| **Text log** for this run | `logs/pipeline_*.log` |

DuckDB — optional faster SQL engine for analytics. Set `backend = "duckdb"` and `path = "warehouse/taxi.duckdb"` in `config.toml` (same table names, different file).

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
