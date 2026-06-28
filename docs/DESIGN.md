# NYC Taxi ETL — design

Design for the batch ETL pipeline: medallion layers, linear orchestration, swappable warehouse, validation gates.

Config: `config.toml` at project root. PRD: [`../prd_3.md`](../prd_3.md).

---

## Rules

- **One orchestrator:** `pipeline.run_partition()` in `src/taxi_etl/pipeline.py` — read top to bottom.
- **CLI entry:** `scripts/run_pipeline.py` loads config and calls `run_partition()`.
- **Airflow optional:** `dags/` subprocesses the CLI — no ETL logic in Airflow, no Airflow imports in `taxi_etl`.
- **Factory at one seam:** `make_warehouse()` — SQLite default, DuckDB optional. Dialect-specific SQL in `transform_sql.py`.
- **Two kinds of quality control:** row cleaning during `build_silver` (`transform_sql.valid_where`) vs partition gates in `validate.py`.

---

## System overview

TLC Parquet + zone CSV → bronze on disk → ETL → warehouse tables. CLI triggers each run; optional Airflow subprocesses the same CLI.

![System overview](diagrams/system_overview.png)

| Piece | Role |
|-------|------|
| **Bronze** | Immutable Parquet + ingest metadata on disk |
| **ETL** | `run_partition()` — silver, validate, gold, validate, audit |
| **Warehouse** | `silver_trips`, `fact_rides`, dims, `pipeline_runs` |
| **CLI** | `scripts/run_pipeline.py` |
| **Airflow** (optional) | Scheduler — BashOperator to CLI only |

---

## Orchestration

There is a single ETL implementation. Airflow does not import `taxi_etl`.

![Orchestration workflow](diagrams/orchestration_workflow.png)

| Entry | What runs |
|-------|-----------|
| **Terminal** | `python scripts/run_pipeline.py --year YYYY --month MM` |
| **Airflow** | DAG `taxi_monthly_etl` → same CLI via subprocess |

Both paths call `pipeline.run_partition()` in one Python process.

Details: [README — Orchestration](../README.md#orchestration--how-and-where-runs-start)

---

## Pipeline workflow

Linear module flow — no factory packages beyond `warehouses/`.

![Pipeline workflow](diagrams/pipeline_workflow.png)

| Module | Role |
|--------|------|
| `ingest.py` | `prepare_bronze`, downloads |
| `transform.py` | `build_silver`, `build_gold` |
| `transform_sql.py` | SQL fragments by `warehouse.dialect` |
| `validate.py` | `validate_silver`, `validate_gold` |
| `warehouses/` | `make_warehouse`, `create_tables`, `execute` |
| `schema.py` | `ddl_sqlite()`, `ddl_duckdb()` |

**`run_partition()` steps:**

1. `setup_logging`
2. `make_warehouse` · `create_tables` · `insert_pipeline_run`
3. `prepare_bronze`
4. `build_silver`
5. `validate_silver`
6. `build_gold`
7. `validate_gold`
8. `update_pipeline_run` + `validation_json`

---

## Data structure

Medallion layers and gold star schema. Bronze is on disk; silver/gold live in the warehouse file.

![Data structure](diagrams/data_structure.png)

| Layer | Artifacts |
|-------|-----------|
| **Bronze** | `data/bronze/trips/…/*.parquet`, zone CSV, ingest metadata |
| **Silver** | `silver_trips`, `rejects_quarantine` |
| **Gold** | `dim_date`, `dim_location`, `fact_rides` |
| **Run history** | `pipeline_runs`, `logs/` |

Column definitions: [README — Technical reference](../README.md#technical-reference)

---

## Validation gates

Row cleaning (per row, run continues) vs partition checks (whole month, fail stops run).

![Validation gates](diagrams/validation_gates.png)

### Row cleaning (inside `build_silver`)

**Not** `validate.py`. Per row via `transform_sql.valid_where(dialect)`.

| Rule | Fails when |
|------|------------|
| Pickup exists | `tpep_pickup_datetime` is null |
| Dropoff exists | `tpep_dropoff_datetime` is null |
| Pickup zone valid | `PULocationID` null or ≤ 0 |
| Fare non-negative | `fare_amount` null or < 0 |
| Distance non-negative | `trip_distance` null or < 0 |
| Time order | dropoff before pickup |

Failed row → `rejects_quarantine`. Run continues.

### Partition checks (`validate.py`)

| When | Function | On failure |
|------|----------|------------|
| After silver, before gold | `validate_silver` | Run stops; gold not built |
| After gold | `validate_gold` | Run `failed`; `validation_json` saved |

**`validate_silver`:** `silver_min_rows` ≥ `min_rows_per_month` · `silver_null_pickup_pct` ≤ `max_null_pickup_pct`

**`validate_gold`:** `gold_min_rows` ≥ `min_rows_per_month` · `gold_location_fks` = 0 orphans (no config)

```toml
[validation]
max_null_pickup_pct = 0.01
min_rows_per_month = 1000
```

Pipeline order: `build_silver` → `validate_silver` → `build_gold` → `validate_gold`

Walkthrough with example rows: [README — Validation gates](../README.md#validation-gates-validatepy)

---

## Warehouse backend

Config picks SQLite or DuckDB. Two separate uses of the warehouse class:

![Warehouse backend](diagrams/warehouse_backend.png)

```toml
[warehouse]
backend = "sqlite"   # sqlite | duckdb
path = "warehouse/taxi.db"
```

`make_warehouse(config)` → `SqliteWarehouse` or `DuckdbWarehouse` (both extend `BaseWarehouse`).

| Path | When | What |
|------|------|------|
| **Create tables** | Pipeline start | `create_tables()` → `ddl_sqlite()` or `ddl_duckdb()` in `schema.py` |
| **Transform SQL** | `build_silver` / `build_gold` | `transform.py` reads `warehouse.dialect` → `transform_sql.py` → `warehouse.execute()` |

`warehouse.dialect` is always `"sqlite"` or `"duckdb"` — same string as `config [warehouse] backend`.

Dialect examples: SQLite uses `REAL` / `datetime()`; DuckDB uses `DOUBLE` / `EXTRACT` / `datediff`.

More detail: [README — Backend and dialect](../README.md#warehouse-backend-and-sql-dialect)

---

## Project layout

```
nyc-taxi-etl/
├── README.md
├── docs/
│   ├── DESIGN.md
│   └── diagrams/
├── requirements.txt          # pyarrow only
├── config.toml
│
├── scripts/
│   ├── download_data.py
│   └── run_pipeline.py       # CLI → pipeline.run_partition()
│
├── src/taxi_etl/
│   ├── config.py
│   ├── pipeline.py           # linear orchestration
│   ├── pipeline_types.py     # run + validation records
│   ├── ingest.py
│   ├── transform.py          # build_silver, build_gold
│   ├── transform_sql.py      # dialect-specific SQL fragments
│   ├── validate.py
│   ├── schema.py             # table names + DDL
│   ├── warehouses/
│   │   ├── base_warehouse.py
│   │   ├── sqlite_warehouse.py
│   │   ├── duckdb_warehouse.py
│   │   └── warehouse_factory.py   # make_warehouse()
│   └── logging_setup.py
│
├── sql/                      # reference notes (logic in transform.py)
├── dags/                     # optional Airflow DAGs; subprocess to CLI
├── dashboard/                # optional Streamlit UI
│
├── data/                     # gitignored
├── warehouse/                # gitignored
└── logs/                     # gitignored
```

Full config: `config.toml` at project root.

---

## Locked decisions (v1)

| Topic | Choice |
|-------|--------|
| Warehouse | `make_warehouse()` — SQLite default, DuckDB optional |
| Orchestration | `pipeline.run_partition()` + CLI script (Airflow subprocesses CLI) |
| Dependencies | `pyarrow` + Python stdlib |
| Tests | Manual smoke run + validation gates |
| Git | Code + config; ignore `data/`, `warehouse/`, `logs/` |

---

## Related docs

| Doc | Contents |
|-----|----------|
| [`../README.md`](../README.md) | Quick start, walkthrough, technical reference |
