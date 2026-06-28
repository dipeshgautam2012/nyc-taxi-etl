"""Bronze → silver and silver → gold transforms (SQL executed via warehouse)."""

from pathlib import Path

from taxi_etl.ingest import read_zone_lookup_rows
from taxi_etl.schema import (
    TABLE_BRONZE_STAGING,
    TABLE_DIM_DATE,
    TABLE_DIM_LOCATION,
    TABLE_FACT_RIDES,
    TABLE_REJECTS,
    TABLE_SILVER_TRIPS,
)
from taxi_etl.transform_sql import date_key_expr, pickup_date_expr, silver_select, valid_where
from taxi_etl.warehouses.base_warehouse import BaseWarehouse


def build_silver(
    warehouse: BaseWarehouse,
    *,
    year: int,
    month: int,
    run_id: str,
    bronze_paths: list[Path],
) -> tuple[int, int, int]:
    """Bronze Parquet → cleaned silver_trips + rejects_quarantine for this partition.

    Reads raw TLC columns from bronze, renames/derives fields, splits valid vs invalid rows.
    Returns (bronze_rows, silver_rows, quarantine_rows).
    """
    dialect = warehouse.dialect
    staging = TABLE_BRONZE_STAGING

    # 1. Load bronze Parquet into a scratch table
    bronze_rows = warehouse.load_bronze_parquet(bronze_paths, staging_table=staging)
    select_body = silver_select(staging, dialect)
    where_clause = valid_where(dialect)

    # 2. Replace any prior silver/quarantine rows for this year/month
    warehouse.execute(
        f"DELETE FROM {TABLE_SILVER_TRIPS} WHERE year = ? AND month = ?",
        (year, month),
    )
    warehouse.execute(
        f"DELETE FROM {TABLE_REJECTS} WHERE year = ? AND month = ?",
        (year, month),
    )

    # 3. Valid rows → silver_trips
    warehouse.execute(
        f"""
        INSERT INTO {TABLE_SILVER_TRIPS} (
            vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, pulocation_id, dolocation_id,
            payment_type, fare_amount, tip_amount, total_amount,
            trip_duration_minutes, pickup_year, pickup_month, pickup_day,
            pickup_hour, pickup_dow, year, month, run_id, _source_file, _ingested_at
        )
        SELECT
            vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, pulocation_id, dolocation_id,
            payment_type, fare_amount, tip_amount, total_amount,
            trip_duration_minutes, pickup_year, pickup_month, pickup_day,
            pickup_hour, pickup_dow, ?, ?, ?, _source_file, _ingested_at
        FROM ({select_body} WHERE {where_clause}) s
        """,
        (year, month, run_id),
    )

    # 4. Invalid rows → rejects_quarantine
    warehouse.execute(
        f"""
        INSERT INTO {TABLE_REJECTS} (
            reject_reason, vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime,
            passenger_count, trip_distance, pulocation_id, dolocation_id,
            payment_type, fare_amount, tip_amount, total_amount,
            year, month, run_id, _source_file, _ingested_at
        )
        SELECT
            'invalid_pickup,dropoff,fare,distance,or_time_order',
            CAST(VendorID AS INTEGER),
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            passenger_count,
            trip_distance,
            CAST(PULocationID AS INTEGER),
            CAST(DOLocationID AS INTEGER),
            CAST(payment_type AS INTEGER),
            fare_amount,
            tip_amount,
            total_amount,
            ?, ?, ?,
            _source_file,
            _ingested_at
        FROM {staging}
        WHERE NOT ({where_clause})
        """,
        (year, month, run_id),
    )

    silver_rows = warehouse.scalar_int(
        f"SELECT COUNT(*) FROM {TABLE_SILVER_TRIPS} WHERE year = ? AND month = ?",
        (year, month),
    )
    quarantine_rows = warehouse.scalar_int(
        f"SELECT COUNT(*) FROM {TABLE_REJECTS} WHERE year = ? AND month = ?",
        (year, month),
    )
    warehouse.execute(f"DROP TABLE IF EXISTS {staging}")
    return bronze_rows, silver_rows, quarantine_rows


def _populate_dim_location_from_lookup(warehouse: BaseWarehouse, config) -> None:
    """Fill dim_location from taxi_zone_lookup.csv (borough, zone per LocationID)."""
    for location_id, borough, zone_name, service_zone in read_zone_lookup_rows(config):
        warehouse.execute(
            f"""
            INSERT OR REPLACE INTO {TABLE_DIM_LOCATION}
            (location_id, borough, zone_name, service_zone)
            VALUES (?, ?, ?, ?)
            """,
            (location_id, borough, zone_name, service_zone),
        )


def _add_unknown_locations(warehouse: BaseWarehouse, *, year: int, month: int) -> None:
    """Insert placeholder dim_location rows for zone IDs in trips but missing from the lookup CSV."""
    warehouse.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_DIM_LOCATION} (location_id, borough, zone_name, service_zone)
        SELECT DISTINCT loc_id, 'Unknown', 'Unknown', 'Unknown'
        FROM (
            SELECT pulocation_id AS loc_id FROM {TABLE_SILVER_TRIPS}
            WHERE year = ? AND month = ?
            UNION
            SELECT dolocation_id FROM {TABLE_SILVER_TRIPS}
            WHERE year = ? AND month = ?
        )
        WHERE loc_id IS NOT NULL
        """,
        (year, month, year, month),
    )


def build_gold(
    warehouse: BaseWarehouse,
    *,
    year: int,
    month: int,
    run_id: str,
) -> int:
    """Silver trips → star schema (dim_location, dim_date, fact_rides) for this partition.

    Source is silver_trips only — rows already passed cleaning in build_silver.
    Returns fact_rides row count for the partition.
    """
    dialect = warehouse.dialect
    date_key = date_key_expr(dialect)
    pickup_date = pickup_date_expr(dialect)

    # 1. Location lookup: TLC zone CSV, then placeholders for IDs missing from CSV
    _populate_dim_location_from_lookup(warehouse, warehouse.config)
    _add_unknown_locations(warehouse, year=year, month=month)

    # 2. Time lookup: one dim_date row per distinct pickup hour in silver
    warehouse.execute(
        f"""
        INSERT OR IGNORE INTO {TABLE_DIM_DATE} (
            date_key, pickup_date, year, month, day, hour, day_of_week
        )
        SELECT DISTINCT
            {date_key},
            {pickup_date},
            pickup_year,
            pickup_month,
            pickup_day,
            pickup_hour,
            pickup_dow
        FROM {TABLE_SILVER_TRIPS}
        WHERE year = ? AND month = ?
        """,
        (year, month),
    )

    # 3. Fact table: one row per silver trip, FKs to dim_date and dim_location
    warehouse.execute(
        f"DELETE FROM {TABLE_FACT_RIDES} WHERE year = ? AND month = ?",
        (year, month),
    )

    if dialect == "duckdb":
        warehouse.execute(
            f"""
            INSERT INTO {TABLE_FACT_RIDES} (
                fact_ride_id, date_key, pickup_location_id, dropoff_location_id,
                vendor_id, passenger_count, trip_distance, fare_amount, tip_amount,
                total_amount, trip_duration_minutes, payment_type, year, month, run_id
            )
            SELECT
                row_number() OVER (
                    ORDER BY tpep_pickup_datetime, pulocation_id, dolocation_id
                ),
                {date_key},
                pulocation_id,
                dolocation_id,
                vendor_id,
                passenger_count,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount,
                trip_duration_minutes,
                payment_type,
                ?, ?,
                ?
            FROM {TABLE_SILVER_TRIPS}
            WHERE year = ? AND month = ?
            """,
            (year, month, run_id, year, month),
        )
    else:
        warehouse.execute(
            f"""
            INSERT INTO {TABLE_FACT_RIDES} (
                date_key, pickup_location_id, dropoff_location_id,
                vendor_id, passenger_count, trip_distance, fare_amount, tip_amount,
                total_amount, trip_duration_minutes, payment_type, year, month, run_id
            )
            SELECT
                {date_key},
                pulocation_id,
                dolocation_id,
                vendor_id,
                passenger_count,
                trip_distance,
                fare_amount,
                tip_amount,
                total_amount,
                trip_duration_minutes,
                payment_type,
                ?, ?,
                ?
            FROM {TABLE_SILVER_TRIPS}
            WHERE year = ? AND month = ?
            """,
            (year, month, run_id, year, month),
        )

    return warehouse.scalar_int(
        f"SELECT COUNT(*) FROM {TABLE_FACT_RIDES} WHERE year = ? AND month = ?",
        (year, month),
    )
