"""Bronze landing — download TLC Parquet and zone lookup."""

import csv
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlretrieve

import pyarrow as pa
import pyarrow.parquet as pq

from taxi_etl.config import AppConfig


def bronze_trip_path(
    config: AppConfig, *, year: int, month: int, subdir: str = "trips"
) -> Path:
    partition = config.bronze_dir / subdir / f"year={year}" / f"month={month:02d}"
    return partition / f"{config.taxi_type}_tripdata_{year}-{month:02d}.parquet"


def zone_lookup_path(
    config: AppConfig,
    *,
    reference_subdir: str = "reference",
    filename: str = "taxi_zone_lookup.csv",
) -> Path:
    return config.bronze_dir / reference_subdir / filename


def download_trips(config: AppConfig, *, year: int, month: int) -> Path:
    """Download trip Parquet if not already on disk; return local path either way."""
    dest = bronze_trip_path(config, year=year, month=month)
    # if the file already exists, return the path
    if dest.exists():
        return dest
    # if the file does not exist, create the parent directory
    dest.parent.mkdir(parents=True, exist_ok=True)
    # download the file to a temporary file
    url = f"{config.base_url}/{dest.name}"
    tmp = dest.with_suffix(".parquet.part")
    urlretrieve(url, tmp)
    tmp.rename(dest)
    return dest


def download_zone_lookup(config: AppConfig) -> Path:
    """Download zone lookup CSV if not already on disk; return local path either way."""
    dest = zone_lookup_path(config)
    # if the file already exists, return the path
    if dest.exists():
        return dest
    # if the file does not exist, create the parent directory
    dest.parent.mkdir(parents=True, exist_ok=True)
    # download the file to a temporary file
    tmp = dest.with_suffix(".csv.part")
    urlretrieve(config.zone_lookup_url, tmp)
    tmp.rename(dest)
    return dest


def _add_ingest_metadata(
    source: Path,
    *,
    run_id: str,
    dest: Path | None = None,
) -> Path:
    """Stamp bronze file with ingest metadata columns (immutable landing)."""
    if dest and dest.exists() and dest != source:
        return dest

    table = pq.ParquetFile(source).read()
    if "_run_id" in table.column_names:
        return source

    ingested_at = datetime.now(timezone.utc)
    n = table.num_rows
    if "_ingested_at" not in table.column_names:
        table = table.add_column(
            table.num_columns,
            "_ingested_at",
            pa.array([ingested_at] * n, type=pa.timestamp("us", tz="UTC")),
        )
    if "_source_file" not in table.column_names:
        table = table.add_column(
            table.num_columns,
            "_source_file",
            pa.array([source.name] * n, type=pa.string()),
        )
    if "_run_id" not in table.column_names:
        table = table.add_column(
            table.num_columns,
            "_run_id",
            pa.array([run_id] * n, type=pa.string()),
        )

    if dest and dest != source:
        dest.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, dest)
        return dest

    tmp = source.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp)
    tmp.replace(source)
    return source


def prepare_bronze(
    config: AppConfig,
    *,
    year: int,
    month: int,
    run_id: str,
    skip_download: bool = False,
) -> list[Path]:
    if not skip_download:
        download_zone_lookup(config)
        download_trips(config, year=year, month=month)

    trip_path = bronze_trip_path(config, year=year, month=month)
    if not trip_path.exists():
        raise FileNotFoundError(f"bronze trip file missing: {trip_path}")

    _add_ingest_metadata(trip_path, run_id=run_id)
    return [trip_path]


def read_zone_lookup_rows(config: AppConfig) -> list[tuple[int, str, str, str]]:
    path = zone_lookup_path(config)
    if not path.exists():
        download_zone_lookup(config)
    rows: list[tuple[int, str, str, str]] = []
    with path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(
                (
                    int(row["LocationID"]),
                    row.get("Borough") or "",
                    row.get("Zone") or "",
                    row.get("service_zone") or "",
                )
            )
    return rows
