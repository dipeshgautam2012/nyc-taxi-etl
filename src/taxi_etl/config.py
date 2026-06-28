"""App configuration type and TOML loader."""

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    bronze_dir: Path
    logs_dir: Path
    warehouse_backend: str
    warehouse_path: Path
    taxi_type: str
    base_url: str
    zone_lookup_url: str
    max_null_pickup_pct: float
    min_rows_per_month: int


def load_config(path: Path) -> AppConfig:
    """Load config from an explicit TOML path. Relative paths in the file resolve against its parent directory."""
    root = path.parent
    with path.open("rb") as fh:
        raw = tomllib.load(fh)

    data = raw["data"]
    warehouse = raw["warehouse"]
    download = raw["download"]
    validation = raw["validation"]

    return AppConfig(
        bronze_dir=root / data["bronze_dir"],
        logs_dir=root / data["logs_dir"],
        warehouse_backend=str(warehouse.get("backend", "sqlite")).strip().lower(),
        warehouse_path=root / warehouse["path"],
        taxi_type=download["taxi_type"].strip().lower(),
        base_url=download["base_url"].rstrip("/"),
        zone_lookup_url=download["zone_lookup_url"],
        max_null_pickup_pct=float(validation["max_null_pickup_pct"]),
        min_rows_per_month=int(validation["min_rows_per_month"]),
    )
