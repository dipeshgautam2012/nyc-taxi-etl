"""Return a warehouse implementation from AppConfig.warehouse_backend."""

from taxi_etl.config import AppConfig
from taxi_etl.warehouses.base_warehouse import BaseWarehouse
from taxi_etl.warehouses.sqlite_warehouse import SqliteWarehouse

WAREHOUSE_BACKENDS = frozenset({"sqlite", "duckdb"})


def make_warehouse(config: AppConfig) -> BaseWarehouse:
    backend = config.warehouse_backend.strip().lower()
    if backend not in WAREHOUSE_BACKENDS:
        supported = ", ".join(sorted(WAREHOUSE_BACKENDS))
        raise ValueError(f"unsupported warehouse backend: {backend!r}; supported: {supported}")
    if backend == "duckdb":
        from taxi_etl.warehouses.duckdb_warehouse import DuckdbWarehouse

        return DuckdbWarehouse(config)
    return SqliteWarehouse(config)
