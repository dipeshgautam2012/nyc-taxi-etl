"""Analytics warehouse contract — SQLite default, DuckDB optional."""

from abc import ABC, abstractmethod
from pathlib import Path

from taxi_etl.config import AppConfig
from taxi_etl.pipeline_types import PipelineRunRecord


class BaseWarehouse(ABC):
    """Warehouse contract. dialect is "sqlite" or "duckdb"; set on each concrete class."""
    dialect: str

    def __init__(self, config: AppConfig) -> None:
        self.config = config

    @abstractmethod
    def connect(self) -> None:
        ...

    @abstractmethod
    def close(self) -> None:
        ...

    def __enter__(self) -> "BaseWarehouse":
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @abstractmethod
    def create_tables(self) -> None:
        ...

    @abstractmethod
    def execute(self, sql: str, params: tuple = ()) -> None:
        ...

    @abstractmethod
    def executemany(self, sql: str, rows: list[tuple]) -> None:
        ...

    @abstractmethod
    def scalar(self, sql: str, params: tuple = ()) -> int | float | str | None:
        ...

    def scalar_int(self, sql: str, params: tuple = ()) -> int:
        value = self.scalar(sql, params)
        if value is None:
            return 0
        return int(value)

    @abstractmethod
    def insert_pipeline_run(self, record: PipelineRunRecord) -> None:
        ...

    @abstractmethod
    def update_pipeline_run(self, record: PipelineRunRecord) -> None:
        ...

    @abstractmethod
    def load_bronze_parquet(self, paths: list[Path], *, staging_table: str) -> int:
        ...
