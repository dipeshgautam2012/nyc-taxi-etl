"""Optional warehouse — DuckDB file; native Parquet reads in SQL."""

from pathlib import Path

import duckdb

from taxi_etl.config import AppConfig
from taxi_etl.pipeline_types import PipelineRunRecord
from taxi_etl.schema import ddl_duckdb
from taxi_etl.warehouses.base_warehouse import BaseWarehouse


class DuckdbWarehouse(BaseWarehouse):
    dialect = "duckdb"

    def __init__(self, config: AppConfig) -> None:
        super().__init__(config)
        self._conn: duckdb.DuckDBPyConnection | None = None

    def connect(self) -> None:
        self.config.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self.config.warehouse_path))

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("warehouse not connected")
        return self._conn

    def create_tables(self) -> None:
        for stmt in ddl_duckdb():
            self.conn.execute(stmt)

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.execute(sql, params)

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        self.conn.executemany(sql, rows)

    def scalar(self, sql: str, params: tuple = ()) -> int | float | str | None:
        row = self.conn.execute(sql, params).fetchone()
        if not row:
            return None
        return row[0]

    def insert_pipeline_run(self, record: PipelineRunRecord) -> None:
        self.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, year, month, status, started_at,
                bronze_rows, silver_rows, quarantine_rows, gold_rows
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.run_id,
                record.year,
                record.month,
                record.status.value,
                record.started_at.isoformat(),
                record.counts.bronze_rows,
                record.counts.silver_rows,
                record.counts.quarantine_rows,
                record.counts.gold_rows,
            ),
        )

    def update_pipeline_run(self, record: PipelineRunRecord) -> None:
        self.execute(
            """
            UPDATE pipeline_runs SET
                status = ?,
                finished_at = ?,
                bronze_rows = ?,
                silver_rows = ?,
                quarantine_rows = ?,
                gold_rows = ?,
                validation_json = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            (
                record.status.value,
                record.finished_at.isoformat() if record.finished_at else None,
                record.counts.bronze_rows,
                record.counts.silver_rows,
                record.counts.quarantine_rows,
                record.counts.gold_rows,
                record.validation_json,
                record.error_message,
                record.run_id,
            ),
        )

    def load_bronze_parquet(self, paths: list[Path], *, staging_table: str) -> int:
        self.execute(f"DROP TABLE IF EXISTS {staging_table}")
        if not paths:
            return 0
        if len(paths) == 1:
            source: str | list[str] = str(paths[0].resolve())
        else:
            source = [str(path.resolve()) for path in paths]
        self.execute(
            f"CREATE TABLE {staging_table} AS SELECT * FROM read_parquet(?)",
            (source,),
        )
        return self.scalar_int(f"SELECT COUNT(*) FROM {staging_table}")
