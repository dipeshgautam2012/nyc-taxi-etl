"""Default warehouse — SQLite file at warehouse/taxi.db."""

import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from taxi_etl.config import AppConfig
from taxi_etl.pipeline_types import PipelineRunRecord
from taxi_etl.schema import ddl_sqlite
from taxi_etl.warehouses.base_warehouse import BaseWarehouse


def _sqlite_type(arrow_type: pa.DataType) -> str:
    if pa.types.is_integer(arrow_type):
        return "INTEGER"
    if pa.types.is_floating(arrow_type):
        return "REAL"
    if pa.types.is_boolean(arrow_type):
        return "INTEGER"
    if pa.types.is_timestamp(arrow_type):
        return "TEXT"
    return "TEXT"


class SqliteWarehouse(BaseWarehouse):
    dialect = "sqlite"

    def __init__(self, config: AppConfig) -> None:
        super().__init__(config)
        self._conn: sqlite3.Connection | None = None

    def connect(self) -> None:
        self.config.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.config.warehouse_path)
        self._conn.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("warehouse not connected")
        return self._conn

    def create_tables(self) -> None:
        for stmt in ddl_sqlite():
            self.conn.execute(stmt)
        self.conn.commit()

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.conn.execute(sql, params)
        self.conn.commit()

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        self.conn.executemany(sql, rows)
        self.conn.commit()

    def scalar(self, sql: str, params: tuple = ()) -> int | float | str | None:
        cur = self.conn.execute(sql, params)
        row = cur.fetchone()
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
        total = 0
        col_names: list[str] = []
        insert_sql = ""

        for path in paths:
            table = pq.ParquetFile(path).read()
            if not col_names:
                schema = table.schema
                col_names = schema.names
                cols_sql = ", ".join(f'"{c}"' for c in col_names)
                col_defs = ", ".join(
                    f'"{name}" {_sqlite_type(schema.field(i).type)}'
                    for i, name in enumerate(col_names)
                )
                self.execute(f"CREATE TABLE {staging_table} ({col_defs})")
                placeholders = ", ".join("?" for _ in col_names)
                insert_sql = f'INSERT INTO {staging_table} ({cols_sql}) VALUES ({placeholders})'

            batch = [tuple(row.get(c) for c in col_names) for row in table.to_pylist()]
            self.executemany(insert_sql, batch)
            total += len(batch)
        return total
