"""Partition-level quality gates (not per-row silver cleaning).

After build_silver / build_gold, validate_silver and validate_gold check the whole year/month
partition. Failed checks raise ValidationError.

Row-level rules: transform_sql.valid_where (during build_silver → rejects_quarantine).
Thresholds: AppConfig.min_rows_per_month, AppConfig.max_null_pickup_pct.
"""

import json

from taxi_etl.config import AppConfig
from taxi_etl.pipeline_types import CheckResult, ValidationResult
from taxi_etl.schema import TABLE_DIM_LOCATION, TABLE_FACT_RIDES, TABLE_SILVER_TRIPS
from taxi_etl.warehouses.base_warehouse import BaseWarehouse


class ValidationError(Exception):
    """Raised when a quality gate fails; carries ValidationResult for validation_json."""

    def __init__(self, result: ValidationResult) -> None:
        self.result = result
        super().__init__(result.to_dict())


def validate_silver(
    warehouse: BaseWarehouse,
    config: AppConfig,
    *,
    year: int,
    month: int,
) -> ValidationResult:
    """Quality gate after build_silver — fail the run before gold is built.

    Checks (all must pass):
      silver_min_rows        — COUNT(silver_trips) >= min_rows_per_month
      silver_null_pickup_pct — share of null tpep_pickup_datetime <= max_null_pickup_pct
    """
    checks: list[CheckResult] = []

    silver_rows = warehouse.scalar_int(
        f"SELECT COUNT(*) FROM {TABLE_SILVER_TRIPS} WHERE year = ? AND month = ?",
        (year, month),
    )
    checks.append(
        CheckResult(
            name="silver_min_rows",
            passed=silver_rows >= config.min_rows_per_month,
            detail=f"silver_rows={silver_rows}, min={config.min_rows_per_month}",
        )
    )

    null_pickup = warehouse.scalar_int(
        f"""
        SELECT COUNT(*) FROM {TABLE_SILVER_TRIPS}
        WHERE year = ? AND month = ? AND tpep_pickup_datetime IS NULL
        """,
        (year, month),
    )
    null_pct = (null_pickup / silver_rows) if silver_rows else 0.0
    checks.append(
        CheckResult(
            name="silver_null_pickup_pct",
            passed=null_pct <= config.max_null_pickup_pct,
            detail=f"null_pickup_pct={null_pct:.4f}, max={config.max_null_pickup_pct}",
        )
    )

    return ValidationResult(passed=all(c.passed for c in checks), checks=checks)


def validate_gold(
    warehouse: BaseWarehouse,
    config: AppConfig,
    *,
    year: int,
    month: int,
) -> ValidationResult:
    """Quality gate after build_gold — fail the run if analytics tables look broken.

    Checks (all must pass):
      gold_min_rows      — COUNT(fact_rides) >= min_rows_per_month
      gold_location_fks  — every pickup/dropoff location_id in fact_rides exists in dim_location
    """
    checks: list[CheckResult] = []

    gold_rows = warehouse.scalar_int(
        f"SELECT COUNT(*) FROM {TABLE_FACT_RIDES} WHERE year = ? AND month = ?",
        (year, month),
    )
    checks.append(
        CheckResult(
            name="gold_min_rows",
            passed=gold_rows >= config.min_rows_per_month,
            detail=f"gold_rows={gold_rows}, min={config.min_rows_per_month}",
        )
    )

    orphans = warehouse.scalar_int(
        f"""
        SELECT COUNT(*) FROM {TABLE_FACT_RIDES} f
        LEFT JOIN {TABLE_DIM_LOCATION} p ON f.pickup_location_id = p.location_id
        LEFT JOIN {TABLE_DIM_LOCATION} d ON f.dropoff_location_id = d.location_id
        WHERE f.year = ? AND f.month = ?
          AND (p.location_id IS NULL OR d.location_id IS NULL)
        """,
        (year, month),
    )
    checks.append(
        CheckResult(
            name="gold_location_fks",
            passed=orphans == 0,
            detail=f"orphan_location_fks={orphans}",
        )
    )

    return ValidationResult(passed=all(c.passed for c in checks), checks=checks)


def validation_to_json(result: ValidationResult) -> str:
    return json.dumps(result.to_dict())
