"""Linear ETL: bronze → silver → validate → gold → validate → log."""

import json
import uuid
from datetime import datetime, timezone

from taxi_etl.config import AppConfig
from taxi_etl.ingest import prepare_bronze
from taxi_etl.logging_setup import setup_logging
from taxi_etl.pipeline_types import PartitionCounts, PipelineRunRecord, RunStatus
from taxi_etl.transform import build_gold, build_silver
from taxi_etl.validate import (
    ValidationError,
    validate_gold,
    validate_silver,
    validation_to_json,
)
from taxi_etl.warehouses import make_warehouse


def run_partition(
    config: AppConfig,
    *,
    year: int,
    month: int,
    skip_download: bool = False,
) -> PipelineRunRecord:
    """Run full ETL for one partition (year + month): bronze → silver → gold."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    logger = setup_logging(logs_dir=config.logs_dir, run_id=run_id)

    record = PipelineRunRecord(
        run_id=run_id,
        year=year,
        month=month,
        status=RunStatus.RUNNING,
        started_at=started_at,
    )
    validation_payload: dict = {}

    try:
        # create the warehouse schema and insert the run record
        with make_warehouse(config) as warehouse:
            warehouse.create_tables()
            warehouse.insert_pipeline_run(record)

            logger.info("run_id=%s year=%s month=%s", run_id, year, month)

            # prepare the bronze layer by downloading the files and adding the ingest metadata
            bronze_paths = prepare_bronze(
                config,
                year=year,
                month=month,
                run_id=run_id,
                skip_download=skip_download,
            )
            logger.info("bronze: %s", bronze_paths[0])
            # build the silver layer by cleaning the bronze layer and adding the silver metadata
            bronze_rows, silver_rows, quarantine_rows = build_silver(
                warehouse,
                year=year,
                month=month,
                run_id=run_id,
                bronze_paths=bronze_paths,
            )
            record.counts = PartitionCounts(
                bronze_rows=bronze_rows,
                silver_rows=silver_rows,
                quarantine_rows=quarantine_rows,
            )
            logger.info(
                "silver bronze=%s silver=%s quarantine=%s",
                bronze_rows,
                silver_rows,
                quarantine_rows,
            )

            silver_validation = validate_silver(warehouse, config, year=year, month=month)
            validation_payload["silver"] = silver_validation.to_dict()
            if not silver_validation.passed:
                raise ValidationError(silver_validation)

            # build the gold layer by transforming the silver layer and adding the gold metadata
            gold_rows = build_gold(warehouse, year=year, month=month, run_id=run_id)
            record.counts.gold_rows = gold_rows
            logger.info("gold rows=%s", gold_rows)

            gold_validation = validate_gold(warehouse, config, year=year, month=month)
            validation_payload["gold"] = gold_validation.to_dict()
            if not gold_validation.passed:
                raise ValidationError(gold_validation)

            record.status = RunStatus.SUCCESS
            record.finished_at = datetime.now(timezone.utc)
            record.validation_json = json.dumps(validation_payload)
            warehouse.update_pipeline_run(record)
            logger.info("success run_id=%s", run_id)
            return record

    except ValidationError as exc:
        record.status = RunStatus.FAILED
        record.finished_at = datetime.now(timezone.utc)
        record.validation_json = validation_to_json(exc.result)
        record.error_message = "validation failed"
        logger.error("validation failed: %s", exc.result.to_dict())
        _save_failed_run(config, record)
        raise

    except Exception as exc:
        record.status = RunStatus.FAILED
        record.finished_at = datetime.now(timezone.utc)
        record.error_message = str(exc)
        if validation_payload:
            record.validation_json = json.dumps(validation_payload)
        logger.exception("pipeline failed: %s", exc)
        _save_failed_run(config, record)
        raise


def _save_failed_run(config: AppConfig, record: PipelineRunRecord) -> None:
    try:
        with make_warehouse(config) as warehouse:
            warehouse.update_pipeline_run(record)
    except Exception:
        pass
