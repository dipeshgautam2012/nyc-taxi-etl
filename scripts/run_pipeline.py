#!/usr/bin/env python3
"""Run bronze → silver → gold pipeline for one year/month partition."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from taxi_etl.config import load_config
from taxi_etl.pipeline import run_partition


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run NYC taxi ETL for one partition.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--skip-download", action="store_true")
    args = parser.parse_args(argv)

    config = load_config(ROOT / "config.toml")
    record = run_partition(
        config,
        year=args.year,
        month=args.month,
        skip_download=args.skip_download,
    )
    print(
        f"status={record.status.value} run_id={record.run_id} "
        f"silver={record.counts.silver_rows} gold={record.counts.gold_rows}"
    )


if __name__ == "__main__":
    main()
