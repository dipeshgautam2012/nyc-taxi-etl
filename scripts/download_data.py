#!/usr/bin/env python3
"""Download TLC Parquet and reference files into bronze."""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from taxi_etl.config import load_config
from taxi_etl.ingest import download_trips, download_zone_lookup


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download NYC TLC data into bronze.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args(argv)

    config = load_config(ROOT / "config.toml")
    zone = download_zone_lookup(config)
    trips = download_trips(config, year=args.year, month=args.month)
    print(f"zone lookup: {zone}")
    print(f"trips: {trips}")


if __name__ == "__main__":
    main()
