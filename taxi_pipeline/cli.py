"""Command-line entry point for downloading, processing, and querying taxi data.

Commands:
    download: ingest one day's CSV pages into Bronze, without Spark processing.
    run: execute the Dagster Bronze -> Silver -> Gold assets for a date range.
    query: load one day's published Gold Parquet files into temporary DuckDB
        views and execute SQL without rerunning the pipeline.

Run inside the container with ``python -m taxi_pipeline.cli <command>``.
Date ranges include the start date and exclude the end date.
"""

import argparse
import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import duckdb
from dagster import DagsterInstance, materialize

from taxi_pipeline.definitions import PARTITIONS, bronze, silver, gold
from taxi_pipeline.ingestion import download_day
from taxi_pipeline.storage import Lake


def main():
    """Parse command-line arguments and dispatch to the requested operation."""
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run", help="Materialize all assets, one day at a time")
    run.add_argument("--start", default="2023-01-01")
    run.add_argument("--end", default="2023-01-08", help="Exclusive end date")
    download = sub.add_parser("download", help="Download Bronze only")
    download.add_argument("--date", default="2023-01-01")
    query = sub.add_parser("query", help="Query the published Gold snapshot for a day")
    query.add_argument("--date", default="2023-01-01")
    query.add_argument("--sql", default="SELECT * FROM daily_metrics")
    args = parser.parse_args()
    if args.command == "run":
        current, end = date.fromisoformat(args.start), date.fromisoformat(args.end)
        if current >= end:
            parser.error("--start must precede --end (exclusive)")
        allowed = set(PARTITIONS.get_partition_keys())
        days = []
        while current < end:
            day = current.isoformat()
            if day not in allowed:
                parser.error(f"Date {day} outside the configured partition range")
            days.append(day)
            current += timedelta(days=1)
        with DagsterInstance.get() as instance:
            for day in days:
                result = materialize([bronze, silver, gold], partition_key=day, instance=instance)
                if not result.success:
                    raise RuntimeError(f"Pipeline failed for {day}")
                print(f"SUCCESS {day}", flush=True)
    elif args.command == "download":
        date.fromisoformat(args.date)
        lake = Lake()
        lake.ensure_bucket()
        print(json.dumps(download_day(lake, args.date), indent=2))
    else:
        date.fromisoformat(args.date)
        lake = Lake()
        manifest = lake.manifest("gold", args.date)
        with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as connection:
            for name, keys in manifest["tables"].items():
                paths = lake.download(keys, Path(tmp) / name)
                connection.read_parquet(paths).create_view(name)
            result = connection.sql(args.sql)
            print(result)


if __name__ == "__main__":
    main()
