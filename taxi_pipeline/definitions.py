"""Dagster orchestration for the daily Bronze -> Silver -> Gold pipeline.

Define the historical date partitions and asset dependencies. Coordinate
ingestion, temporary local staging, Spark transformations, quality checks,
and snapshot publication in S3-compatible storage. Assets return manifests
describing stored data rather than passing Spark DataFrames between steps.

Business transformations are implemented in processing.py; storage operations
and API ingestion are implemented in storage.py and ingestion.py.
"""

import tempfile
from pathlib import Path
from uuid import uuid4

from dagster import (AssetExecutionContext, DailyPartitionsDefinition, Definitions,
                     RetryPolicy, asset)
from pyspark.sql import functions as F

from taxi_pipeline.ingestion import download_day
from taxi_pipeline.processing import (aggregate_trips, classify_trips, clean_trips,
                                      read_bronze, spark_session)
from taxi_pipeline.storage import Lake

PARTITIONS = DailyPartitionsDefinition(start_date="2023-01-01", end_date="2023-01-08")


@asset(partitions_def=PARTITIONS, group_name="taxi", retry_policy=RetryPolicy(max_retries=2, delay=10))
def bronze(context: AssetExecutionContext):
    """Ingest the selected day and return its published Bronze manifest.

    Ensure the bucket exists, download CSV pages, and expose row count,
    page count, and snapshot location as Dagster output metadata.
    """
    lake = Lake()
    lake.ensure_bucket()
    manifest = download_day(lake, context.partition_key)
    context.add_output_metadata({"rows": manifest["rows"], "pages": len(manifest["pages"]),
                                 "snapshot": manifest["snapshot"]})
    return manifest


@asset(partitions_def=PARTITIONS, group_name="taxi")
def silver(context: AssetExecutionContext, bronze):
    """Clean the referenced Bronze snapshot and publish a Silver manifest.

    Stage CSV files locally, classify and deduplicate trips using Spark,
    reconcile parsed rows with Bronze, and reject empty or wholly invalid
    input. Store accepted and quarantined rows as Parquet alongside a quality
    report recording accepted, rejected, and removed duplicate row counts.
    Stop the Spark session even if processing or publication fails.
    """
    lake, day = Lake(), context.partition_key
    prefix = f"silver/date={day}/snapshot={uuid4().hex}"
    spark = spark_session()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = lake.download([page["key"] for page in bronze["pages"]], root / "raw")
            classified = classify_trips(read_bronze(spark, paths), day).cache()
            clean, rejected = clean_trips(classified)
            clean = clean.cache()
            raw_count, clean_count, rejected_count = classified.count(), clean.count(), rejected.count()
            if raw_count != bronze["rows"]:
                raise ValueError("Bronze manifest row count differs from parsed CSV")
            if raw_count == 0 or clean_count == 0:
                raise ValueError("Empty input or no valid trips; Gold publication blocked")
            quality = {
                "raw_rows": raw_count, "valid_rows": clean_count,
                "rejected_rows": rejected_count,
                "duplicate_rows": raw_count - rejected_count - clean_count,
                "rejection_reasons": {r["rejection_reason"]: r["count"] for r in
                                      rejected.groupBy("rejection_reason").count().collect()},
            }
            clean.write.mode("overwrite").parquet(str(root / "clean"))
            rejected.write.mode("overwrite").parquet(str(root / "rejected"))
            manifest = {
                "date": day, "snapshot": prefix, "bronze_snapshot": bronze["snapshot"],
                "files": lake.upload_tree(root / "clean", f"{prefix}/trips"),
                "quarantine_files": lake.upload_tree(root / "rejected", f"{prefix}/quarantine"),
                "quality": quality,
            }
            lake.put_json(f"{prefix}/quality.json", quality)
            lake.put_json(f"{prefix}/manifest.json", manifest)
            lake.publish("silver", day, manifest)
            context.add_output_metadata({**{k: v for k, v in quality.items() if k != "rejection_reasons"},
                                         "snapshot": prefix})
            return manifest
    finally:
        spark.stop()


@asset(partitions_def=PARTITIONS, group_name="taxi")
def gold(context: AssetExecutionContext, silver):
    """Aggregate the referenced Silver snapshot and publish a Gold manifest.

    Build daily metrics, all-zone totals, and top-zone rankings. Reconcile
    daily and all-zone trip counts and revenue with Silver before uploading
    the tables and publishing their manifest. A failed check preserves the
    previously published Gold manifest. Always stop the Spark session.
    """
    lake, day = Lake(), context.partition_key
    prefix = f"gold/date={day}/snapshot={uuid4().hex}"
    spark = spark_session()
    try:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = lake.download(silver["files"], root / "silver")
            trips = spark.read.parquet(*paths).cache()
            tables = aggregate_trips(trips)
            actual = tables["daily_metrics"].first().asDict()
            expected = trips.agg(F.count("*").alias("count"), F.sum("total_usd").alias("revenue")).first()
            zone_total = tables["pickup_zones"].agg(F.sum("trip_count"), F.sum("total_revenue_usd")).first()
            if (actual["trip_count"] != silver["quality"]["valid_rows"]
                    or actual["trip_count"] != expected["count"]
                    or actual["total_revenue_usd"] != expected["revenue"]
                    or zone_total[0] != expected["count"] or zone_total[1] != expected["revenue"]):
                raise ValueError("Gold/Silver count or revenue reconciliation failed")
            files = {}
            for name, table in tables.items():
                table.coalesce(1).write.mode("overwrite").parquet(str(root / name))
                files[name] = lake.upload_tree(root / name, f"{prefix}/{name}")
            manifest = {"date": day, "snapshot": prefix,
                        "silver_snapshot": silver["snapshot"], "tables": files,
                        "trip_count": actual["trip_count"],
                        "total_revenue_usd": str(actual["total_revenue_usd"])}
            lake.put_json(f"{prefix}/manifest.json", manifest)
            lake.publish("gold", day, manifest)
            context.add_output_metadata({"trip_count": actual["trip_count"],
                                         "total_revenue_usd": str(actual["total_revenue_usd"]),
                                         "snapshot": prefix})
            return manifest
    finally:
        spark.stop()


defs = Definitions(assets=[bronze, silver, gold])
