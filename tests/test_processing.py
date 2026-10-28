"""Spark tests for Silver quality rules and Gold calculations.

Use small synthetic DataFrames containing duplicates, invalid values, and
unknown zones. Verify accepted/rejected counts, revenue, duration averages,
zone totals, and transformation behavior with empty input. No API or S3
connection is required; tests use a local Spark session.
"""

from decimal import Decimal

import pytest
from pyspark.sql import types as T

from taxi_pipeline.ingestion import FIELDS
from taxi_pipeline.processing import aggregate_trips, classify_trips, clean_trips, spark_session


@pytest.fixture(scope="module")
def spark():
    """Share a local Spark session across this module and stop it after testing."""
    session = spark_session()
    yield session
    session.stop()


def row(trip_id, **overrides):
    """Create a source-format test row, with optional overrides for invalid cases."""
    values = dict.fromkeys(FIELDS)
    values.update(trip_id=trip_id, trip_start_timestamp="2023-01-01T10:00:00.000",
                  trip_end_timestamp="2023-01-01T10:15:00.000", trip_seconds="600",
                  trip_miles="2.5", trip_total="15.50", pickup_community_area="8",
                  payment_type="Cash")
    values.update(overrides)
    return tuple(values[field] for field in FIELDS)


def test_validation_dedup_and_gold_reconcile(spark):
    """Verify quality rules, deduplication, and expected metrics on imperfect trips."""
    rows = [row("A"), row("A"), row("B", pickup_community_area=None, trip_total="20.25"),
            row("C", trip_seconds="-1"), row("D", trip_total="broken"),
            row("E", trip_start_timestamp="2023-01-02T10:00:00.000"),
            row("F", trip_end_timestamp="2023-01-01T09:00:00.000"),
            row("", trip_total="1")]
    schema = T.StructType([T.StructField(c, T.StringType()) for c in FIELDS])
    classified = classify_trips(spark.createDataFrame(rows, schema), "2023-01-01")
    clean, rejected = clean_trips(classified)
    assert clean.count() == 2
    assert rejected.count() == 5
    assert clean.filter("pickup_zone IS NULL").count() == 1
    reasons = ";".join(r.rejection_reason for r in rejected.collect())
    for reason in ["invalid_duration", "invalid_total", "outside_partition", "end_before_start", "missing_trip_id"]:
        assert reason in reasons
    tables = aggregate_trips(clean)
    daily = tables["daily_metrics"].first()
    assert daily.trip_count == 2
    assert daily.total_revenue_usd == Decimal("35.75")
    assert daily.average_duration_seconds == 600
    assert daily.unknown_pickup_count == 1
    assert sum(r.trip_count for r in tables["pickup_zones"].collect()) == 2
    assert tables["top_pickup_zones"].count() == 1


def test_empty_input_retains_writable_schemas(spark):
    """Check that empty input produces empty, structured transformation outputs."""
    schema = T.StructType([T.StructField(c, T.StringType()) for c in FIELDS])
    classified = classify_trips(spark.createDataFrame([], schema), "2023-01-01")
    clean, rejected = clean_trips(classified)
    assert clean.count() == rejected.count() == 0
    assert aggregate_trips(clean)["daily_metrics"].count() == 0
