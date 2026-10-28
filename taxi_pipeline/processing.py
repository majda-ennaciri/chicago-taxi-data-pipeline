"""PySpark transformations for Silver cleaning and Gold business metrics.

Read Bronze CSV fields as strings, convert them to explicit analytical types,
flag invalid trips, and separate valid deduplicated trips from quarantine.
Build daily metrics, pickup-zone totals, and the top ten known pickup zones.

Functions operate on local paths or Spark DataFrames. Dagster orchestration,
object storage transfers, and publication checks live in definitions.py.
"""

from pyspark.sql import SparkSession, Window, functions as F, types as T

from taxi_pipeline.ingestion import FIELDS


def spark_session():
    """Create or reuse a local two-thread Spark session configured for Chicago time."""
    spark = (SparkSession.builder.master("local[2]").appName("chicago-taxi")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.sql.session.timeZone", "America/Chicago")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze(spark, paths):
    """Read staged CSV files into a DataFrame with explicit string columns.

    Type conversion happens during classification so invalid values can be
    flagged. Malformed CSV records fail parsing rather than being skipped.
    """
    schema = T.StructType([T.StructField(name, T.StringType()) for name in FIELDS])
    return (spark.read.schema(schema).option("header", True)
            .option("mode", "FAILFAST").option("multiLine", True).csv(paths))


def classify_trips(raw, day):
    """Add typed columns and rejection reasons without filtering out rows.

    Check IDs, timestamps, departure date, chronology, positive duration,
    nonnegative distance, and nonnegative total amount. Multiple rejection
    reasons are separated by semicolons; an empty string means no rejection.
    Missing or out-of-range community areas become NULL without rejection.
    Source columns are retained, with trip_id trimmed of surrounding spaces.

    Args:
        raw: Bronze DataFrame with the source columns defined in FIELDS.
        day: Expected departure date in YYYY-MM-DD format.
    """
    df = (raw.withColumn("trip_id", F.trim("trip_id"))
          .withColumn("started_at", F.to_timestamp("trip_start_timestamp"))
          .withColumn("ended_at", F.to_timestamp("trip_end_timestamp"))
          .withColumn("duration_seconds", F.col("trip_seconds").cast("long"))
          .withColumn("distance_miles", F.col("trip_miles").cast("decimal(12,3)"))
          .withColumn("total_usd", F.col("trip_total").cast("decimal(14,2)")))
    for name in ["fare", "tips", "tolls", "extras"]:
        df = df.withColumn(f"{name}_usd", F.col(name).cast("decimal(14,2)"))
    for side in ["pickup", "dropoff"]:
        zone = F.col(f"{side}_community_area").cast("int")
        df = df.withColumn(f"{side}_zone", F.when(zone.between(1, 77), zone))
    rules = [
        (F.col("trip_id").isNull() | (F.col("trip_id") == ""), "missing_trip_id"),
        (F.col("started_at").isNull() | F.col("ended_at").isNull(), "invalid_timestamp"),
        (F.col("started_at").isNotNull() &
         (F.to_date("started_at") != F.lit(day).cast("date")), "outside_partition"),
        (F.col("ended_at") < F.col("started_at"), "end_before_start"),
        (F.col("duration_seconds").isNull() | (F.col("duration_seconds") <= 0), "invalid_duration"),
        (F.col("distance_miles").isNull() | (F.col("distance_miles") < 0), "invalid_distance"),
        (F.col("total_usd").isNull() | (F.col("total_usd") < 0), "invalid_total"),
    ]
    return df.withColumn("rejection_reason", F.concat_ws(";", *[
        F.when(condition, F.lit(reason)) for condition, reason in rules
    ]))


def clean_trips(classified):
    """Return (silver, rejected) DataFrames from classified trips.

    Silver contains typed, normalized columns and one valid row per trip_id.
    Conflicting duplicates are ordered by their source-field JSON so selection
    is reproducible; this does not identify the newest record. Rejected rows
    retain their rejection reasons. Removed duplicates remain in Bronze and
    are counted separately by the orchestrator, not placed in quarantine.
    """
    candidates = classified.filter(F.col("rejection_reason") == "")
    # Deterministic tie breaker even when duplicate IDs contain different values.
    order = F.to_json(F.struct(*[F.col(c) for c in sorted(FIELDS)]))
    ranked = candidates.withColumn("_rank", F.row_number().over(
        Window.partitionBy("trip_id").orderBy(order)))
    silver = ranked.filter("_rank = 1").select(
        "trip_id", "started_at", "ended_at", "duration_seconds", "distance_miles",
        "pickup_zone", "dropoff_zone", "fare_usd", "tips_usd", "tolls_usd",
        "extras_usd", "total_usd",
        F.coalesce(F.upper(F.trim("payment_type")), F.lit("UNKNOWN")).alias("payment_type"),
        F.coalesce(F.trim("company"), F.lit("UNKNOWN")).alias("company"),
        F.to_date("started_at").alias("trip_date"),
    )
    rejected = classified.filter(F.col("rejection_reason") != "")
    return silver, rejected


def aggregate_trips(silver):
    """Return three Gold DataFrames calculated from accepted Silver trips.

    daily_metrics: trip count, declared total revenue in USD, average duration
        in seconds, average distance in miles, and unknown pickup-zone count.
    pickup_zones: counts and revenue by departure date and pickup zone,
        including an unknown-zone group.
    top_pickup_zones: up to ten known zones per day, ranked by trip count
        descending and zone number ascending to break ties.

    Averages are calculated over individual trips, not over daily averages.
    """
    daily = silver.groupBy("trip_date").agg(
        F.count("*").alias("trip_count"), F.sum("total_usd").alias("total_revenue_usd"),
        F.avg("duration_seconds").alias("average_duration_seconds"),
        F.avg("distance_miles").alias("average_distance_miles"),
        F.sum(F.when(F.col("pickup_zone").isNull(), 1).otherwise(0)).alias("unknown_pickup_count"),
    )
    by_zone = silver.groupBy("trip_date", "pickup_zone").agg(
        F.count("*").alias("trip_count"), F.sum("total_usd").alias("total_revenue_usd"))
    known = by_zone.filter(F.col("pickup_zone").isNotNull())
    top_zones = known.withColumn("rank", F.row_number().over(
        Window.partitionBy("trip_date").orderBy(F.desc("trip_count"), "pickup_zone")
    )).filter("rank <= 10")
    return {"daily_metrics": daily, "pickup_zones": by_zone, "top_pickup_zones": top_zones}
