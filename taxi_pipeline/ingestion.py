"""Ingest Chicago Taxi Trips from the public SODA API into Bronze.

Download one day at a time using ordered, offset-based pagination. Validate
the CSV header, preserve each response's original bytes in object storage,
and record row counts and SHA-256 hashes in a manifest. Publish the daily
manifest only after every page has been successfully stored.

This module handles HTTP ingestion; it does not clean trips or run Spark.
"""

import csv
import hashlib
import io
import os
from datetime import date, timedelta
from uuid import uuid4

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

SOURCE = "https://data.cityofchicago.org/resource/wrvz-psew.csv"
FIELDS = [
    "trip_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds",
    "trip_miles", "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
]


def download_day(lake, day):
    """Download a complete day and return its published Bronze manifest.

    Args:
        lake: Lake instance providing the S3 client, bucket, and JSON helpers.
        day: Departure date in YYYY-MM-DD format, interpreted as source-local
            calendar time. The next day's midnight is excluded.

    Returns:
        A dictionary containing the source, query, snapshot prefix, total row
        count, and the object keys, row counts, and hashes of downloaded pages.

    Raises:
        ValueError: If the date, configured page size, or CSV header is invalid.
        requests.HTTPError: If an HTTP error remains after configured retries.

    Failed downloads can leave partial snapshot files, but do not replace the
    manifest pointing to the last successfully published Bronze snapshot.
    """
    start = date.fromisoformat(day)
    stop = start + timedelta(days=1)
    page_size = int(os.getenv("PAGE_SIZE", "5000"))
    if not 1 <= page_size <= 50000:
        raise ValueError("PAGE_SIZE must be between 1 and 50000")
    prefix = f"bronze/date={day}/snapshot={uuid4().hex}"
    params = {
        "$select": ",".join(FIELDS),
        "$where": f"trip_start_timestamp >= '{start}T00:00:00' AND "
                  f"trip_start_timestamp < '{stop}T00:00:00'",
        "$order": "trip_start_timestamp,trip_id",
        "$limit": page_size,
    }
    pages, total = [], 0
    retry = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        token = os.getenv("SODA_APP_TOKEN")
        if token:
            session.headers["X-App-Token"] = token
        offset = 0
        while True:
            query = dict(params, **{"$offset": offset})
            response = session.get(SOURCE, params=query, timeout=(15, 180))
            response.raise_for_status()
            content = response.content
            reader = csv.reader(io.StringIO(content.decode("utf-8-sig")))
            header = next(reader, None)
            if header != FIELDS:
                raise ValueError(f"Unexpected CSV schema: {header}")
            count = sum(1 for _ in reader)
            key = f"{prefix}/page-{offset:09d}.csv"
            lake.client.put_object(Bucket=lake.bucket, Key=key, Body=content,
                                   ContentType="text/csv")
            pages.append({"key": key, "rows": count,
                          "sha256": hashlib.sha256(content).hexdigest()})
            total += count
            if count < page_size:
                break
            offset += page_size
    manifest = {"source": SOURCE, "date": day, "query": params, "rows": total,
                "pages": pages, "snapshot": prefix}
    lake.put_json(f"{prefix}/manifest.json", manifest)
    lake.publish("bronze", day, manifest)
    return manifest
