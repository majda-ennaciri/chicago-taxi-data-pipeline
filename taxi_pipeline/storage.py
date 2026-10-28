"""Shared access to the S3-compatible object store hosting the datalake.

The Lake class wraps bucket creation, JSON metadata, Parquet uploads, and
temporary local downloads. Bronze, Silver, and Gold data remain in object
storage; local files are only staging space for Spark and DuckDB.

Connection settings and credentials come from environment variables supplied
by Docker Compose. The local deployment uses SeaweedFS rather than AWS S3.
"""

import json
import os
import time
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


class Lake:
    """Provide storage operations using one configured S3 client and bucket."""

    def __init__(self):
        """Configure the client; bucket existence is checked separately."""
        self.bucket = os.getenv("S3_BUCKET", "chicago-taxi")
        self.client = boto3.client(
            "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://storage:8333"),
            region_name="us-east-1",
            config=Config(retries={"max_attempts": 5}, s3={"addressing_style": "path"}),
        )

    def ensure_bucket(self):
        """Create a missing bucket, retrying connection failures during startup."""
        for attempt in range(20):
            try:
                self.client.head_bucket(Bucket=self.bucket)
                return
            except ClientError as exc:
                if exc.response["Error"]["Code"] in ("404", "NoSuchBucket"):
                    self.client.create_bucket(Bucket=self.bucket)
                    return
                raise
            except EndpointConnectionError:
                if attempt == 19:
                    raise
                time.sleep(2)

    def put_json(self, key, value):
        """Serialize a JSON-compatible value and store it at the given object key."""
        self.client.put_object(Bucket=self.bucket, Key=key,
                               Body=json.dumps(value, indent=2).encode(),
                               ContentType="application/json")

    def get_json(self, key):
        """Read and deserialize a JSON object, closing its response stream."""
        body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"]
        try:
            return json.loads(body.read())
        finally:
            body.close()

    def upload_tree(self, directory, prefix):
        """Upload all Parquet files below a local directory and return their keys.

        Relative directory paths are preserved under prefix. Spark control
        files are excluded. Raise RuntimeError if no Parquet files are found.
        """
        keys = []
        for path in sorted(Path(directory).rglob("*.parquet")):
            key = f"{prefix}/{path.relative_to(directory).as_posix()}"
            self.client.upload_file(str(path), self.bucket, key)
            keys.append(key)
        if not keys:
            raise RuntimeError(f"No Parquet files produced in {directory}")
        return keys

    def download(self, keys, directory):
        """Download object keys to numbered local files and return their paths."""
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        paths = []
        for index, key in enumerate(keys):
            path = directory / f"{index:05d}-{Path(key).name}"
            self.client.download_file(self.bucket, key, str(path))
            paths.append(str(path))
        return paths

    def publish(self, layer, day, manifest):
        """Replace the daily manifest pointing to a completed layer snapshot.

        The caller must finish writing and validating the snapshot first.
        This operation does not provide a transaction across multiple layers.
        """
        # This single small object is published only after all data files exist.
        self.put_json(f"{layer}/date={day}/manifest.json", manifest)

    def manifest(self, layer, day):
        """Return the latest published manifest for a layer and calendar day."""
        return self.get_json(f"{layer}/date={day}/manifest.json")
