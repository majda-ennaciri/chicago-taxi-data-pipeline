"""Unit tests for Bronze ingestion without a live API or object store.

Mock HTTP and S3 interactions to check pagination offsets, original CSV bytes,
row counts, and refusal to publish a manifest when the CSV schema is invalid.
"""

import csv
import io
from unittest.mock import Mock, patch

import pytest

from taxi_pipeline.ingestion import FIELDS, download_day


def response(rows):
    """Build a mock HTTP response containing a CSV header and synthetic rows."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(FIELDS)
    writer.writerows([[str(i)] + [""] * (len(FIELDS) - 1) for i in rows])
    result = Mock()
    result.content = output.getvalue().encode()
    return result


def test_pages_and_exact_bytes(monkeypatch):
    """Check two-page ingestion, row counting, byte preservation, and publication."""
    monkeypatch.setenv("PAGE_SIZE", "2")
    pages = [response([1, 2]), response([3])]
    lake = Mock()
    with patch("taxi_pipeline.ingestion.requests.Session") as factory:
        session = factory.return_value.__enter__.return_value
        session.get.side_effect = pages
        manifest = download_day(lake, "2023-01-01")
    assert manifest["rows"] == 3
    assert [c.kwargs["params"]["$offset"] for c in session.get.call_args_list] == [0, 2]
    assert lake.client.put_object.call_args_list[0].kwargs["Body"] == pages[0].content
    lake.publish.assert_called_once()


def test_schema_failure_does_not_publish():
    """Ensure an unexpected header raises an error without publishing a manifest."""
    lake = Mock()
    bad = Mock(content=b"unexpected\nvalue\n")
    with patch("taxi_pipeline.ingestion.requests.Session") as factory:
        factory.return_value.__enter__.return_value.get.return_value = bad
        with pytest.raises(ValueError, match="schema"):
            download_day(lake, "2023-01-01")
    lake.publish.assert_not_called()
