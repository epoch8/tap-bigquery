"""BigQuery tap class."""

from typing import List, Type

from singer_sdk import SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from tap_bigquery.client import BigQueryStream


class TapBigQuery(SQLTap):
    """BigQuery tap class."""
    name = "tap-bigquery"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "project_id",
            th.StringType,
            required=True,
            description="GCP Project"
        ),
    ).to_dict()

    default_stream_class: Type[SQLStream] = BigQueryStream
