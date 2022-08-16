"""SQL client handling.

This includes BigQueryStream and BigQueryConnector.
"""

from typing import List, Tuple

from singer_sdk import SQLConnector, SQLStream


class BigQueryConnector(SQLConnector):
    """Connects to the BigQuery SQL source."""

    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        return f"bigquery://{config['project_id']}"

    def get_object_names(
        self, engine, inspected, schema_name: str
    ) -> List[Tuple[str, bool]]:
        """Return discoverable object names."""
        # Bigquery inspections returns table names in the form
        # `schema_name.table_name` which later results in the project name
        # override due to specifics in behavior of sqlalchemy-bigquery
        #
        # Let's strip `schema_name` prefix on the inspection

        return [
            (table_name.split(".")[-1], is_view)
            for (table_name, is_view) in super().get_object_names(
                engine, inspected, schema_name
            )
        ]

class BigQueryStream(SQLStream):
    """Stream class for BigQuery streams."""

    connector_class = BigQueryConnector
