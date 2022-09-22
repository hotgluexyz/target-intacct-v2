"""intacct target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import RecordSink


class intacctSink(RecordSink):
    """intacct target sink class."""

    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        # Sample:
        # ------
        # client.write(record)
