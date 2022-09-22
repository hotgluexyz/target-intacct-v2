"""intacct target class."""

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_intacct.sinks import (
    intacctSink,
)


class Targetintacct(Target):
    """Sample target for intacct."""

    name = "target-intacct"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "company_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "sender_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "sender_password",
            th.StringType,
            required=True
        ),
        th.Property(
            "user_id",
            th.StringType,
            required=True
        ),
        th.Property(
            "user_password",
            th.StringType,
            required=True
        ),
    ).to_dict()

    default_sink_class = intacctSink


if __name__ == "__main__":
    Targetintacct.cli()
