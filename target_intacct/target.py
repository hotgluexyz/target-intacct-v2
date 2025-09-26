"""intacct target class."""

from singer_sdk import typing as th
from singer_sdk.target_base import Target
from target_hotglue.target import TargetHotglue
from target_intacct.sinks import BillPaymentsSink, intacctSink


class Targetintacct(TargetHotglue):
    """Sample target for intacct."""

    name = "target-intacct"
    config_jsonschema = th.PropertiesList(
        th.Property("company_id", th.StringType, required=True),
        th.Property("sender_id", th.StringType, required=True),
        th.Property("sender_password", th.StringType, required=True),
        th.Property("user_id", th.StringType, required=True),
        th.Property("user_password", th.StringType, required=True),
    ).to_dict()

    default_sink_class = intacctSink
    SINK_TYPES = [BillPaymentsSink, intacctSink]

    def get_sink_class(self, stream_name: str):
        """Get sink for a stream.
        """
        if stream_name == "BillPayments":
            return BillPaymentsSink
        return intacctSink


if __name__ == "__main__":
    Targetintacct.cli()
