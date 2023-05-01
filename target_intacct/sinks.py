"""intacct target sink class, which handles writing streams."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Union

from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import RecordSink

from target_intacct.mapping import UnifiedMapping

from .client import SageIntacctSDK, get_client
from .const import DEFAULT_API_URL, KEY_PROPERTIES, REQUIRED_CONFIG_KEYS

# import xmltodict



class intacctSink(RecordSink):
    """intacct target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        super().__init__(target, stream_name, schema, key_properties)

        self.target_name = "intacct-v2"

        self.client = get_client(
            api_url=target.config.get("api_url", DEFAULT_API_URL),
            company_id=target.config["company_id"],
            sender_id=target.config["sender_id"],
            sender_password=target.config["sender_password"],
            user_id=target.config["user_id"],
            user_password=target.config["user_password"],
            headers={"User-Agent": target.config["user_agent"]}
            if "user_agent" in target.config
            else {},
        )

        # Lookup for vendors
        vendors = self.client.get_entity(
            object_type="accounts_payable_vendors", fields=["VENDORID", "NAME"]
        )
        self.vendors = self.dictify(vendors, "NAME", "VENDORID")

        # Lookup for Locations
        locations = self.client.get_entity(
            object_type="locations", fields=["LOCATIONID", "NAME"]
        )
        self.locations = self.dictify(locations, "NAME", "LOCATIONID")

        # Lookup for accounts
        accounts = self.client.get_entity(object_type='general_ledger_accounts',fields=["RECORDNO", "ACCOUNTNO", "TITLE"])
        self.accounts = self.dictify(accounts,"TITLE","ACCOUNTNO")

        # Lookup for items
        items = self.client.get_entity(object_type="item", fields=["ITEMID", "NAME"])
        self.items = self.dictify(items, "NAME", "ITEMID")

    def dictify(sefl, array, key, value):
        array_ = {}
        for i in array:
            array_[i[key]] = i[value]
        return array_

    def purchase_invoices_upload(self, record):

        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "purchase_invoices", self.target_name)

        # Get the matching values for the payload :
        # Matching "VENDORNAME" -> "VENDORID"
        if payload.get("VENDORNAME"):
            payload["VENDORID"] = self.vendors[payload["VENDORNAME"]]
            payload.pop("VENDORNAME")

        # Matching ""
        for item in payload.get("APBILLITEMS").get("APBILLITEM"):
            if payload.get("LOCATIONNAME"):
                item["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]

            # TODO For now the account number is set by hand.
            # item["ACCOUNTNO"] = "6220"
            if item.get("ACCOUNTNAME") and self.accounts.get(item['ACCOUNTNAME']):
                item["ACCOUNTNO"] = self.accounts.get(item['ACCOUNTNAME'])
            else:
                raise Exception(f"ERROR: ACCOUNTNAME not found. \n Intaccts Requires an ACCOUNTNAME associated with each line item")

            if payload.get("ITEMNAME") and self.items.get(payload.get("ITEMNAME")):
                item["ITEMID"] = self.items.get(payload.get("ITEMNAME"))
                item.pop("ITEMNAME")

        payload.pop("LOCATIONNAME")

        payload["WHENCREATED"] = payload["WHENCREATED"].split("T")[0]

        data = {"create": {"object": "accounts_payable_bills", "APBILL": payload}}

        self.client.format_and_send_request(data)

    def process_record(self, record: dict, context: dict) -> None:
        if self.stream_name in ["PurchaseInvoices", "Bills"]:
            self.purchase_invoices_upload(record)
