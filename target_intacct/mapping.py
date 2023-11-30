import json
import os
import requests
import base64

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class UnifiedMapping:
    def __init__(self) -> None:
        pass

    def read_json_file(self, filename):
        # read file
        with open(os.path.join(__location__, f"{filename}"), "r") as filetoread:
            data = filetoread.read()

        # parse file
        content = json.loads(data)

        return content

    # Microsoft dynamics address mapping
    def map_address(self, address, address_mapping, payload):
        if isinstance(address, str):
            address = json.loads(address)
        if isinstance(address, dict):
            for key, value in address.items():
                if key in address_mapping.keys():
                    payload[address_mapping[key]] = value
        return payload

    def map_lineItems(self, line_items, line_items_mapping):
        line_items_ = []
        if isinstance(line_items, str):
            line_items = json.loads(line_items)
        if isinstance(line_items, list):
            if len(line_items) > 0:
                line_items_ = []
                for item in line_items:
                    item_ = {}
                    for key, value in item.items():
                        if key in line_items_mapping[0].keys():
                            item_[line_items_mapping[0][key]] = value
                    payload_return = {}
                    for key in item_.keys():
                        if key is not None:
                            payload_return[key] = item_[key]
                    line_items_ += [payload_return]

        return line_items_

    def map_custom_fields(self, payload, fields):
        # Populate custom fields.
        for key, val in fields:
            payload[key] = val
        return payload

    def prepare_payload(self, record, endpoint="invoice", target="intacct"):
        mapping = self.read_json_file(f"mapping_{target}.json")
        ignore = mapping["ignore"]
        mapping = mapping[endpoint]
        payload = {}
        payload_return = {}
        lookup_keys = mapping.keys()
        for lookup_key in lookup_keys:
            if (
                lookup_key == "address"
                or lookup_key == "addresses"
                and target == "intacct-v2"
            ):
                payload = self.map_address(
                    record.get(lookup_key, []), mapping[lookup_key], payload
                )
            elif lookup_key == "lineItems" and endpoint == "apadjustment":
                payload["APADJUSTMENTITEMS"] = {"LINEITEM": []}
                lines = self.map_lineItems(
                    record.get(lookup_key, []), mapping[lookup_key]
                )
                payload["APADJUSTMENTITEMS"]["LINEITEM"] = payload["APADJUSTMENTITEMS"]["LINEITEM"] + lines
            elif (lookup_key == "lineItems" or lookup_key == "expenses") and target == "intacct-v2":
                payload["APBILLITEMS"] = {"APBILLITEM": []}
                lines = self.map_lineItems(
                    record.get(lookup_key, []), mapping[lookup_key]
                )
                payload["APBILLITEMS"]["APBILLITEM"] = payload["APBILLITEMS"]["APBILLITEM"] + lines
            elif lookup_key == "lines" and target == "intacct-v2":
                payload["ENTRIES"] = {"GLENTRY": []}
                lines = self.map_lineItems(
                    record.get(lookup_key, []), mapping[lookup_key]
                )
                payload["ENTRIES"]["GLENTRY"] = lines
            elif "date" in lookup_key.lower():
                val = record.get(lookup_key)
                if val:
                    payload[mapping[lookup_key]] = val.split("T")[0]
                else:
                    val = ""
            else:
                val = record.get(lookup_key, "")
                if val:
                    payload[mapping[lookup_key]] = val

        for key in payload.keys():
            if key not in ignore and key is not None:
                payload_return[key] = payload[key]
        return payload_return

    def prepare_attachment_payload(self, data, action="create", existing_attachments=[]):
        attachments = data.get("attachments", [])
        invoice_number = data.get("invoiceNumber")

        for attachment in attachments:
            url = attachment.get("url")
            if url:
                response = requests.get(url)
                data = base64.b64encode(response.content)
                data = data.decode()
                attachment["data"] = data

        attachment_payload = [{"attachment":{
            "attachmentname": att.get("name"),
            "attachmenttype": "pdf",
            "attachmentdata": att.get("data"),
            }} for att in attachments if att.get("name") not in existing_attachments]

        payload = {
            f"{action}_supdoc": {
                "object": "supdoc",
                "supdocid": invoice_number[-20:], #only 20 chars allowed
                "supdocname": invoice_number,
                "supdocfoldername": invoice_number,
                "attachments": attachment_payload
            }
        }
        if attachment_payload:
            return payload
        return None