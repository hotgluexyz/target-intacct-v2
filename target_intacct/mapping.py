import json
import os
import requests
import base64
import ast

__location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class UnifiedMapping:
    def __init__(self, config=None) -> None:
        self.config = config
        pass

    def read_json_file(self, filename):
        # read file
        with open(os.path.join(__location__, f"{filename}"), "r") as filetoread:
            data = filetoread.read()

        # parse file
        content = json.loads(data)

        return content

    def parse_objs(self, obj):
        try:
            return json.loads(obj)
        except:
            return ast.literal_eval(obj)
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
        if isinstance(line_items, dict):
            line_items = [line_items]

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

    def order_dicts(self, dict, keys_order):
        new_dict = {key: dict.get(key, None) for key in keys_order if key in dict}
        new_dict.update({key: dict[key] for key in dict if key not in keys_order})
        return new_dict

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
                payload["apadjustmentitems"] = {"lineitem": []}
                lines = self.map_lineItems(
                    record.get(lookup_key, []), mapping[lookup_key]
                )
                payload["apadjustmentitems"]["lineitem"] = payload["apadjustmentitems"]["lineitem"] + lines
            elif (lookup_key == "lineItems" or lookup_key == "expenses") and target == "intacct-v2":
                # expenses and lineItems are mapped to APBILLITEM, if APBILLITEMS has data add new lines there
                if not payload.get("APBILLITEMS", {}).get("APBILLITEM"):
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

    def get_attachment_type(self, att_name):
        try:
            return att_name.split(".")[1]
        except:
            return "pdf"

    def prepare_attachment_payload(self, data, action="create", existing_attachments=[]):
        attachments = data.get("attachments", [])
        invoice_number = data.get("invoiceNumber")
        supdoc_id = str(invoice_number)[-20:].strip("-") # supdocid only allows 20 chars

        if isinstance(attachments, str):
            attachments = self.parse_objs(attachments)

        for attachment in attachments:
            url = attachment.get("url")
            if url:
                response = requests.get(url)
                data = base64.b64encode(response.content)
                data = data.decode()
                attachment["data"] = data
            else:
                att_path = f"{self.config.get('input_path')}/{attachment.get('id')}_{attachment.get('name')}"
                with open(att_path, "rb") as attach_file:
                    data = base64.b64encode(attach_file.read()).decode()
                    attachment["data"] = data

        attachment_payload = {"attachment": [{
            "attachmentname": f'{att.get("id")}_{att.get("name")}' if att.get("id") else att.get("name"),
            "attachmenttype": self.get_attachment_type(att.get("name")),
            "attachmentdata": att.get("data"),
            } for att in attachments if att.get("data") not in existing_attachments]}

        payload = {
            f"{action}_supdoc": {
                "object": "supdoc",
                "supdocid": supdoc_id, #only 20 chars allowed
                "supdocname": invoice_number,
                "supdocfoldername": supdoc_id, # we name the folder the same as the supdoc for easy correlation
                "attachments": attachment_payload
            }
        }
        if attachment_payload.get("attachment"):
            return payload
        return None