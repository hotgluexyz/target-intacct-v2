import json
import os

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

        return {"APBILLITEM": line_items_}

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
            if lookup_key == "address" and target == "intacct-v2":
                payload = self.map_address(
                    record.get(lookup_key, []), mapping[lookup_key], payload
                )
            elif lookup_key == "lineItems" and target == "intacct-v2":
                payload["APBILLITEMS"] = self.map_lineItems(
                    record.get(lookup_key, []), mapping[lookup_key]
                )
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
