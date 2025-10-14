"""intacct target sink class, which handles writing streams."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Mapping, Optional, Union
from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase
from singer_sdk.sinks import RecordSink

from target_intacct.mapping import UnifiedMapping

from .client import SageIntacctSDK, get_client
from .const import DEFAULT_API_URL, KEY_PROPERTIES, REQUIRED_CONFIG_KEYS
import re
# import xmltodict


class intacctSink(HotglueSink):
    """intacct target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        self._state = dict(target._state)
        self._target = target
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
            use_locations=target.config.get("use_locations", False) and self.stream_name != "Suppliers",
            location_id=target.config.get("location_id")
        )

        self.vendors = None
        self.locations = None
        self.accounts = None
        self.items = None
        self.banks = None
        self.classes = None
        self.projects = None
        self.departments = None
        self.po_transaction_types = None
        self.customers = None
        self.journal_entries = None


    @property
    def name(self):
        return self.stream_name

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """Preprocess the record."""
        return record

    def get_vendors(self):
        # Lookup for vendors
        if self.vendors is None:
            vendors = self.client.get_entity(
                object_type="accounts_payable_vendors", fields=["VENDORID", "NAME"]
            )
            self.vendors = self.dictify(vendors, "NAME", "VENDORID")
        return self.vendors

    def get_classes(self):
        # Lookup for vendors
        if self.classes is None:
            classes = self.client.get_entity(
                object_type="classes", fields=["CLASSID", "NAME"]
            )
            self.classes = self.dictify(classes, "NAME", "CLASSID")
        return self.classes

    def get_projects(self): 
        # Lookup for vendors
        if self.projects is None:
            projects = self.client.get_entity(
                object_type="projects", fields=["RECORDNO", "PROJECTID", "NAME"]
            )
            self.projects = self.dictify(projects, "NAME", "PROJECTID")
            self.projects_recordno = self.dictify(projects, "RECORDNO", "PROJECTID")
        return self.projects

    def get_locations(self):
        # Lookup for Locations
        if self.locations is None:
            locations = self.client.get_entity(
                object_type="locations", fields=["LOCATIONID", "NAME"]
            )
            self.locations = self.dictify(locations, "NAME", "LOCATIONID")
        return self.locations

    def get_accounts(self):
        if self.accounts is None:
            # Lookup for accounts
            accounts = self.client.get_entity(
                object_type="general_ledger_accounts",
                fields=["RECORDNO", "ACCOUNTNO", "TITLE"],
            )
            self.accounts = self.dictify(accounts, "TITLE", "ACCOUNTNO")
            self.accounts_recordno = self.dictify(accounts, "RECORDNO", "ACCOUNTNO")
        return self.accounts

    def get_departments(self):
        if self.departments is None:
            # Lookup for accounts
            departments = self.client.get_entity(
                object_type="departments",
                fields=["DEPARTMENTID", "TITLE"],
            )
            self.departments = self.dictify(departments, "TITLE", "DEPARTMENTID")
        return self.departments

    def get_po_transaction_types(self):
        if self.po_transaction_types is None:
            # Lookup for accounts
            self.po_transaction_types = self.client.get_entity(
                object_type="po_transaction_types",
                fields=["DOCID", "DOCCLASS"],
            )
        return self.po_transaction_types

    def get_po_transaction_type(self):
        override_po_transaction_type = self._target.config.get("po_transaction_type", None)
        if override_po_transaction_type:
            return override_po_transaction_type
        
        po_transaction_types = self.get_po_transaction_types()
        po_transaction_type = next(
            (po_transaction_type.get("DOCID") for po_transaction_type in po_transaction_types
                if po_transaction_type.get("DOCCLASS") == "Order"
                and po_transaction_type["DOCID"].lower().replace(" ", "") == "purchaseorder"),
                None)
        
        if not po_transaction_type:
            raise Exception("No purchase order transaction type found. Please configure the po_transaction_type in the config.json file.")
        
        return po_transaction_type

    def get_items(self):
        if self.items is None:
            # Lookup for items
            items = self.client.get_entity(
                object_type="item", fields=["RECORDNO", "ITEMID", "NAME"]
            )
            self.items = self.dictify(items, "NAME", "ITEMID")
            self.items_recordno = self.dictify(items, "RECORDNO", "ITEMID")
        return self.items

    def get_customers(self):
        # Lookup for customers
        if self.customers is None:
            customers = self.client.get_entity(
                object_type="customers", fields=["CUSTOMERID", "NAME"]
            )
            self.customers = self.dictify(customers, "NAME", "CUSTOMERID")
        return self.customers

    def get_journal_entries(self):
        # Lookup for journal_entries
        if self.journal_entries is None:
            journal_entries = self.client.get_entity(
                object_type="general_ledger_journal_entries", fields=["BATCH_TITLE", "RECORDNO"]
            )
            self.journal_entries = self.dictify(journal_entries, "BATCH_TITLE", "RECORDNO")
        return self.journal_entries


    def dictify(sefl, array, key, value):
        array_ = {}
        for i in array:
            array_[i[key]] = i[value]
        return array_

    def post_attachments(self, payload, record):
        mapping = UnifiedMapping(config=self.config)
        #prepare attachment payload
        att_payload = mapping.prepare_attachment_payload(record)
        if att_payload:
            att_id = att_payload["create_supdoc"]["supdocid"]
            #1. create folder
            #check if the folder exists:
            check_folder = {"get":{"@object": "supdocfolder", "@key": att_id}}
            folder = self.client.format_and_send_request(check_folder)

            if folder.get("data", {}).get("supdocfolder"):
                self.logger.info(f"Folder with name {att_id} already exists")
            else:
                # if folder doesn't exist create folder
                folder_payload = {"create_supdocfolder": {"supdocfoldername": att_id, "object": "supdocfolder"}}
                self.client.format_and_send_request(folder_payload)
            #2. post attachments
            #check if supdoc exists
            check_supdoc = {"get":{"@object": "supdoc", "@key": att_id}}
            supdoc = self.client.format_and_send_request(check_supdoc) or dict()
            supdoc = supdoc.get("data", {}) or dict()

            #updating existing supdoc
            supdoc = supdoc.get("supdoc")
            if supdoc:
                self.logger.info(f"supdoc with id {att_id} already exists, updating existing supdoc")
                attachments = supdoc.get("attachments", {}).get("attachment")
                #getting a list of existing attachments to avoid duplicates
                existing_attachments = {"content":[], "names":[]}
                if isinstance(attachments, dict):
                    existing_attachments["names"] = [attachments.get("attachmentname")]
                    existing_attachments["content"] = (attachments.get("attachmentdata"))
                elif isinstance(attachments, list):
                    existing_attachments["content"] = [att.get("attachmentdata") for att in attachments]
                    existing_attachments["names"] = [att.get("attachmentname") for att in attachments]
                #update att_payload to
                att_payload = mapping.prepare_attachment_payload(record, "update", existing_attachments)
            #send attachments
            if att_payload:
                self.client.format_and_send_request(att_payload)
            return att_id
        return None
    
    def get_employee_id_by_recordno(self, recordno):
        employee = self.client.query_entity(
            object_type="employees",
                fields={
                    "RECORDNO",
                    "EMPLOYEEID",
                },
                filters={"equalto": {"field": "RECORDNO", "value": f"{recordno}"}},
        )
        if employee:
            return employee["EMPLOYEEID"]
        raise Exception(f"Employee with recordno {recordno} not found.")

    def purchase_invoices_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "purchase_invoices", self.target_name)

        # Check if the invoice exists
        bill = None
        if payload.get("RECORDID"):
            # validate RECORDID
            invalid_chars = r"[\"\'&<>#?]"  # characters not allowed for RECORDID [&, <, >, #, ?]
            is_id_valid = not bool(re.search(invalid_chars, payload.get("RECORDID")))

            if not is_id_valid:
                raise Exception(
                    f"RECORDID '{payload.get('RECORDID')}' contains one or more invalid characters '&,<,>,#,?'. Please provide a RECORDID that does not include these characters."
                )
            # check if record exists
            bill = self.client.get_entity(object_type="accounts_payable_bills", fields=["RECORDNO", "STATE", "VENDORNAME", "BASECURR"], filter={"filter": {"equalto":{"field":"RECORDID","value": payload.get("RECORDID")}}})

        #send attachments
        supdoc_id = self.post_attachments(payload, record)
        if supdoc_id:
            payload["SUPDOCID"] = supdoc_id
        
        # Map action
        action = payload.get("STATE")
        if action:
            if action.lower() == "draft":
                payload["ACTION"] = "Draft"
        payload.pop("STATE", None)

        # Get the matching values for the payload :
        # Matching "VENDORNAME" -> "VENDORID"
        self.get_vendors()
        if payload.get("VENDORNAME") and not payload.get("VENDORID"):
            vendor_name = self.vendors.get(payload["VENDORNAME"])
            if vendor_name:
                payload["VENDORID"] = self.vendors[payload["VENDORNAME"]]
            else:
                raise Exception(
                    f"ERROR: VENDORNAME {payload['VENDORNAME']} not found for this account."
                )
        payload.pop("VENDORNAME", None)
        
        if payload.get("VENDORID") not in self.vendors.values():
            raise Exception(
                f"ERROR: VENDORID {payload['VENDORID']} not found for this account."
            )

        # Matching ""
        for item in payload.get("APBILLITEMS").get("APBILLITEM"):
            if item.get("EMPLOYEENO"):
                item["EMPLOYEEID"] = self.get_employee_id_by_recordno(item["EMPLOYEENO"])
                item.pop("EMPLOYEENO", None)

            if item.get("LOCATIONNAME"):
                self.get_locations()
                location = self.locations.get(item["LOCATIONNAME"])
                if location:
                    item["LOCATIONID"] = self.locations.get(item["LOCATIONNAME"])
                else:
                    raise Exception(f"Location '{payload['LOCATIONNAME']}' does not exist. Did you mean any of these: {list(self.locations.keys())}?")
            elif payload.get("LOCATIONNAME"):
                self.get_locations()
                location = self.locations.get(payload["LOCATIONNAME"])
                if location:
                    item["LOCATIONID"] = self.locations.get(payload["LOCATIONNAME"])
                else:
                    raise Exception(f"Location '{payload['LOCATIONNAME']}' does not exist. Did you mean any of these: {list(self.locations.keys())}?")

            if item.get("VENDORNAME") and not item.get("VENDORID"):
                self.get_vendors()
                item["VENDORID"] = self.vendors.get(item["VENDORNAME"])
            item.pop("VENDORNAME", None)

            if item.get("CLASSNAME"):
                self.get_classes()
                item["CLASSID"] = self.classes[item["CLASSNAME"]]
                item.pop("CLASSNAME")

            if item.get("PROJECTNAME"):
                self.get_projects()
                project_id = self.projects.get(item["PROJECTNAME"])
                if project_id:
                    item["PROJECTID"] = project_id
                item.pop("PROJECTNAME")

            #add custom fields to the item payload
            custom_fields = item.pop("customFields", None)
            if custom_fields:
                [item.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]

            self.get_accounts()
            if item.get("ACCOUNTID"):
                item["ACCOUNTNO"] = next(( self.accounts_recordno.get(x) for x in self.accounts_recordno if x == item['ACCOUNTID']), None)
                item.pop("ACCOUNTID", None)
            if item.get("ACCOUNTNAME") and not item.get("ACCOUNTNO"):
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
            elif not item.get("ACCOUNTNO"):
                raise Exception(    
                    f"ERROR: Account not provided or not valid for this tenant in item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                )                

            self.get_items()
            if payload.get("ITEMNAME") and self.items.get(payload.get("ITEMNAME")):
                item["ITEMID"] = self.items.get(payload.get("ITEMNAME"))
                item.pop("ITEMNAME")

            self.get_departments()
            if item.get("DEPARTMENT"):
                item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENT")]
                item.pop("DEPARTMENT")
            elif item.get("DEPARTMENTNAME"):
                item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENTNAME")]
                item.pop("DEPARTMENTNAME")

        payload.pop("LOCATIONNAME", None)

        if isinstance(payload["WHENCREATED"], datetime):
            payload["WHENCREATED"] = payload["WHENCREATED"].strftime("%Y-%m-%d")
        else:
            payload["WHENCREATED"] = payload["WHENCREATED"].split("T")[0]

        if bill:
            payload.update(bill)
            data = {"update": {"object": "accounts_payable_bills", "APBILL": payload}}
        else:
            data = {"create": {"object": "accounts_payable_bills", "APBILL": payload}}

        try:
            response = self.client.format_and_send_request(data)
            record_number = response.get("data", {}).get("apbill", {}).get("RECORDNO")
            return record_number, True, {}
        except Exception as e:
            # if invoice is new and attachments were posted, delete attachments
            if supdoc_id and list(data.keys())[0] == "create": 
                del_supdoc = {"delete_supdoc": {"@key": supdoc_id, "object": "supdoc"}}
                self.client.format_and_send_request(del_supdoc)
                self.logger.info(f"Supdoc '{supdoc_id}' deleted due invoice failed while being created.")
            raise Exception(e)

    def bills_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "bills", self.target_name)

        bill = None
        if payload.get("RECORDID"):
            # validate RECORDID
            invalid_chars = r"[\"\'&<>#?]"  # characters not allowed for RECORDID [&, <, >, #, ?]
            is_id_valid = not bool(re.search(invalid_chars, payload.get("RECORDID")))

            if not is_id_valid:
                raise Exception(
                    f"RECORDID '{payload.get('RECORDID')}' contains one or more invalid characters '&,<,>,#,?'. Please provide a RECORDID that does not include these characters."
                )
            # check if record exists
            bill = self.client.get_entity(object_type="accounts_payable_bills", fields=["RECORDNO"], filter={"filter": {"equalto":{"field":"RECORDID","value": payload.get("RECORDID")}}})

        #send attachments
        supdoc_id = self.post_attachments(payload, record)
        if supdoc_id:
            payload["SUPDOCID"] = supdoc_id
        
        # Map action
        action = payload.get("STATE")
        if action:
            if action.lower() == "draft":
                payload["ACTION"] = "Draft"
        payload.pop("STATE", None)

        #include locationid at header level
        if payload.get("LOCATIONNAME"):
            self.get_locations()
            if self.locations.get(payload["LOCATIONNAME"]):
                payload["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]
                payload.pop("LOCATIONNAME")
            else:
                raise Exception(
                    f"ERROR: Location '{payload['LOCATIONNAME']}' does not exist. Did you mean any of these: {list(self.locations.keys())}?"
                )

        #look for vendorName, vendorNumber and vendorId
        if not payload.get("VENDORID"):
            self.get_vendors()
            if payload.get("VENDORNAME"):
                vendor_name = self.vendors.get(payload["VENDORNAME"])
                if vendor_name:
                    vendor_dict = {"VENDORID": self.vendors[payload["VENDORNAME"]]}
                    payload = {**vendor_dict, **payload}
                else:
                    raise Exception(
                        f"ERROR: Vendor {payload['VENDORNAME']} does not exist. Did you mean any of these: {list(self.vendors.keys())}?"
                    )

            elif payload.get("VENDORNUMBER"):
                vendor_id = payload.pop("VENDORNUMBER")
                if vendor_id in self.vendors.values():
                    payload["VENDORID"] = vendor_id
                else:
                    raise Exception(f"ERROR: VENDORID {payload['VENDORNUMBER']} not found for this account.")

        for item in payload.get("APBILLITEMS").get("APBILLITEM"):
            if item.get("EMPLOYEENO"):
                item["EMPLOYEEID"] = self.get_employee_id_by_recordno(item["EMPLOYEENO"])
                item.pop("EMPLOYEENO", None)

            if payload.get("VENDORNAME"):
                self.get_vendors()
                item["VENDORID"] = self.vendors[payload["VENDORNAME"]]

            #include locationid at line level
            if payload.get("LOCATIONNAME"):
                self.get_locations()
                item["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]

            if item.get("CLASSNAME"):
                self.get_classes()
                if self.classes.get(item["CLASSNAME"]):
                    item["CLASSID"] = self.classes[item["CLASSNAME"]]
                    item.pop("CLASSNAME")
                else:
                    self.logger.info(
                        f"Skipping class due Class {payload['CLASSNAME']} does not exist. Did you mean any of these: {list(self.classes.keys())}?"
                    )


            if item.get("PROJECTNAME") and not item.get("PROJECTID"):
                self.get_projects()
                if self.projects.get(item["PROJECTNAME"]):
                    item["PROJECTID"] = self.projects[item["PROJECTNAME"]]
                else:
                    self.logger.info(
                        f"Skipping project due Project {payload['PROJECTNAME']} does not exist. Did you mean any of these: {list(self.projects.keys())}?"
                    )
            item.pop("PROJECTNAME", None)

            if payload.get("ITEMNAME") and not item.get("ITEMID"):
                self.get_items()
                item["ITEMID"] = self.items.get(payload.get("ITEMNAME"))
            item.pop("ITEMNAME", None)

            #use account instead of accountno
            self.get_accounts()
            if item.get("ACCOUNTID"):
                item["ACCOUNTNO"] = next(( self.accounts_recordno.get(x) for x in self.accounts_recordno if x == item['ACCOUNTID']), None)
                item.pop("ACCOUNTID", None)
            if item.get("ACCOUNTNAME") and not item.get("ACCOUNTNO"):
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
            if not item.get("ACCOUNTNO"):
                raise Exception(
                    f"ERROR: Account not provided or not valid for this tenant in item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                )

            #departmentid is optional
            self.get_departments()
            if item.get("DEPARTMENT") and not item.get("DEPARTMENTID"):
                item["DEPARTMENTID"] = self.departments.get(item.get("DEPARTMENT"))
            item.pop("DEPARTMENT", None)
            if item.get("DEPARTMENTNAME") and not item.get("DEPARTMENTID"):
                item["DEPARTMENTID"] = self.departments.get(item.get("DEPARTMENTNAME"))
            item.pop("DEPARTMENTNAME", None)

            
            #add custom fields to the item payload
            custom_fields = item.pop("customFields", None)
            if custom_fields:
                [item.update({cf.get("name"): cf.get("value")}) for cf in custom_fields]

        if payload.get("WHENCREATED"):
            if isinstance(payload["WHENCREATED"], datetime):
                payload["WHENCREATED"] = payload["WHENCREATED"].strftime("%Y-%m-%d")
            else:
                payload["WHENCREATED"] = payload["WHENCREATED"].split("T")[0]
        else:
            payload["WHENCREATED"] = datetime.now().strftime("%Y-%m-%d")

        if bill:
            payload.update(bill)
            data = {"update": {"object": "accounts_payable_bills", "APBILL": payload}}
        else:
            data = {"create": {"object": "accounts_payable_bills", "APBILL": payload}}

        try:
            response = self.client.format_and_send_request(data)
            record_number = response.get("data", {}).get("apbill", {}).get("RECORDNO")
            return record_number, response.get("status") == "success", {}
        except Exception as e:
            # if invoice is new and attachments were posted, delete attachments
            if supdoc_id and list(data.keys())[0] == "create": 
                del_supdoc = {"delete_supdoc": {"@key": supdoc_id, "object": "supdoc"}}
                self.client.format_and_send_request(del_supdoc)
                self.logger.info(f"Supdoc '{supdoc_id}' deleted due bill failed while being created.")
            raise Exception(e)

    def journal_entries_upload(self, record):

        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "journal_entries", self.target_name)

        if payload.get("JOURNAL"):
            payload["BATCH_TITLE"] = payload.get("JOURNAL")

        if "APBILLITEMS" in payload.keys():
            payload.pop("APBILLITEMS")

        for item in payload.get("ENTRIES").get("GLENTRY"):
            self.get_accounts()
            if item.get("ACCOUNTID"):
                item["ACCOUNTNO"] = next(( self.accounts_recordno.get(x) for x in self.accounts_recordno if x == item['ACCOUNTID']), None)
                item.pop("ACCOUNTID", None)
            if item.get("ACCOUNTNAME") and item.get("ACCOUNTNO") not in self.accounts.values():
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
                item.pop("ACCOUNTNAME")
            if not item.get("ACCOUNTNO"):
                raise Exception(
                    f"ERROR: Account not provided or not valid for this tenant on item {item}. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                )
            if item.get("TR_TYPE"):
                value = 1 if item.get("TR_TYPE").lower() == "debit" else -1
                item["TR_TYPE"] = value

            self.get_departments()
            if not item.get("DEPARTMENTID"):
                if item.get("DEPARTMENT"):
                    item["DEPARTMENT"] = self.departments[item.get("DEPARTMENT")]
                    item.pop("DEPARTMENT")
                if item.get("DEPARTMENTNAME"):
                    item["DEPARTMENT"] = self.departments[item.get("DEPARTMENTNAME")]
                    item.pop("DEPARTMENTNAME")
            elif item.get("DEPARTMENTID"):
                item["DEPARTMENT"] = item.get("DEPARTMENTID")

            self.get_locations()
            if not item.get("LOCATION"):
                if item.get("LOCATIONNAME"):
                    item["LOCATION"] = self.locations[item.get("LOCATIONNAME")]
                    item.pop("LOCATIONNAME")

            if item.get("CLASSNAME"):
                self.get_classes()
                item["CLASSID"] = self.classes.get(item["CLASSNAME"])
                item.pop("CLASSNAME")

            if item.get("CUSTOMERNAME"):
                self.get_customers()
                item["CUSTOMERID"] = self.customers.get(item["CUSTOMERNAME"])
                item.pop("CUSTOMERNAME")

            if item.get("VENDORNAME"):
                self.get_vendors()
                item["VENDORID"] = self.vendors.get(item["VENDORNAME"])
                item.pop("VENDORNAME")

        payload["BATCH_DATE"] = payload["BATCH_DATE"].split("T")[0]

        data = {"create": {"object": "GLBATCH", "GLBATCH": payload}}

        self.client.format_and_send_request(data)
        return None, True, {}

    def suppliers_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(
            record, "account_payable_vendors", self.target_name
        )
        # VENDORID is required if company does not use document sequencing
        vendor_id = payload.get("VENDORID")

        if vendor_id:
            valid_vendor_id = bool(re.match("^[A-Za-z0-9- ]*$", vendor_id))

            if valid_vendor_id:
                payload["VENDORID"] = vendor_id[
                    :20
                ]  # Intact size limit on VENDORID (20 characters)
                data = {"create": {"object": "account_payable_vendors", "VENDOR": payload}}

                self.get_vendors()
                if (not payload["VENDORID"] in self.vendors.items()) and (
                    not payload["NAME"] in self.vendors.keys()
                ):
                    self.client.format_and_send_request(data)
                    return vendor_id, True, {}
            else:
                raise Exception(f"Skipping vendor with {vendor_id} due to unsupported chars. Only letters, numbers and dashes accepted")
        else:
            raise Exception("Skipping vendor {payload} because vendorid is empty")
            

    def apadjustment_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(
            record, "apadjustment", self.target_name
        )

        if payload.get("action"):
            action = payload["action"]
            payload["action"] = "Draft" if action.lower() == "draft" else "Submit"

        if payload.get("vendorname"):
            self.get_vendors()
            payload["vendorid"] = self.vendors[payload["vendorname"]]
        
        if payload.get("currency"):
            payload["basecurr"] = payload["currency"]

        for item in payload.get("apadjustmentitems").get("lineitem", []):
            if item.get("accountid") and not item.get("glaccountno") and not item.get("accountlabel"):
                self.get_accounts()
                item["glaccountno"] = self.accounts_recordno.get(item["accountid"])
                item.pop("accountid")
            elif item.get("accountlabel") and not item.get("glaccountno"):
                self.get_accounts()
                item["glaccountno"] = self.accounts.get(item["accountlabel"])
                item.pop("accountlabel")
            else:
                try:
                    item.pop("accountlabel")
                except:
                    pass
            
            if item.get("vendorname"):
                if not item.get("vendorid"):
                    self.get_vendors()
                    try:
                        item["vendorid"] = self.vendors[item["vendorname"]]
                    except: 
                        raise Exception(
                        f"ERROR: vendorname {item['vendorname']} not found for this account."
                    )
                item.pop("vendorname")

            if item.get("projectname"):
                if not item.get("projectid"):
                    self.get_projects()
                    try:
                        item["projectid"] = self.projects[item["projectname"]]
                    except: 
                        raise Exception(
                        f"ERROR: projectname {item['projectname']} not found for this account."
                    )
                item.pop("projectname")
            
            if item.get("locationname"):
                if not item.get("locationid"):
                    self.get_locations()
                    try:
                        item["locationid"] = self.locations[item["locationname"]]
                    except: 
                        raise Exception(
                        f"ERROR: locationname {item['locationname']} not found for this account."
                    )
                item.pop("locationname")
            
            if item.get("classname"):
                if not item.get("classid"):
                    self.get_classes()
                    try:
                        item["classid"] = self.classes[item["classname"]]
                    except: 
                        raise Exception(
                        f"ERROR: classname {item['classname']} not found for this account."
                    )
                item.pop("classname")
            
            if item.get("departmentname"):
                if not item.get("departmentid"):
                    self.get_departments()
                    try:
                        item["departmentid"] = self.departments[item["departmentname"]]
                    except: 
                        raise Exception(
                        f"ERROR: departmentname {item['departmentname']} not found for this account."
                    )
                item.pop("departmentname")
            
        # order line fields
        lines = payload.get("apadjustmentitems").get("lineitem", [])
        first_keys = ["glaccountno", "accountlabel", "amount","memo", "locationid", "departmentid", "projectid", "taskid", "vendorid", "classid"]
        payload["apadjustmentitems"]["lineitem"] = [UnifiedMapping().order_dicts(line, first_keys) for line in lines]

        if payload.get("datecreated"):
            payload["datecreated"] = {
                "year": payload["datecreated"].split("-")[0],
                "month": payload["datecreated"].split("-")[1],
                "day": payload["datecreated"].split("-")[2],
            }

        ordered_payload = {}
        for key in ["vendorid", "datecreated", "adjustmentno", "action", "billno", "description", "basecurr", "currency", "exchratetype", "apadjustmentitems"]:
            if key in payload.keys():
                ordered_payload[key] = payload[key]
            elif key == "exchratetype" and key not in payload.keys():
                ordered_payload[key] = "Intacct Daily Rate"

        data = {"create_apadjustment": {"object": "apadjustment", "APADJUSTMENT": ordered_payload}}
        response = self.client.format_and_send_request(data, use_payload=True)
        key = response.get("key")
        return key, response.get("status") == "success", {}


    def purchase_orders_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "purchase_orders", self.target_name)

        if not payload.get("vendorid"):
            raise Exception("vendorid is required")

        payload["transactiontype"] = self.get_po_transaction_type()


        # Check if the invoice exists
        order = None
        if payload.get("RECORDNO"):
            # check if record exists
            recordno = payload.pop("RECORDNO")
            order = self.client.get_entity(object_type="purchase_orders", fields=["RECORDNO"], filter={"filter": {"equalto":{"field":"RECORDNO","value": recordno}}, "select": {"field": ["RECORDNO", "DOCNO"]}}, docparid="Purchase Order")
            if order:
                order_lines = self.client.get_entity(object_type="purchase_orders_entry", fields=["RECORDNO"], filter={"filter": {"equalto":{"field":"DOCHDRNO","value": recordno}}, "pagesize": "2000"}, docparid="Purchase Order")

        # Get the matching values for the payload :
        self.get_vendors()
        vendor_name = payload.pop("vendorname", None)
        if vendor_name and not payload.get("vendorid"):
            try:
                payload["vendorid"] = self.vendors[vendor_name]
            except:
                return None, False, {
                    "error": f"ERROR: Vendor {vendor_name} does not exist. Did you mean any of these: {list(self.vendors.keys())}?"
                }

        if payload.get("datecreated"):
            payload["datecreated"] = {
                "year": payload.get("datecreated").split("-")[0],
                "month": payload.get("datecreated").split("-")[1],
                "day": payload.get("datecreated").split("-")[2],
            }

        if payload.get("datedue"):
            payload["datedue"] = {
                "year": payload.get("datedue").split("-")[0],
                "month": payload.get("datedue").split("-")[1],
                "day": payload.get("datedue").split("-")[2],
            }

        if payload.get("basecurr"):
            payload["currency"] = payload["basecurr"]
        payload["returnto"] = {"contactname": None}
        payload["payto"] = {"contactname": None}
        payload["exchratetype"] = "Intacct Daily Rate"

        key_order = ["transactiontype", "datecreated", "vendorid", "documentno", "referenceno", "termname", "datedue", "message", "returnto", "payto", "basecurr", "currency", "exchratetype", "potransitems"]
        payload = UnifiedMapping().order_dicts(payload, key_order)

        items = payload.get("potransitems").get("potransitem", [])
        for item in items:
            item["unit"] = "Each"

            self.get_locations()
            location_name = item.pop("locationname", None)
            if location_name and not item.get("locationid"):
                try:
                    item["locationid"] = self.locations[location_name]
                except:
                    return {
                        "error": f"ERROR: Location {location_name} does not exist. Did you mean any of these: {list(self.locations.keys())}?"
                    }
            
            self.get_departments()
            department_name = item.pop("departmentname", None)
            if department_name and not item.get("departmentid"):
                try:
                    item["departmentid"] = self.departments[department_name]
                except:
                    return None, False, {
                        "error": f"ERROR: Department {department_name} does not exist. Did you mean any of these: {list(self.departments.keys())}?"
                    }
                
            self.get_projects()
            project_name = item.pop("projectname", None)
            if project_name and not item.get("projectid"):
                try:
                    item["projectid"] = self.projects[project_name]
                except:
                    return None, False, {
                        "error": f"ERROR: Project {project_name} does not exist. Did you mean any of these: {list(self.projects.keys())}?"
                    }

            if item.get("projectid") and item.get("projectid") in self.projects_recordno.keys():
                item["projectid"] = self.projects_recordno.get(item.get("projectid"))
            elif item.get("projectid") and item.get("projectid") in self.projects.values():
                item["projectid"] = item.get("projectid")
            elif item.get("projectid"):
                return None, False, {
                    "error": f"ERROR: Project {item.get('projectid')} does not exist. Did you mean any of these: {list(self.projects.values())}?"
                }
                
            self.get_classes()
            class_name = item.pop("classname", None)
            if class_name and not item.get("classid"):
                try:
                    item["classid"] = self.classes[class_name]
                except:
                    return None, False, {
                        "error": f"ERROR: Class {class_name} does not exist. Did you mean any of these: {list(self.classes.keys())}?"
                    }

            self.get_items()
            if item.get("itemid") and self.items_recordno.get(item.get("itemid")):
                item["itemid"] = self.items_recordno.get(item.get("itemid"))
            elif item.get("itemname") and self.items.get(item.get("itemname")):
                item["itemid"] = self.items.get(item.get("itemname"))
            elif item.get("itemid") and item.get("itemid") in self.items.values():
                item["itemid"] = item.get("itemid")
            elif item.get("itemid"):
                return None, False, {
                    "error": f"ERROR: Product {item.get('itemid')} or {item.get('itemname')} does not exist. Did you mean any of these: {list(self.items.values())}?"
                }




        key_order = ["itemid", "quantity", "unit", "price", "tax", "locationid", "departmentid", "memo", "projectid", "employeeid", "classid"]
        payload["potransitems"]["potransitem"] = [UnifiedMapping().order_dicts(item, key_order) for item in items]

        if order:
            # when updating the record we need to remove some fields from the payload
            fields_remove = ["vendorid", "transactiontype", "documentno"]
            for field in fields_remove:
                payload.pop(field, None)

            # add the PK for update
            payload["@key"] = f"Purchase Order-{order['DOCNO']}"
            
            payload["updatepotransitems"] = {**payload.pop("potransitems", None)}
            if order_lines:
                # delete existing lines
                payload["updatepotransitems"]["updatepotransitem"] = [{"@line_num": n, "itemid": None} for n in range(1, len(order_lines)+1)]            

            data = {"update_potransaction": {"object": "PODOCUMENT", "PODOCUMENT": payload}}
        else:
            data = {"create_potransaction": {"object": "PODOCUMENT", "PODOCUMENT": payload}}

        result = self.client.format_and_send_request(data, use_payload=True)

        if order: 
            # Upsert
            return order.get("RECORDNO"), result.get("status") == "success", {}
        else:
            # Doc number is returned as Purchase Order-DOCNO
            # Need to interchange DOCNO with RECORDNO
            docno = result.get("key", "").split("-")[1]
            order = self.client.get_entity(object_type="purchase_orders", fields=["DOCNO"], filter={"filter": {"equalto":{"field":"DOCNO","value": docno}}, "select": {"field": ["RECORDNO", "DOCNO"]}}, docparid="Purchase Order")
            return order.get("RECORDNO"), result.get("status") == "success", {}


    def get_banks(self):
        # Lookup for banks
        if self.banks is None:
            banks = self.client.get_entity(
                object_type="payment_provider_bank_accounts",
                fields=["BANKACCOUNTID", "PROVIDERID"],
            )
            self.banks = banks
        return self.banks


    def upsert_record(self, record: dict, context: dict) -> None:

        if self.stream_name == "Suppliers":
            id, success, state = self.suppliers_upload(record)
        if self.stream_name == "PurchaseInvoices":
            id, success, state = self.purchase_invoices_upload(record)
        if self.stream_name == "Bills":
            id, success, state = self.bills_upload(record)
        if self.stream_name == "JournalEntries":
            id, success, state = self.journal_entries_upload(record)
        if self.stream_name == "APAdjustment":
            id, success, state = self.apadjustment_upload(record)
        if self.stream_name == "PurchaseOrders":
            id, success, state = self.purchase_orders_upload(record)

        return id, success, state


class BillPaymentsSink(intacctSink):
    name = "BillPayments"



    def query_bill(self, filters: list[dict]):
        if not filters:
            raise Exception("No filters provided")

        filter_clause = {"and": {
            "equalto": filters
        }} if len(filters) > 1 else {"equalto": filters[0]}

        # Lookup for Bills
        return self.client.query_entity(
            object_type="accounts_payable_bills",
            fields={
                "RECORDNO",
                "VENDORNAME",
                "VENDORID",
                "RECORDID",
                "DOCNUMBER",
                "CURRENCY",
                "TRX_TOTALDUE",
            },
            filters=filter_clause,
        )

    def preprocess_record(self, record: dict, context: dict) -> dict:
        """
        Transforms the record from Unified V2 BillPayment format into Intacct payload

        Supported fields:
        - billId
        - billNumber
        - paymentDate
        - amount
        - currency
        - accountNumber
        - vendorId
        - vendorName


        NOT UNIFIED but supported: paymentMethod
        """

        bill_filters = []
        if record.get("billNumber"):
            bill_filters.append({"field": "RECORDID", "value": f"{record['billNumber']}"})
        if record.get("billId"):
            bill_filters.append({"field": "RECORDNO", "value": f"{record['billId']}"})
        if record.get("vendorId"):
            bill_filters.append({"field": "VENDORID", "value": f"{record['vendorId']}"})
        if record.get("vendorName"):
            bill_filters.append({"field": "VENDORNAME", "value": f"{record['vendorName']}"})

        if not bill_filters:
            return {"error": "No bill identifiers provided on payment record."}

        # Get the bill with the id
        bills = self.query_bill(bill_filters)

        if not bills:
            return {"error": f"No bill for record {record} found."}

        if len(bills) > 1:
            return {"error": f"Multiple bills matched for record {record} found. Add additional fields to the payload to filter the bill."}

        bill = bills[0]

        # If no payment date is set, we fall back to today
        payment_date = record.get("paymentDate")

        if payment_date is None:
            payment_date = datetime.today().strftime("%m/%d/%Y")
        elif isinstance(payment_date, str):
            # Parse it from ISO
            payment_date = datetime.strptime(payment_date.split("T")[0], "%Y-%m-%d").strftime("%m/%d/%Y")
        elif isinstance(payment_date, datetime):
            payment_date = payment_date.strftime("%m/%d/%Y")

        if not record.get("accountNumber"):
            return {"error": "accountNumber is a required field"}

        bank_name = record["accountNumber"]
        # TODO: not sure why we need this
        if "--" in bank_name:
            bank_name = bank_name.split("--")[0]

        if not record.get("paymentMethod"):
            return {"error": "paymentMethod is a required field"}


        payload = {
            "FINANCIALENTITY": bank_name,
            "PAYMENTMETHOD": record["paymentMethod"],
            "VENDORID": record.get("vendorId") or bill["VENDORID"],
            "CURRENCY": record.get("currency") or bill["CURRENCY"],
            "PAYMENTDATE": payment_date,
            "APPYMTDETAILS": {
                "APPYMTDETAIL": {
                    "RECORDKEY": bill["RECORDNO"],
                    "TRX_PAYMENTAMOUNT": record.get("amount") or bill["TRX_TOTALDUE"],
                }
            },
        }

        return payload


    def upsert_record(self, record, context):
        """Process the record."""

        state = {}
        if record.get("error"):
            state["error"] = record["error"]
            return None, False, state

        data = {"create": {"object": "accounts_payable_payments", "APPYMT": record}}
        response = self.client.format_and_send_request(data)
        record_number = response.get("data", {}).get("appymt", {}).get("RECORDNO")



        return record_number, True, state

