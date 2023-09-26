"""intacct target sink class, which handles writing streams."""

from __future__ import annotations

from datetime import datetime
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

        self.vendors = None
        self.locations = None
        self.accounts = None
        self.items = None
        self.banks = None
        self.classes = None
        self.projects = None
        self.departments = None
        self.customers = None
        self.journal_entries = None

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
                object_type="projects", fields=["PROJECTID", "NAME"]
            )
            self.projects = self.dictify(projects, "NAME", "PROJECTID")
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

    def get_items(self):
        if self.items is None:
            # Lookup for items
            items = self.client.get_entity(
                object_type="item", fields=["ITEMID", "NAME"]
            )
            self.items = self.dictify(items, "NAME", "ITEMID")
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

    def purchase_invoices_upload(self, record):

        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "purchase_invoices", self.target_name)

        #prepare attachment payload
        att_payload = mapping.prepare_attachment_payload(record)
        if att_payload:
            #create folder
            folder_payload = {"create_supdocfolder": {"supdocfoldername": payload.get("RECORDID"), "object": "supdocfolder"}}
            self.client.format_and_send_request(folder_payload)
            # post attachments
            self.client.format_and_send_request(att_payload)
            payload["SUPDOCID"] = att_payload["create_supdoc"]["supdocid"]

        # Get the matching values for the payload :
        # Matching "VENDORNAME" -> "VENDORID"
        if payload.get("VENDORNAME"):
            self.get_vendors()
            payload["VENDORID"] = self.vendors[payload["VENDORNAME"]]

        # Matching ""
        for item in payload.get("APBILLITEMS").get("APBILLITEM"):
            if payload.get("LOCATIONNAME"):
                self.get_locations()
                item["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]
            
            if payload.get("LOCATIONNAME"):
                self.get_locations()
                item["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]

            if item.get("VENDORNAME"):
                self.get_vendors()
                item["VENDORID"] = self.vendors[payload["VENDORNAME"]]
                item.pop("VENDORNAME")
            
            if not item.get("VENDORNAME") and payload.get("VENDORNAME"):
                item["VENDORID"] = self.vendors[payload["VENDORNAME"]]

            if item.get("CLASSNAME"):
                self.get_classes()
                item["CLASSID"] = self.classes[item["CLASSNAME"]]
                item.pop("CLASSNAME")
            
            if item.get("PROJECTNAME"):
                self.get_projects()
                item["PROJECTID"] = self.projects[item["PROJECTNAME"]]
                item.pop("PROJECTNAME")

            # TODO For now the account number is set by hand.
            # item["ACCOUNTNO"] = "6220"
            self.get_accounts()
            if item.get("ACCOUNTNAME") and self.accounts.get(item["ACCOUNTNAME"]):
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
            elif not item.get("ACCOUNTNO"):
                raise Exception(
                    f"ERROR: ACCOUNTNO or ACCOUNTNAME not found. \n Intaccts Requires an ACCOUNTNAME associated with each line item"
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

        payload["WHENCREATED"] = payload["WHENCREATED"].split("T")[0]

        data = {"create": {"object": "accounts_payable_bills", "APBILL": payload}}

        self.client.format_and_send_request(data)
    
    def bills_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(record, "bills", self.target_name)

        #prepare attachment payload
        att_payload = mapping.prepare_attachment_payload(record)
        if att_payload:
            #create folder
            folder_payload = {"create_supdocfolder": {"supdocfoldername": payload.get("RECORDID"), "object": "supdocfolder"}}
            self.client.format_and_send_request(folder_payload)
            # post attachments
            self.client.format_and_send_request(att_payload)
            payload["SUPDOCID"] = att_payload["create_supdoc"]["supdocid"]
        
        #include locationid at header level
        if payload.get("LOCATIONNAME"):
            self.get_locations()
            payload["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]
            payload.pop("LOCATIONNAME")

        #include vendorname and vendornumber
        if payload.get("VENDORNAME"):
            self.get_vendors()
            vendor_dict = {"VENDORID": self.vendors[payload["VENDORNAME"]]}
            payload = {**vendor_dict, **payload}

        if payload.get("VENDORNUMBER"):
            self.get_vendors()
            payload["VENDORNUMBER"] = self.vendors[payload["VENDORID"]]
        
        for item in payload.get("APBILLITEMS").get("APBILLITEM"):
            if payload.get("VENDORNAME"):
                self.get_vendors()
                item["VENDORID"] = self.vendors[payload["VENDORNAME"]]

            #include locationid at line level
            if payload.get("LOCATIONNAME"):
                self.get_locations()
                item["LOCATIONID"] = self.locations[payload["LOCATIONNAME"]]

            if item.get("CLASSNAME"):
                self.get_classes()
                item["CLASSID"] = self.classes[item["CLASSNAME"]]
                item.pop("CLASSNAME")
            
            if item.get("PROJECTNAME"):
                self.get_projects()
                item["PROJECTID"] = self.projects[item["PROJECTNAME"]]
                item.pop("PROJECTNAME")

            #use accountname instead of accountno
            self.get_accounts()
            if item.get("ACCOUNTNAME") and not item.get("ACCOUNTNO"):
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
            if item.get("ACCOUNTNO") and not item.get("ACCOUNTNAME"):
                acct_name = next(( x for x in self.accounts if self.accounts.get(x) == item['ACCOUNTNO']), None)
                item["ACCOUNTNAME"] = acct_name
            elif not item.get("ACCOUNTNO"):
                raise Exception(
                    f"ERROR: ACCOUNTNAME or ACCOUNTNO not found for this tenant. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                )

            #we add departmentid as intacct requires it
            self.get_departments()
            if item.get("DEPARTMENT"):
                item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENT")]
                item.pop("DEPARTMENT")
            elif item.get("DEPARTMENTNAME"):
                item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENTNAME")]
                item.pop("DEPARTMENTNAME")

        payload["WHENCREATED"] = payload["WHENCREATED"].split("T")[0]

        data = {"create": {"object": "accounts_payable_bills", "APBILL": payload}}

        self.client.format_and_send_request(data)


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
            if item.get("ACCOUNTNAME"):
                item["ACCOUNTNO"] = self.accounts.get(item["ACCOUNTNAME"])
                item.pop("ACCOUNTNAME")
            if not item.get("ACCOUNTNAME") and not item.get("ACCOUNTNO"):
                raise Exception(
                    f"ERROR: ACCOUNTNO not found. \n Intaccts Requires an ACCOUNTNO associated with each line item"
                )
            if item.get("TR_TYPE"):
                value = 1 if item.get("TR_TYPE").lower() == "debit" else -1
                item["TR_TYPE"] = value

            self.get_departments()
            if not item.get("DEPARTMENTID"):
                if item.get("DEPARTMENT"):
                    item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENT")]
                    item.pop("DEPARTMENT")
                if item.get("DEPARTMENTNAME"):
                    item["DEPARTMENTID"] = self.departments[item.get("DEPARTMENTNAME")]
                    item.pop("DEPARTMENTNAME")
            elif item.get("DEPARTMENTID"):
                item["DEPARTMENTID"] = item.get("DEPARTMENTID")

            self.get_locations()
            if not item.get("LOCATION"):
                if item.get("LOCATIONNAME"):
                    item["LOCATION"] = self.locations[item.get("LOCATIONNAME")]
                    item.pop("LOCATIONNAME")

            self.get_classes()
            if item.get("CLASSNAME"):
                item["CLASSID"] = self.classes.get(item["CLASSNAME"])
                item.pop("CLASSNAME")
            
            self.get_customers()
            if item.get("CUSTOMERNAME"):
                item["CUSTOMERID"] = self.customers.get(item["CUSTOMERNAME"])
                item.pop("CUSTOMERNAME")
            
            self.get_vendors()
            if item.get("VENDORNAME"):
                item["VENDORID"] = self.vendors.get(item["VENDORNAME"])
                item.pop("VENDORNAME")

        payload["BATCH_DATE"] = payload["BATCH_DATE"].split("T")[0]
        
        data = {"create": {"object": "GLBATCH", "GLBATCH": payload}}

        self.client.format_and_send_request(data)


    def suppliers_upload(self, record):
        # Format data
        mapping = UnifiedMapping()
        payload = mapping.prepare_payload(
            record, "account_payable_vendors", self.target_name
        )
        payload["VENDORID"] = payload["VENDORID"][
            :20
        ]  # Intact size limit on VENDORID (20 characters)
        data = {"create": {"object": "account_payable_vendors", "VENDOR": payload}}

        self.get_vendors()
        if (not payload["VENDORID"] in self.vendors.items()) and (
            not payload["NAME"] in self.vendors.keys()
        ):
            self.client.format_and_send_request(data)

    def get_banks(self):
        # Lookup for banks
        if self.banks is None:
            banks = self.client.get_entity(
                object_type="payment_provider_bank_accounts",
                fields=["BANKACCOUNTID", "PROVIDERID"],
            )
            self.banks = banks
        return self.banks

    def query_bill(self, bill_number):
        if self.items is None:
            # Lookup for Bills
            bills = self.client.query_entity(
                object_type="accounts_payable_bills",
                bill_number=bill_number,
                fields={
                    "RECORDNO",
                    "VENDORNAME",
                    "VENDORID",
                    "RECORDID",
                    "DOCNUMBER",
                    "CURRENCY",
                    "TRX_TOTALDUE",
                },
                filters={"equalto": {"field": "RECORDID", "value": f"{bill_number}"}},
            )
        return bills

    def pay_bill(self, record):
        bill = self.query_bill(record["billNumber"])
        if bill is not None:
            if "paymentDate" not in record:
                payment_date = datetime.today().strftime("%d/%m/%Y")
            elif record["paymentDate"] is None:
                payment_date = datetime.today().strftime("%d/%m/%Y")
            else:
                payment_date = record["paymentDate"]
            bank_name = record["bankName"]
            if "--" in bank_name:
                bank_name = bank_name.split("--")[0]
            payload = {
                "FINANCIALENTITY": bank_name,
                "PAYMENTMETHOD": record["paymentMethod"],
                "VENDORID": bill["VENDORID"],
                "CURRENCY": bill["CURRENCY"],
                "PAYMENTDATE": payment_date,
                "APPYMTDETAILS": {
                    "APPYMTDETAIL": {
                        "RECORDKEY": bill["RECORDNO"],
                        "TRX_PAYMENTAMOUNT": bill["TRX_TOTALDUE"],
                    }
                },
            }
            data = {
                "create": {"object": "accounts_payable_payments", "APPYMT": payload}
            }
            self.client.format_and_send_request(data)

    def process_record(self, record: dict, context: dict) -> None:

        if self.stream_name == "Suppliers":
            self.suppliers_upload(record)
        if self.stream_name == "PurchaseInvoices":
            self.purchase_invoices_upload(record)
        if self.stream_name == "Bills":
            self.bills_upload(record)
        if self.stream_name == "JournalEntries":
            self.journal_entries_upload(record)
        if self.stream_name == "PayBill":
            self.pay_bill(record)
