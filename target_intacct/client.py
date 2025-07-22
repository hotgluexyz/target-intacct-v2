"""
API Base class with util functions
"""
import datetime as dt
import json
import re
import uuid
from typing import Dict, List, Union
from urllib.parse import unquote

import requests
import singer
import xmltodict
import logging
import backoff
import copy

from target_intacct.exceptions import (
    ExpiredTokenError,
    InternalServerError,
    InvalidTokenError,
    NoPrivilegeError,
    NotFoundItemError,
    SageIntacctSDKError,
    WrongParamsError,
)

from .const import GET_BY_DATE_FIELD, INTACCT_OBJECTS


def _format_date_for_intacct(datetime: dt.datetime) -> str:
    """
    Intacct expects datetimes in a 'MM/DD/YY HH:MM:SS' string format.
    Args:
        datetime: The datetime to be converted.

    Returns:
        'MM/DD/YY HH:MM:SS' formatted string.
    """
    return datetime.strftime("%m/%d/%Y %H:%M:%S")


class SageIntacctSDK:
    """The base class for all API classes."""

    def __init__(
        self,
        api_url: str,
        company_id: str,
        sender_id: str,
        sender_password: str,
        user_id: str,
        user_password: str,
        headers: Dict,
        use_locations: bool,
        location_id: str
    ):
        self.__api_url = api_url
        self.__company_id = company_id
        self.__sender_id = sender_id
        self.__sender_password = sender_password
        self.__user_id = user_id
        self.__user_password = user_password
        self.__headers = headers
        self.__use_locations = use_locations
        self.__location_id = location_id

        """
        Initialize connection to Sage Intacct
        :param sender_id: Sage Intacct sender id
        :param sender_password: Sage Intacct sender password
        :param user_id: Sage Intacct user id
        :param company_id: Sage Intacct company id
        :param user_password: Sage Intacct user password
        """
        # Initializing variables
        self._set_session_id(
            user_id=self.__user_id,
            company_id=self.__company_id,
            user_password=self.__user_password,
            location_id=self.__location_id
        )

    def _set_session_id(self, user_id: str, company_id: str, user_password: str, location_id = None):
        """
        Sets the session id for APIs
        """
        login = {
                    "userid": user_id,
                    "companyid": company_id,
                    "password": user_password,
                }
        
        if self.__use_locations and location_id:
            login["locationid"] = location_id
        
        timestamp = dt.datetime.now()
        dict_body = {
            "request": {
                "control": {
                    "senderid": self.__sender_id,
                    "password": self.__sender_password,
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {
                        "login": login
                    },
                    "content": {
                        "function": {
                            "@controlid": str(uuid.uuid4()),
                            "getAPISession": None,
                        }
                    },
                },
            }
        }

        response = self._post_request(dict_body, self.__api_url)

        if response["authentication"]["status"] == "success":
            session_details = response["result"]["data"]["api"]
            self.__api_url = session_details["endpoint"]
            self.__session_id = session_details["sessionid"]

        else:
            raise SageIntacctSDKError("Error: {0}".format(response["errormessage"]))
        
    def clean_creds(self, key_field: str, request_body: dict):
        request_body = request_body.copy()
        if request_body.get(key_field, {}).get("control", {}):
            for key, _ in request_body[key_field]["control"].items():
                request_body[key_field]["control"][key] = "***"
        
        if request_body.get(key_field, {}).get("operation", {}).get("authentication", {}):
            for key, _ in request_body[key_field]["operation"]["authentication"].items():
                request_body[key_field]["operation"]["authentication"][key] = "***"
        
        return request_body

    @singer.utils.ratelimit(10, 1)
    # backoff this function, min time to wait is 5 second, max is 10 seconds
    @backoff.on_exception(backoff.expo, (requests.exceptions.RequestException), max_tries=5, base=3)
    def _post_request(self, dict_body: dict, api_url: str) -> Dict:
        """
        Create a HTTP post request.

        Parameters:
            dict_body (dict): HTTP POST body data for the wanted API.
            api_url (str): Url for the wanted API.

        Returns:
            A response from the request (dict).
        """

        api_headers = {"content-type": "application/xml"}
        api_headers.update(self.__headers)
        body = xmltodict.unparse(dict_body).encode('utf-8')
        response = requests.post(api_url, headers=api_headers, data=body)

        clean_body = self.clean_creds("request", dict_body)

        if not "attachmentdata" in str(body):
            logging.info(f"Raw response {clean_body} with status code {response.status_code}")

        try:
            parsed_xml = xmltodict.parse(response.text)
            parsed_response = json.loads(json.dumps(parsed_xml))
        except:
            raise Exception(f"Error: {response.text}, Status code: {response.status_code}")
        
        clean_parsed_response = copy.deepcopy(parsed_response)
        clean_parsed_response = self.clean_creds("response", clean_parsed_response)

        if "attachmentdata" in str(body):
            logging.info(f"response with status code {response.status_code} for request to {response.url}")
        else:
            logging.info(f"parsed response {clean_parsed_response} with status code {response.status_code} for request to {response.url}")

        #getting the errors
        res = parsed_response["response"]
        if res.get("errormessage"):
            error = res.get("errormessage")
        elif res.get("operation"):
            error = res.get("operation").get("result", {}).get("errormessage", {})

        if response.status_code == 200:
            if parsed_response["response"]["control"]["status"] == "success":
                api_response = parsed_response["response"]["operation"]

            if parsed_response["response"]["control"]["status"] == "failure":
                exception_msg = self.decode_support_id(
                    parsed_response["response"]["errormessage"]
                )
                raise WrongParamsError(
                    "Some of the parameters are wrong", exception_msg
                )

            if api_response["authentication"]["status"] == "failure":
                raise InvalidTokenError(
                    "Invalid token / Incorrect credentials",
                    api_response["errormessage"],
                )

            if api_response["result"]["status"] == "success":
                return api_response

        if response.status_code == 400:
            raise WrongParamsError("Some of the parameters are wrong", error)

        if response.status_code == 401:
            raise InvalidTokenError(
                "Invalid token / Incorrect credentials", error
            )

        if response.status_code == 403:
            raise NoPrivilegeError(
                "Forbidden, the user has insufficient privilege", error
            )

        if response.status_code == 404:
            raise NotFoundItemError("Not found item with ID", error)

        if response.status_code == 498:
            raise ExpiredTokenError("Expired token, try to refresh it", error)

        if response.status_code == 500:
            raise InternalServerError("Internal server error", error)

        logging.info("Error while sending request data: {0}".format(clean_body))
        raise SageIntacctSDKError("Error: {0}".format(error))

    def support_id_msg(self, errormessages) -> Union[List, Dict]:
        """
        Finds whether the error messages is list / dict and assign type and error assignment.

        Parameters:
            errormessages (dict / list): error message received from Sage Intacct.

        Returns:
            Error message assignment and type.
        """
        error = {}
        if isinstance(errormessages["error"], list):
            error["error"] = errormessages["error"][0]
            error["type"] = "list"
        elif isinstance(errormessages["error"], dict):
            error["error"] = errormessages["error"]
            error["type"] = "dict"

        return error

    def decode_support_id(self, errormessages: Union[List, Dict]) -> Union[List, Dict]:
        """
        Decodes Support ID.

        Parameters:
            errormessages (dict / list): error message received from Sage Intacct.

        Returns:
            Same error message with decoded Support ID.
        """
        support_id_msg = self.support_id_msg(errormessages)
        data_type = support_id_msg["type"]
        error = support_id_msg["error"]
        if error and error["description2"]:
            message = error["description2"]
            support_id = re.search("Support ID: (.*)]", message)
            if support_id and support_id.group(1):
                decoded_support_id = unquote(support_id.group(1))
                message = message.replace(support_id.group(1), decoded_support_id)

        if data_type == "list":
            errormessages["error"][0]["description2"] = message if message else None
        elif data_type == "dict":
            errormessages["error"]["description2"] = message if message else None

        return errormessages

    def format_and_send_request(self, data: Dict, use_payload=False) -> Union[List, Dict]:
        """
        Format data accordingly to convert them to xml.

        Parameters:
            data (dict): HTTP POST body data for the wanted API.

        Returns:
            A response from the _post_request (dict).
        """
        key = next(iter(data))
        try:
            object_type = data[key]["object"]
        except:
            object_type = data[key]["@object"]
        logging.info(f"Creating request with object_type: {object_type} and action {data[key]}")

        # Remove object entry if unnecessary
        if "create" in key or "update" in key or "delete" in key or key in ["create", "update", "delete"]:
            data[key].pop("object", None)

        timestamp = dt.datetime.now()

        payload_data = data[key]
        if use_payload:
            payload_data = data[key][object_type.upper()]

        dict_body = {
            "request": {
                "control": {
                    "senderid": self.__sender_id,
                    "password": self.__sender_password,
                    "controlid": timestamp,
                    "uniqueid": False,
                    "dtdversion": 3.0,
                    "includewhitespace": False,
                },
                "operation": {
                    "authentication": {"sessionid": self.__session_id},
                    "content": {
                        "function": {"@controlid": str(uuid.uuid4()), key: payload_data}
                    },
                },
            }
        }
        with singer.metrics.http_request_timer(endpoint=object_type):
            response = self._post_request(dict_body, self.__api_url)
        return response["result"]

    def get_entity(self, *, object_type: str, fields: List[str], filter={}, docparid=None) -> List[Dict]:
        """
        Get multiple objects of a single type from Sage Intacct.

        Returns:
            List of Dict in object_type schema.
        """
        intacct_object_type = INTACCT_OBJECTS[object_type]
        total_intacct_objects = []
        get_count = {
            "query": {
                "object": intacct_object_type,
                "select": {"field": "RECORDNO"},
                "pagesize": "1",
                "options": {"showprivate": "true"},
            }
        }

        if filter:
            get_count["query"].update(filter)

        if docparid:
            get_count["docparid"] = docparid

        response = self.format_and_send_request(get_count)

        if filter:
            response = response["data"].get(INTACCT_OBJECTS[object_type])
            return response

        count = int(response["data"]["@totalcount"])
        pagesize = 1000
        offset = 0
        for _i in range(0, count, pagesize):
            data = {
                "query": {
                    "object": intacct_object_type,
                    "select": {"field": fields},
                    "options": {"showprivate": "true"},
                    "pagesize": pagesize,
                    "offset": offset,
                }
            }
            intacct_objects = self.format_and_send_request(data)["data"][
                intacct_object_type
            ]
            # When only 1 object is found, Intacct returns a dict, otherwise it returns a list of dicts.
            if isinstance(intacct_objects, dict):
                intacct_objects = [intacct_objects]

            total_intacct_objects = total_intacct_objects + intacct_objects

            offset = offset + pagesize
        return total_intacct_objects

    def get_sample(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {
            "readByQuery": {
                "object": intacct_object.upper(),
                "fields": "*",
                "query": None,
                "pagesize": "10",
            }
        }

        return self.format_and_send_request(data)["data"][intacct_object.lower()]

    def get_definition(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {"lookup": {"object": intacct_object.upper()}}

        response = self.format_and_send_request(data)
        return response

    def get_data(self, intacct_object: str):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        data = {"read": {"object": intacct_object.upper(), "keys": 5, "fields": "*"}}

        response = self.format_and_send_request(data)
        return response

    def post_journal(self, journal):
        """
        Post journal to Intacct
        """
        data = {"create": {"object": "GLBATCH", "GLBATCH": journal}}

        response = self.format_and_send_request(data)
        return response

    def delete_journal(self, recordno):
        data = {"delete": {"object": "GLBATCH", "keys": recordno}}

        response = self.format_and_send_request(data)
        return response

    def query_entity(
        self, object_type: str, bill_number: str, fields: dict, filters={}
    ):
        """
        Get a sample of data from an endpoint, useful for determining schemas.
        Returns:
            List of Dict in objects schema.
        """
        intacct_object_type = INTACCT_OBJECTS[object_type]
        data = {
            "query": {
                "object": intacct_object_type,
                "select": {"field": fields},
                "pagesize": "1000",
            }
        }
        if len(filters) > 0:
            data["query"].update({"filter": filters})

        entities = self.format_and_send_request(data)
        if int(entities["data"]["@totalcount"]) > 0:
            return entities["data"][intacct_object_type]
        else:
            return None


def get_client(
    *,
    api_url: str,
    company_id: str,
    sender_id: str,
    sender_password: str,
    user_id: str,
    user_password: str,
    headers: Dict,
    use_locations: bool,
    location_id: str
) -> SageIntacctSDK:
    """
    Initializes and returns a SageIntacctSDK object.
    """
    connection = SageIntacctSDK(
        api_url=api_url,
        company_id=company_id,
        sender_id=sender_id,
        sender_password=sender_password,
        user_id=user_id,
        user_password=user_password,
        headers=headers,
        use_locations=use_locations,
        location_id=location_id
    )

    return connection
