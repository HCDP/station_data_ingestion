import requests
import json
from time import sleep
import requests
import json
import random
from enum import Enum
import urllib3
import os
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MultipleMatchMode(Enum):
    ERROR = 0
    FIRST_MATCH = 1
    FIRST_MATCH_WARN = 2
    SKIP = 3
    SKIP_WARN = 4
    ALL = 5
    ALL_WARN = 6

class RecordNotUniqueException(Exception):
    pass


class V2Handler:
    def __init__(self, config):
        self.__retry = config["retry"]
        self.__agave_url = f"{config['tenant_url']}/meta/v2/data"
        self.__hcdp_api_url = config["hcdp_api_url"]
        self.__agave_headers = {
            "Authorization": f"Bearer {config['agave_token']}",
            "Content-Type": "application/json"
        }

        self.__hcdp_headers = {
            "Authorization": f"Bearer {config['hcdp_api_token']}",
            "Content-Type": "application/json"
        }

    def __req_with_retry(self, method, url, params, retry, delay = 0):
        #pause for specified amount of time
        sleep(delay)
        
        def retry_set_err(e):
            #get backoff
            backoff = self.__get_backoff(delay)
            #decrease retry number
            next_retry = retry - 1
            #if have retries remaining try again return recursive result, otherwise just return error response
            if next_retry >= 0:
                return self.__req_with_retry(method, url, params, next_retry, backoff)
            else:
                return {
                    "res": None,
                    "error": e
                }
        res = None
        try:
            #may raise ConnectionError, res will be None if last failure is a connection error
            res = method(url, **params)
        #all request errors inherited from requests.exceptions.RequestException
        except requests.exceptions.RequestException as e:
            #retry request and set error
            return retry_set_err(e)
        try:
            #will raise an HTTPError if request returned an error response
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            #retry request and set error
            return retry_set_err(e)
            
        #return response
        return {
            "response": res,
            "error": None
        }
            

    def __get_backoff(self, delay):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        return backoff

    def __get_success(self, res):
        status = res.status_code
        status_group = status // 100
        return status_group == 2

    def retrieve_by_uuid(self, uuid):
        url = f"{self.__agave_url}/{uuid}"
        params = {
            "headers": self.__agave_headers,
            "verify": False
        }
        res_data = self.__req_with_retry(requests.get, url, params, self.__retry)
        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]
        
        res = res_data["response"]
        data = res.json()["result"]
        return data



    def query_data(self, data, limit = None, offset = None):
        query = json.dumps(data)

        params = {
            "q": query
        }

        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        request_params = {
            "params": params,
            "headers": self.__agave_headers,
            "verify": False
        }

        res_data = self.__req_with_retry(requests.get, self.__agave_url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]
        
        res = res_data["response"]
        data = res.json()["result"]
        return data

    def query_uuids(self, data, limit = None, offset = None):
        uuids = []
        #get result of query
        data = self.query_data(data, limit = limit, offset = offset)
        #list uuids from matching records
        for record in data:
            uuids.append(record["uuid"])
        return uuids

    def check_duplicate(self, data, key_fields):
        duplicate_data = {
            "is_duplicate": False,
            "changed": False,
            "duplicate_uuid": None
        }
        key_data = {
            "name": data["name"],
        }

        for field in key_fields:
            key = f"value.{field}"
            key_data[key] = data["value"][field]
        matches = self.query_data(key_data)
        #just throw an error if multiple, not used in any other way for now
        if len(matches) > 1:
            raise RecordNotUniqueException("Multiple entries match the specified key data")
        #python can compare dicts with ==
        elif len(matches) == 1:
            duplicate_data["is_duplicate"] = True
            duplicate_data["duplicate_uuid"] = matches[0]["uuid"]
            duplicate_data["changed"] = (matches[0]["value"] != data["value"])
        return duplicate_data

    def bulkDelete(self, uuids):
        delete_endpoint = f"{self.__hcdp_api_url}/db/bulkDelete"
        payload = {
            "uuids": uuids
        }
        payload = json.dumps(payload)

        request_params = {
            "data": payload,
            "headers": self.__hcdp_headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, delete_endpoint, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]


    def create_check_duplicates(self, data, key_fields, replace = True, multiple_match_mode = MultipleMatchMode.ERROR):
        key_data = {
            "name": data["name"],
        }

        for field in key_fields:
            key = f"value.{field}"
            key_data[key] = data["value"][field]
        uuids = self.query_uuids(key_data)
        num_uuids = len(uuids)
        #create new record if none exists matching key fields
        if num_uuids == 0:
            self.create(data)
        elif replace:
            #replace data on match and handle multiple matches according to mode
            if num_uuids == 1 or multiple_match_mode == MultipleMatchMode.FIRST_MATCH:
                uuid = uuids[0]
                self.replace(data, uuid)
            elif multiple_match_mode == MultipleMatchMode.FIRST_MATCH_WARN:
                print("Warning: found multiple entries matching the specified key data. Replacing first match...")
                uuid = uuids[0]
                self.replace(data, uuid)
            elif multiple_match_mode == MultipleMatchMode.ALL:
                #replace first and delete rest
                first = True
                for uuid in uuids:
                    if first:
                        self.replace(data, uuid)
                        first = False
                    else:
                        self.delete(uuid)
            elif multiple_match_mode == MultipleMatchMode.ALL_WARN:
                print("Warning: found multiple entries matching the specified key data. Replacing all matches...")
                #replace first and delete rest
                first = True
                for uuid in uuids:
                    if first:
                        self.replace(data, uuid)
                        first = False
                    else:
                        self.delete(uuid)
            elif multiple_match_mode == MultipleMatchMode.SKIP_WARN:
                print("Warning: found multiple entries matching the specified key data. Skipping...")
            elif multiple_match_mode == MultipleMatchMode.ERROR:
                raise RecordNotUniqueException("Multiple entries match the specified key data")
            #skip mode does nothing


    def delete_by_key(self, key_data, multiple_delete_mode = MultipleMatchMode.ALL):
        uuids = self.query_uuids(key_data)
        num_uuids = len(uuids)
        #if 0 matches do nothing
        if num_uuids > 0:
            #delete data on match and handle multiple matches according to mode
            if num_uuids == 1 or multiple_delete_mode == MultipleMatchMode.FIRST_MATCH:
                uuid = uuids[0]
                self.delete(uuid)
            elif multiple_delete_mode == MultipleMatchMode.FIRST_MATCH_WARN:
                print("Warning: found multiple entries matching the specified key data. Deleting first match...")
                uuid = uuids[0]
                self.delete(uuid)
            elif multiple_delete_mode == MultipleMatchMode.ALL:
                for uuid in uuids:
                    self.delete(uuid)
            elif multiple_delete_mode == MultipleMatchMode.ALL_WARN:
                print("Warning: found multiple entries matching the specified key data. Deleting all matches...")
                for uuid in uuids:
                    self.delete(uuid)
            elif multiple_delete_mode == MultipleMatchMode.SKIP_WARN:
                print("Warning: found multiple entries matching the specified key data. Skipping...")
            elif multiple_delete_mode == MultipleMatchMode.ERROR:
                raise RecordNotUniqueException("Multiple entries match the specified key data")
            #skip mode does nothing

    def delete(self, uuid):
        delete_endpoint = f"{self.__hcdp_api_url}/db/delete"
        payload = {
            "uuid": uuid
        }
        payload = json.dumps(payload)

        request_params = {
            "data": payload,
            "headers": self.__hcdp_headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, delete_endpoint, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]

    
    def create(self, data):
        payload = json.dumps(data)

        request_params = {
            "data": payload,
            "headers": self.__agave_headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, self.__agave_url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]


    def replace(self, data, uuid):
        replace_endpoint = f"{self.__hcdp_api_url}/db/replace"
        payload = {
            "uuid": uuid,
            "value": data["value"]
        }
        payload = json.dumps(payload)

        request_params = {
            "data": payload,
            "headers": self.__hcdp_headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, replace_endpoint, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]