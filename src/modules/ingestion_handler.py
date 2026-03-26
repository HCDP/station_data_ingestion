import requests
import json
from time import sleep
import requests
import json
import random
import urllib3
import os
from datetime import timedelta
from time import perf_counter
import asyncio

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from tapipy.tapis import Tapis

class RecordKeyException(Exception):
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
            raise RecordKeyException("Multiple entries match the specified key data")
        #python can compare dicts with ==
        elif len(matches) == 1:
            duplicate_data["is_duplicate"] = True
            duplicate_data["duplicate_uuid"] = matches[0]["uuid"]
            duplicate_data["changed"] = (matches[0]["value"] != data["value"])
        return duplicate_data


    def create_check_duplicates(self, data, key_fields, replace = True):
        duplicate_data = self.check_duplicate(data, key_fields)

        if not duplicate_data["is_duplicate"]:
            self.create(data)
        elif replace and duplicate_data["changed"]:
            self.replace(data, duplicate_data["duplicate_uuid"])
            

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
        
        
        
        



class V3Handler:
    def __init__(self, config = {}):
        self.__retries = config.get("retries") or int(os.getenv("TAPIS_V3_RETRIES"))
        tenant = config.get("tenant") or os.getenv("TAPIS_V3_TENANT")
        base_url = config.get("url") or os.getenv("TAPIS_V3_URL")
        username = config.get("username") or os.getenv("TAPIS_V3_USERNAME")
        password = config.get("password") or os.getenv("TAPIS_V3_PASSWORD")
        self.__db = config.get("db") or os.getenv("TAPIS_V3_DB")
        self.__collection = config.get("collection") or os.getenv("TAPIS_V3_COLLECTION")
        concurrency = config.get("concurrency") or int(os.getenv("TAPIS_V3_CONCURRENCY")) or 1
        print(concurrency)
        self.__semaphore = asyncio.Semaphore(concurrency)

        # Create python Tapis client for user
        self.__client = Tapis(
            base_url = base_url,
            username = username,
            password = password,
            account_type = "user",
            tenant_id = tenant
        )

        # Generate an Access Token that will be used for all API calls
        self.__check_auth()
        

    def __check_auth(self):
        # if no access token or expires in less than 5 minutes reauth
        if self.__client.access_token is None or self.__client.access_token.expires_in() < timedelta(minutes = 5):
            self.__client.get_tokens()
    

    def __get_backoff(self, delay):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        return backoff


    def __handle_retry(self, method, retries = None, delay = 0, ignore_exceptions = (), **kwargs):
        # Check if client token about to expire and reaut if necessary
        self.__check_auth()
        if delay > 0:
            sleep(delay)
        if retries is None:
            retries = self.__retries
        try:
            data = method(**kwargs)
            if isinstance(data, bytes):
                data = json.loads(data.decode('utf-8'))
            return data
        except Exception as e:
            if type(e) in ignore_exceptions or retries < 1:
                raise e
            else:
                return self.__handle_retry(method, retries = retries - 1, delay = self.__get_backoff(delay), ignore_exceptions = ignore_exceptions, **kwargs)
            
    def __create(self, data, db, collection):
        if isinstance(data, list):
            # Bulk ingest up to 500 docs at a time
            chunk_size = 500
            start = 0
            end = 0
            while end < len(data):
                start = end
                end = start + chunk_size
                if end > len(data):
                    end = len(data)
                chunk = data[start : end]
                self.__handle_retry(self.__client.meta.createDocument, db = db, collection = collection, request_body = chunk)
        else:
            self.__handle_retry(self.__client.meta.createDocument, db = db, collection = collection, request_body = data)



    def __replace(self, uuid, data, db, collection):
        self.__handle_retry(self.__client.meta.replaceDocument, db = db, collection = collection, docId = uuid, request_body = data)
            
    
    def get_uuid(self, uuid, db = None, collection = None):
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        res = self.__handle_retry(self.__client.meta.listDocuments, db = db, collection = collection, docId = uuid)
        return res
            
    
    def query_data(self, data, limit = None, offset = None, db = None, collection = None):
        if limit is None:
            limit = 1000
        if offset is None:
            offset = 0
        
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        query = json.dumps(data)
        res = self.__handle_retry(self.__client.meta.listDocuments, db = db, collection = collection, page = offset + 1, pagesize = limit, filter = query)
        return res
     

    def create_docs_unsafe(self, data, db = None, collection = None):
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        if len(data) > 1:
            self.__create(data, db, collection)
        elif len(data) > 0:
            self.__create(data[0], db, collection)


    async def __check_duplicate(self, doc, key_fields, replace, db, collection, semaphore):
        uuid = None,
        action = None
        async with semaphore: 
            key_data = {
                "name": doc["name"],
            }
            for field in key_fields:
                key = f"value.{field}"
                key_data[key] = doc["value"][field]
            matches = self.query_data(key_data, db = db, collection = collection)
            # Throw an error if multiple docs match the key
            if len(matches) > 1:
                raise RecordKeyException("Multiple entries match the specified key data")
            # Flag to replace if there is a single match, the replace flag is set, and the current value does not match the new value
            elif len(matches) > 0 and replace and matches[0]["value"] != doc["value"]:
                uuid = matches[0]["_id"]["$oid"]
                action = "replace"
            elif len(matches) == 0:
                action = "create"
        return (doc, uuid, action)
    

    async def create_docs(self, data, key_fields, replace = True, db = None, collection = None):
        
        ################################
        ########### profiler ###########
        ################################
        
        start_time = perf_counter()
        print(f"Querying duplicate documents")
        
        ################################
        ################################
        ################################
        
        
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        replace_docs = {}
        create_docs = []
        
        
        tasks = [self.__check_duplicate(doc, key_fields, replace, db, collection, self.__semaphore) for doc in data]
        duplicate_data = await asyncio.gather(*tasks)
        
        for doc, uuid, action in duplicate_data:
            if action == "replace":
                replace_docs[uuid] = doc
            elif action == "create":
                create_docs.append(doc)
                

        ################################
        ########### profiler ###########
        ################################
        
        end_time = perf_counter()
        print(f"Completed querying duplicate documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
        ################################
        ################################
        ################################
                
        
        replaced = 0
        created = 0
        
        ################################
        ########### profiler ###########
        ################################
        
        start_time = perf_counter()
        print(f"Replacing {len(replace_docs)} documents")
        
        ################################
        ################################
        ################################
        
        
        for uuid in replace_docs:
            new_doc = replace_docs[uuid]
            self.__replace(uuid, new_doc, db, collection)
            replaced += 1
            
        
        ################################
        ########### profiler ###########
        ################################
        
        end_time = perf_counter()
        print(f"Completed replacing duplicate documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
        ################################
        ################################
        ################################
        
        
        ################################
        ########### profiler ###########
        ################################
        
        start_time = perf_counter()
        print(f"Creating {len(create_docs)} documents")
        
        ################################
        ################################
        ################################
            
        if len(create_docs) > 1:
            self.__create(create_docs, db, collection)
            created += len(create_docs)
        elif len(create_docs) > 0:
            self.__create(create_docs[0], db, collection)
            created += 1
            

        ################################
        ########### profiler ###########
        ################################
        
        end_time = perf_counter()
        print(f"Completed creating documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
        ################################
        ################################
        ################################
        
            
        return {
            "replaced": replaced,
            "created":  created
        }
        
            

    
    