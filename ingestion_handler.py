import requests
import json
from time import sleep
import requests
import json
import random
from enum import Enum
import urllib3
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
        self.__url = config["tenant_url"]

        token = config["token"]

        self.__headers = {
            "Authorization": "Bearer %s" % token,
            "Content-Type": "application/json"
        }

    def __req_with_retry(self, method, url, params, retry, delay = 0):
        #pause for specified amount of time
        sleep(delay)
        res = None
        err = None
        
        def retry_set_err(e):
            nonlocal res
            nonlocal err
            #set error
            err = e
            #get backoff
            backoff = self.__get_backoff(delay)
            #decrease retry number
            next_retry = retry - 1
            #if have retries remaining try again and set res to returned reponse, otherwise just return error response
            if next_retry >= 0:
                res = self.__req_with_retry(method, url, params, next_retry, backoff)

        try:
            #may raise ConnectionError, res will be None if last failure is a connection error
            res = method(url, **params)
        #all request errors inherited from requests.exceptions.RequestException
        except requests.exceptions.RequestException as e:
            #retry request and set error
            retry_set_err(e)
        try:
            #will raise an HTTPError if request returned an error response
            res.raise_for_status()
        except requests.exceptions.HTTPError as e:
            #retry request and set error
            retry_set_err(e)
            
        #return response
        return {
            "response": res,
            "error": err
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

    def query_data(self, data):
        query = json.dumps(data)

        params = {
            "q": query
        }

        request_params = {
            "params": params,
            "headers": self.__headers,
            "verify": False
        }

        res_data = self.__req_with_retry(requests.get, self.__url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]
        
        res = res_data["response"]
        data = res.json()["result"]
        return data

    def query_uuids(self, data):
        uuids = []
        #get result of query
        data = self.query_data(data)
        #list uuids from matching records
        for record in data:
            uuids.append(record["uuid"])
        return uuids


    def create_or_replace(self, data, key_fields, multiple_replace_mode = MultipleMatchMode.ERROR):
        key_data = {
            "name": data["name"],
        }

        for field in key_fields:
            key = "value.%s" % field
            key_data[key] = data["value"][field]

        uuids = self.query_uuids(key_data)
        num_uuids = len(uuids)
        #create new record if none exists matching key fields
        if num_uuids == 0:
            self.create(data)
        #replace data on match and handle multiple matches according to mode
        elif num_uuids == 1 or multiple_replace_mode == MultipleMatchMode.FIRST_MATCH:
            uuid = uuids[0]
            self.replace(data, uuid)
        elif multiple_replace_mode == MultipleMatchMode.FIRST_MATCH_WARN:
            print("Warning: found multiple entries matching the specified key data. Replacing first match...")
            uuid = uuids[0]
            self.replace(data, uuid)
        elif multiple_replace_mode == MultipleMatchMode.ALL:
            #replace first and delete rest
            first = True
            for uuid in uuids:
                if first:
                    self.replace(data, uuid)
                    first = False
                else:
                    self.delete(uuid)
        elif multiple_replace_mode == MultipleMatchMode.ALL_WARN:
            print("Warning: found multiple entries matching the specified key data. Replacing all matches...")
            #replace first and delete rest
            first = True
            for uuid in uuids:
                if first:
                    self.replace(data, uuid)
                    first = False
                else:
                    self.delete(uuid)
        elif multiple_replace_mode == MultipleMatchMode.SKIP_WARN:
            print("Warning: found multiple entries matching the specified key data. Skipping...")
        elif multiple_replace_mode == MultipleMatchMode.ERROR:
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
        meta_url = "%s/%s" % (self.__url, uuid)

        request_params = {
            "headers": self.__headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.delete, meta_url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]

    
    def create(self, data):
        payload = json.dumps(data)

        request_params = {
            "data": payload,
            "headers": self.__headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, self.__url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]


    def replace(self, data, uuid):
        payload = json.dumps(data)

        meta_url = "%s/%s" % (self.__url, uuid)

        request_params = {
            "data": payload,
            "headers": self.__headers,
            "verify": False
        }

        #wrap request in retry and get response
        res_data = self.__req_with_retry(requests.post, meta_url, request_params, self.__retry)

        #if errored out raise last error
        if res_data["error"] is not None:
            raise res_data["error"]



# class V3Handler:

#     def __init__(self, config):
#         self.batch = []

#         #setup retry
#         self.__delay = 0
#         self.__retry = config["retry"]

#         self.__client = Tapis(base_url=config["base_url"], username=config["username"], password=config["password"], account_type="user", tenant_id=config["tenant_id"])
#         self.__client.get_tokens()

#         self.__db = config["db"]
#         self.__collection = config["collection"]

#         #########
#         #testing#
#         #########

#         token = self.__client.access_token.access_token
#         print(token)
#         print(self.__client.meta.listDBNames())


#     def submit(self, document, key_fields, suppress_non_unique):
#         check_exists_config = {
#             "unique_error_level": UniqueErrorLevel.ERROR,
#             "skip_nonunique": False,
#             "document": document,
#             "key_fields": key_fields
#         }
#         #what exception classes does tapis functs throw?
#         update_id = self.__exec_with_retry(self.__check_record_exists, (Exception), check_exists_config, self.__retry, 0)
#         insert_config = {
#             "document": document
#         }
#         if update_id:
#             insert_config["update_id"] = update_id
#             #put in batch?
#             self.__exec_with_retry(self.__update_document, (Exception), insert_config, self.__retry, 0)
#         else:
#             self.__exec_with_retry(self.__create_document, (Exception), insert_config, self.__retry, 0)
#         # #can you batch replace documents???
#         # self.batch.append((data, replace_id))
#         # #if batch replacement just check batch length and only submit if batch size ready
#         # self.__submit(data, self.__retry, 0)



#     def __exec_with_retry(self, exec, exception_classes, data, retry, delay, last_error = None):
#         if retry < 0:
#             raise Exception("Retry limit exceeded. Last error: %s" % str(last_error))
        
#         sleep(delay)

#         try:
#             return exec(data)
#         #only retry if throws TapisException
#         except exception_classes as e:
#             backoff = 0
#             #if first failure backoff of 0.25-0.5 seconds
#             if delay == 0:
#                 backoff = 0.25 + random.uniform(0, 0.25)
#             #otherwise 2-3x current backoff
#             else:
#                 backoff = delay * 2 + random.uniform(0, delay)
#             #retry with one less retry remaining and current backoff
#             self.__submit(data, retry - 1, backoff, e)




#     #batches? need to find out how that works
#     def __create_document(self, data):
#         document = data["document"]
        
#         #what is basic?
#         #t.meta.createDocument(db='',collection=' ',basic=' ',body=' ')
#         self.__client.meta.createDocument(db = self.__db, collection = self.__collection, basic = "???", body = document)


#     def __update_document(self, data):
#         document = data["document"]
#         update_id = data["update_id"]
        
#         #from documentation... so how do I modify it???
#         #t.meta.modifyDocument(db='',collection=' ',documentId=' ')
#         self.__client.meta.modifyDocument


#     def __check_record_exists(self, data):
#         unique_error_level = data["unique_error_level"]
#         skip_nonunique = data["skip_nonunique"]
#         document = data["document"]
#         key_fields = data["key_fields"]


#         record_id = None
#         query = {
#             "name": document["name"]
#         }
#         for field in key_fields:
#             #is value.field the right way to query a nested field in value?
#             query_field = "value.%s"
#             query[query_field] = document["value"][field]
#         #this right?
#         matches = self.__client.meta.listDocuments(db = self.__db, collection = self.__collection, filter = query)
        
#         handle_first = False

#         #there should only be a single match since the fields should create a document key
#         if len(matches) > 0:
#             if len(matches > 1):
#                 if unique_error_level == UniqueErrorLevel.ERROR:
#                     raise RecordNotUniqueException("The record with the provided key fields is non-unique")
#                 if unique_error_level == UniqueErrorLevel.WARNING:
#                     print("Warning: The record with the provided key fields is non-unique")
#                 handle_first = not skip_nonunique
#             else:
#                 handle_first = True

#         if handle_first:
#             record_id = matches[0]["_id"]
#         return record_id
        
        

#     #for batches submit remaining data
#     def complete(self):
#         pass

#     def __enter__(self):
#         return self

#     def __exit__(self, type, value, traceback):
#         self.complete()









    
