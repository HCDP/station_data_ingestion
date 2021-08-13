from tapipy.tapis import Tapis
import random
from time import sleep

class RecordNotUniqueException(Exception):
    pass

class TapisHandler:

    def __init__(self, config):
        self.batch = []

        #setup retry
        self.__delay = 0
        self.__retry = config["retry"]

        self.__client = Tapis(base_url=config["base_url"], username=config["username"], password=config["password"], account_type="user", tenant_id=config["tenant_id"])
        self.__client.get_tokens()

        self.__db = config["db"]
        self.__collection = config["collection"]

        #########
        #testing#
        #########

        token = self.__client.access_token.access_token
        print(token)
        print(self.__client.meta.listDBNames())


    def submit(self, data, key_fields, suppress_non_unique):
        replace_id = self.__check_record_exists(data, key_fields, suppress_non_unique)
        #can you batch replace documents???
        self.batch.append((data, replace_id))
        #if batch replacement just check batch length and only submit if batch size ready
        self.__submit(data, self.__retry, 0)


    def __submit(self, data, retry, delay, last_error = None):
        if retry < 0:
            raise Exception("Retry limit exceeded. Last error: %s" % str(last_error))
        
        sleep(delay)

        #submit to db
        try:
            # self.__client.meta.
            #print(data)
            pass
        except Exception as e:
            backoff = 0
            #if first failure backoff of 0.25-0.5 seconds
            if delay == 0:
                backoff = 0.25 + random.uniform(0, 0.25)
            #otherwise 2-3x current backoff
            else:
                backoff = delay * 2 + random.uniform(0, delay)
            #retry with one less retry remaining and current backoff
            self.__submit(data, retry - 1, backoff, e)

    def __check_record_exists(self, data, key_fields, suppress_non_unique):
        record_id = None
        query = {
            "name": data["name"]
        }
        for field in key_fields:
            #is value.field the right way to query a nested field in value?
            query_field = "value.%s"
            query[query_field] = data["value"][field]
        #this right?
        matches = self.__client.meta.listDocuments(db=self.__db, collection=self.__collection, filter=query)
        
        #there should only be a single match since the fields should create a document key
        if len(matches) > 1 and not suppress_non_unique:
            raise RecordNotUniqueException("The record with the provided key fields is non-unique")
        elif len(matches) > 0:
            record_id = matches[0]["_id"]
        return record_id
        
        

    #for batches submit remaining data
    def complete(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.complete()