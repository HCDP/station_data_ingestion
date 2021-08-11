from tapipy.tapis import Tapis
import random
from time import sleep


class TapisHandler:

    def __init__(self, config):
        #setup retry
        self.__delay = 0
        self.__retry = config["retry"]

        # self.__client = Tapis(base_url=config["base_url"], username=config["username"], password=config["password"], account_type="user", tenant_id=config["tenant_id"])
        # self.__client.get_tokens()

        # #########
        # #testing#
        # #########

        # token = self.__client.access_token.access_token
        # print(token)
        # print(self.__client.meta.listDBNames())


    def submit(self, data, key_fields):
        self.__submit(data, key_fields, self.__retry, 0)

    def __submit(self, data, key_fields, retry, delay, last_error = None):
        if retry < 0:
            raise Exception("Retry limit exceeded. Last error: %s" % str(last_error))
        
        sleep(delay)

        #submit to db
        try:
            #self.client.meta
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
            self.__submit(data, key_fields, retry - 1, backoff, e)

    #for batches submit remaining data
    def complete(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.complete()