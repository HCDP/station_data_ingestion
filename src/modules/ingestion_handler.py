import json
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


class V3Handler:
    def __init__(self, config = None):
        if config is None:
            config = {}
        self.__retries = config.get("retries") or int(os.getenv("TAPIS_V3_RETRIES", 3))
        tenant = config.get("tenant") or os.getenv("TAPIS_V3_TENANT")
        base_url = config.get("url") or os.getenv("TAPIS_V3_URL")
        username = config.get("username") or os.getenv("TAPIS_V3_USERNAME")
        password = config.get("password") or os.getenv("TAPIS_V3_PASSWORD")
        self.__db = config.get("db") or os.getenv("TAPIS_V3_DB")
        self.__collection = config.get("collection") or os.getenv("TAPIS_V3_COLLECTION")
        concurrency = config.get("concurrency") or int(os.getenv("TAPIS_V3_CONCURRENCY", 1))
        self.__concurrency_sem = asyncio.Semaphore(concurrency)
        self.__auth_lock = asyncio.Lock()
        self.__auth_complete_event = asyncio.Event()
        self.__auth_complete_event.set()
        self.__outbound_tapis_calls_drained_event = asyncio.Event()
        self.__outbound_tapis_calls_drained_event.set()
        self.__outbound_tapis_calls_check_lock = asyncio.Lock()
        self.__outbound_tapis_calls = 0

        # Create python Tapis client for user
        self.__client = Tapis(
            base_url = base_url,
            username = username,
            password = password,
            account_type = "user",
            tenant_id = tenant
        )

        # Generate an Access Token that will be used for all API calls
        self.__client.get_tokens()
        

    async def __check_auth(self):
        # If no access token or expires in less than 5 minutes reauth
        if self.__client.access_token is None or self.__client.access_token.expires_in() < timedelta(minutes = 5):
            # Acquire auth lock
            async with self.__auth_lock:
                # check if another task already completed the auth procedure
                if self.__client.access_token is None or self.__client.access_token.expires_in() < timedelta(minutes = 5):
                    # Clear auth complete event to prevent any more tasks from kicking off
                    self.__auth_complete_event.clear()
                    # Wait for all outbound tasks to complete
                    await self.__outbound_tapis_calls_drained_event.wait()
                    try:
                        # Auth in background
                        await asyncio.to_thread(self.__client.get_tokens)
                    finally:
                        # Set auth complete
                        self.__auth_complete_event.set()
    

    def __get_backoff(self, delay):
        backoff = 0
        #if first failure backoff of 0.25-0.5 seconds
        if delay == 0:
            backoff = 0.25 + random.uniform(0, 0.25)
        #otherwise 2-3x current backoff
        else:
            backoff = delay * 2 + random.uniform(0, delay)
        return backoff


    async def __execute_retry_method(self, method, **kwargs):
        # Execute with a maximum concurrency as configured
        async with self.__concurrency_sem:
            # Wait for auth to complete is running
            await self.__auth_complete_event.wait()
            res = None
            # Ensure counter and event checking steps do not have a race condition
            async with self.__outbound_tapis_calls_check_lock:
                # Add to outbound tapis calls count
                self.__outbound_tapis_calls += 1
                # Outbound calls are not drained
                self.__outbound_tapis_calls_drained_event.clear()
            try:
                res = await asyncio.to_thread(method, **kwargs)
            finally:
                # Ensure counter and event checking steps do not have a race condition
                async with self.__outbound_tapis_calls_check_lock:
                    self.__outbound_tapis_calls -= 1
                    if self.__outbound_tapis_calls < 1:
                        self.__outbound_tapis_calls_drained_event.set()
            return res
        

    async def __handle_retry(self, method, retries = None, delay = 0, ignore_exceptions = (), **kwargs):
        # Check if client token about to expire and reauth if necessary
        await self.__check_auth()
        if delay > 0:
            await asyncio.sleep(delay)
        if retries is None:
            retries = self.__retries
        try:
            data = await self.__execute_retry_method(method, **kwargs)
            if isinstance(data, bytes):
                data = json.loads(data.decode('utf-8'))
            return data
        except Exception as e:
            if type(e) in ignore_exceptions or retries < 1:
                raise e
            else:
                return await self.__handle_retry(method, retries = retries - 1, delay = self.__get_backoff(delay), ignore_exceptions = ignore_exceptions, **kwargs)
            
    async def __create(self, data, db, collection):
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
                await self.__handle_retry(self.__client.meta.createDocument, db = db, collection = collection, request_body = chunk)
        else:
            await self.__handle_retry(self.__client.meta.createDocument, db = db, collection = collection, request_body = data)



    async def __replace(self, uuid, data, db, collection):
        await self.__handle_retry(self.__client.meta.replaceDocument, db = db, collection = collection, docId = uuid, request_body = data)
            
    
    async def get_uuid(self, uuid, db = None, collection = None):
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        res = await self.__handle_retry(self.__client.meta.listDocuments, db = db, collection = collection, docId = uuid)
        return res
            
    
    async def query_data(self, data, limit = None, offset = None, db = None, collection = None):
        if limit is None:
            limit = 1000
        if offset is None:
            offset = 0
        
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        query = json.dumps(data)
        res = await self.__handle_retry(self.__client.meta.listDocuments, db = db, collection = collection, page = offset + 1, pagesize = limit, filter = query)
        return res
     

    async def create_docs_unsafe(self, data, db = None, collection = None):
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        if len(data) > 1:
            await self.__create(data, db, collection)
        elif len(data) > 0:
            await self.__create(data[0], db, collection)


    async def __check_duplicate(self, doc, key_fields, replace, db, collection):
        uuid = None
        action = None

        key_data = {
            "name": doc["name"],
        }
        for field in key_fields:
            key = f"value.{field}"
            key_data[key] = doc["value"][field]
        matches = await self.query_data(key_data, db = db, collection = collection)
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
        start_time = perf_counter()
        print(f"Querying duplicate documents")
        
        if db is None:
            db = self.__db
        if collection is None:
            collection = self.__collection
            
        replace_docs = {}
        create_docs = []
        
        
        tasks = [self.__check_duplicate(doc, key_fields, replace, db, collection) for doc in data]
        duplicate_data = await asyncio.gather(*tasks)
        
        for doc, uuid, action in duplicate_data:
            if action == "replace":
                replace_docs[uuid] = doc
            elif action == "create":
                create_docs.append(doc)
        
        end_time = perf_counter()
        print(f"Completed querying duplicate documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
        start_time = perf_counter()
        print(f"Replacing {len(replace_docs)} documents")
        
        tasks = [self.__replace(uuid, replace_docs[uuid], db, collection) for uuid in replace_docs]
        await asyncio.gather(*tasks)
        
        end_time = perf_counter()
        print(f"Completed replacing duplicate documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
        start_time = perf_counter()
        print(f"Creating {len(create_docs)} documents")
            
        if len(create_docs) > 1:
            await self.__create(create_docs, db, collection)
        elif len(create_docs) > 0:
            await self.__create(create_docs[0], db, collection)
        
        end_time = perf_counter()
        print(f"Completed creating documents: Elapsed time: {end_time - start_time:.6f} seconds")
        
            
        return {
            "replaced": len(replace_docs),
            "created":  len(create_docs)
        }
        