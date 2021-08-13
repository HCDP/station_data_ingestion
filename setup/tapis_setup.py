
from tapipy.tapis import Tapis
config = None

client = Tapis(base_url=config["base_url"], username=config["username"], password=config["password"], account_type="user", tenant_id=config["tenant_id"])
client.get_tokens()

db_name = "HCDP"
collections = ["station_value", "station_metadata"]

#what do these return? what happens if db or collection already exists? great documentation
#create database
client.meta.createDB(db=db_name)

#create collections
for collection in collections:
    client.meta.createCollection(db=db_name, collection=collection)
