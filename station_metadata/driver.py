import csv
import re
import json
from sys import stderr, argv
from os.path import join

import sys
import os

sys.path.insert(1, os.path.realpath(os.path.pardir))

from ingestion_handler import V2Handler
from date_parser import DateParser

def get_config(config_file):
    config = None
    with open(config_file) as f:
        config = json.load(f)
    return config


######
#args#
######

if len(argv) < 2:
    print("""
        Invalid command line args. Must contain a config file.

        usage:
        station_ingestor.py <config_file> [<state_file>]
    """, file = stderr)
    exit(1)

config_file = argv[1]

config = get_config(config_file)


##################################
##################################



    
file = config["file"]
tapis_config = config["tapis_config"]
prop_translations = config["prop_translations"]

#in case have multiple station setswith separate id universes
id_field = config["id_field"]
station_group = config["station_group"]
nodata = config["nodata"]

tapis_handler = V2Handler(tapis_config)
with open(file, "r") as fd:
    reader = csv.reader(fd)
    header = None
    station_id_index = 0
    for row in reader:
        row = row[1:]
        if header is None:
            #start at 1 because metadata has weird index col (temp????)
            header = row
            for i in range(len(header)):
                prop = header[i]
                trans = prop_translations.get(prop)
                if trans is not None:
                    header[i] = trans
                if header[i] == id_field:
                    station_id_index = i
        else:
            data = {
                "station_group": station_group,
                "station_id": None,
                "id_field": id_field,
                "value": {}
            }
            for i in range(len(row)):       
                prop = header[i]
                value = row[i]
                if value == nodata:
                    value = None
                data["value"][prop] = value
                if i == station_id_index:
                    data["station_id"] = value

            doc = {
                "name": "hcdp_station_metadata",
                "value": data
            }
            
            tapis_handler.create_or_replace(doc, ["station_group", "station_id"])
print("Complete!")
                        