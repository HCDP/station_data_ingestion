import csv
import re
import json
from sys import stderr, argv
from os.path import join

import sys
import os

sys.path.insert(1, os.path.realpath(os.path.pardir))

from ingestion_handler import TapisHandler
from date_parser import DateParser

######
#args#
######

def print_help():
    print("""
        usage:
        station_meta_ingestor.py (<config> | -f <config_file>)
        -h, --help: show this message
    """)

def invalid():
    print("Invalid command line args.\n")
    print_help()
    exit(1)

def help():
    print_help()
    exit(0)

config = None

#load config
if len(argv) < 2:
    invalid()
if argv[1] == "-h" or argv[1] == "--help":
    help()
if argv[1] == "-f":
    if len(argv) < 3:
        invalid()
    
    config_file = argv[2]
    with open(config_file) as f:
        config = json.load(f)
else:
    config = json.loads(argv[1])


##################################
##################################



    
file = config["file"]
tapis_config = config["tapis_config"]
prop_translations = config["prop_translations"]

#in case have multiple station setswith separate id universes
id_field = config["id_field"]
station_group = config["station_group"]

with TapisHandler(tapis_config) as tapis_handler:
    with open(file, "r") as fd:
        reader = csv.reader(fd)
        header = None
        for row in reader:
            if header is None:
                header = row
                for i in range(len(header)):
                    prop = header[i]
                    trans = prop_translations.get(prop)
                    if trans is not None:
                        header[i] = trans
            else:
                for i in range(len(row)):
                    values = {}
                    data = {
                        "id_field": id_field,
                        "station_group": station_group,
                        "station_id": None,
                        "value": {}
                    }
                    prop = header[i]
                    value = row[i]
                    if prop == id_field:
                        data["station_id"] = value
                    data["value"][prop] = value

                doc = {
                    "name": "hcdp_station_metadata",
                    "value": data
                }
                
                tapis_handler.submit(doc, ["station_group", "station_id"])
print("Complete!")
                        