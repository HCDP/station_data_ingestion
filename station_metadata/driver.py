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
        driver.py <config_file> [<state_file>]
    """, file = stderr)
    exit(1)

config_file = argv[1]

config = get_config(config_file)


##################################
##################################



    
file = config["file"]
tapis_config = config["tapis_config"]
prop_translations = config["prop_translations"]

#in case have multiple station sets with separate id universes
id_field = config["id_field"]
station_group = config["station_group"]
nodata = config["nodata"]

start_col = config.get("start_col")
if start_col is None:
    start_col = 0
end_col = config.get("end_col")


tapis_handler = V2Handler(tapis_config)
with open(file, "r") as fd:
    reader = csv.reader(fd)
    header = None
    for row in reader:
        if end_col is None:
            end_col = len(row)
        row = row[start_col:end_col]
        if header is None:
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
                "id_field": id_field,
            }
            for i in range(len(row)):       
                prop = header[i]
                value = row[i]
                if value != nodata:
                    data[prop] = value

            doc = {
                "name": "hcdp_station_metadata",
                "value": data
            }
            key_fields = ["station_group", id_field]
            tapis_handler.create_check_duplicates(doc, key_fields)
print("Complete!")
                        