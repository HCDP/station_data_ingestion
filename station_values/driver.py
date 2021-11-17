import csv
import re
import json
from sys import stderr, argv
from os.path import join
from dateutil import parser

import sys
import os

sys.path.insert(1, os.path.realpath(os.path.pardir))

from ingestion_handler import V2Handler
from date_parser import DateParser

######
#args#
######

def print_help():
    print("""
        usage:
        station_ingestor.py (<config> | -f <config_file>)
        -h, --help: show this message
    """)

def invalid():
    print("Invalid command line args. Must contain a config file or object.\n")
    print_help()
    exit(1)

def invalid_flag(flag):
    print("Unrecognized flag %s.\n" % flag)
    print_help()
    exit(1)

def help():
    print_help()
    exit(0)

config = None

#inclusive at both ends
start_date = None
end_date = None
#inclusive at one end [)
start_file = None
end_file = None
exec_id = None

#ah python, why can't you just be normal
i = 1
#use while loop because for loops with range iterator can't increment value in loop
while i < len(argv):
    arg = argv[i]
    if arg[0] == "-":
        #no switch statements either...
        if arg == "-f" or arg == "--file":
            i += 1
            config_file = argv[i]
            with open(config_file) as f:
                config = json.load(f)
        elif arg == "-sd" or arg == "--start_date":
            i += 1
            start_date = argv[i]
        elif arg == "-ed" or arg == "--end_date":
            i += 1
            end_date = argv[i]
        elif arg == "-sf" or arg == "--start_file":
            i += 1
            try:
                start_file = int(argv[i])
            except:
                invalid()
        elif arg == "-ef" or arg == "--end_file":
            i += 1
            try:
                end_file = int(argv[i])
            except:
                invalid()
        elif arg == "-id":
            i += 1
            exec_id = argv[i]
        elif arg == "-h" or arg == "--help":
            help()
        else:
            invalid_flag(arg)
    else:
        config = json.loads(arg)
    i += 1

if config is None:
    invalid()

##################################
##################################

    
data = config["data"]
tapis_config = config["tapis_config"]
tapis_handler = V2Handler(tapis_config)

num_files = 0
for data_item in data:
    num_files += len(data_item["files"])

if start_file is None:
    start_file = 0
if end_file is None:
    end_file = num_files

current_file = 0

for data_item in data:
    files = data_item["files"]

    #optional props
    data_col_start = data_item.get("data_col_start") or 1
    id_col = data_item.get("id_col") or 0
    nodata = data_item.get("nodata") or "NA"
    additional_props = data_item.get("additional_properties") or {}
    additional_key_props = data_item.get("additional_key_properties") or []

    #required props
    datatype = data_item["datatype"]
    period = data_item["period"]

    #for updates
    #add additional key props to base set of key props
    key_fields = ["datatype", "period", "date", "fill", "station_id"] + additional_key_props

    
    #convert dates to datetimes
    if start_date is not None:
        start_date = parser.parse(start_date)
    if end_date is not None:
        end_date = parser.parse(end_date)

    for i in range(len(files)):
        if current_file >= end_file:
            break
        elif current_file >= start_file:
            file = files[i]
            with open(file, "r") as fd:
                reader = csv.reader(fd)
                dates = None
                range_start = data_col_start
                range_end = None
                for row in reader:
                    if dates is None:
                        range_end = len(row)
                        dates = [None] * len(row)
                        #transform dates
                        for i in range(range_start, len(row)):
                            date_handler = DateParser(row[i], period)
                            date = date_handler.getDatetime()
                            date_s = date_handler.getISOString()
                            #skip if before start date
                            if start_date is not None and date < start_date:
                                continue
                            #break if past end date
                            if end_date is not None and date > end_date:
                                break
                            dates[i] = date_s
                        #cut date array to range
                        dates = dates[range_start:range_end]
                    else:
                        station_id = row[id_col]
                        #cut to range matching dates
                        values = row[range_start:range_end]
                        for i in range(len(values)):
                            value = values[i]
                            #if value is nodata skip
                            if value != nodata:
                                #transform to numeric
                                value_f = float(value)
                                date = dates[i]

                                data = {
                                    "datatype": datatype,
                                    "period": period,
                                    "station_id": station_id,
                                    "date": date,
                                    "value": value_f
                                }

                                #set up non-required props
                                for prop_key, prop_value in additional_props.items():
                                    data[prop_key] = prop_value

                                doc = {
                                    "name": "hcdp_station_value",
                                    "value": data
                                }
                                
                                tapis_handler.create_or_replace(doc, key_fields)
        current_file += 1
if exec_id is None:
    print("Complete!")
else:
    print("Complete! id: %s" % exec_id)