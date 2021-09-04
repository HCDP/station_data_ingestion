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
        station_ingestor.py (<config> | -f <config_file>)
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

    
data = config["data"]
tapis_config = config["tapis_config"]
tapis_handler = TapisHandler(tapis_config)
for data_item in data:
    files = data_item["files"]

    #optional props
    #inclusive at both ends
    start_date = data_item.get("start_date")
    end_date = data_item.get("end_date")
    data_col_start = data_item.get("data_col_start") or 1
    id_col = data_item.get("id_col") or 0
    nodata = data_item.get("nodata") or "NA"

    #required props
    datatype = data_item["datatype"]
    #if ever have variation without period (e.g. specific datetimes) need to update date handling anyway
    period = data_item["period"]
    # fill = data_item["fill"]
    # tier = data_item["tier"]
    
    additional_props = data_item["additional_properties"]
    additional_key_props = data_item["additional_key_properties"]

    #for updates
    #add additional key props to base set of key props
    key_fields = ["datatype", "period", "date", "station_id"] + additional_key_props

    for file in files:
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
                            #move start index
                            range_start += 1
                            continue
                        #break if past end date
                        if end_date is not None and date > end_date:
                            #set end col
                            range_end = i
                            break
                        dates[i] = date_s
                    #cut date array to range
                    dates = dates[range_start:range_end]
                    print(dates[0])
                    print(dates[len(dates) - 1])
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
                            for prop in additional_props:
                                data[prop] = data_item[prop]

                            doc = {
                                "name": "hcdp_station_value",
                                "value": data
                            }
                            
                            tapis_handler.submit(doc, key_fields)
print("Complete!")
                                