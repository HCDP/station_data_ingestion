import csv
import re
import json
from sys import stderr, argv, exit
from os.path import join
from dateutil import parser

import sys
import os

sys.path.insert(1, os.path.realpath(os.path.pardir))

from ingestion_handler import V2Handler
from date_parser import DateParser
from os.path import isfile


def write_state(state_data, state_file):
    if state_file is not None:
        with open(state_file, "w") as f:
            json.dump(state_data, f)

def get_state(state_file):
    #init state
    #note row and column indices are to data rows/columns (does not include header row or metadata columns, cols should be offset from data col start and row should start at row 1)
    state_data = {
        "file": 0,
        "row": 0,
        "col": 0,
        "complete": False
    }
    #if state file not provided return default, won't be written
    if state_file is not None:
        #if file does not exist return default init state
        if isfile(state_file):
            #otherwise set to saved state
            with open(state_file) as f:
                state_data = json.load(f)

    return state_data

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
state_file = None

if len(argv) > 2:
    state_file = argv[2]

config = get_config(config_file)
state_data = get_state(state_file)


##################################
##################################

#deconstruct config
data = config["data"]
tapis_config = config["tapis_config"]

tapis_handler = V2Handler(tapis_config)

file_num = 0

#need to move cols and rows

for data_item in data:
    files = data_item["files"]

    #optional props
    data_col_start = data_item.get("data_col_start") or 1
    id_col = data_item.get("id_col") or 0
    nodata = data_item.get("nodata") or "NA"
    additional_props = data_item.get("additional_properties") or {}
    additional_key_props = data_item.get("additional_key_properties") or []
    #inclusive at both ends
    start_date = config.get("start_date")
    end_date = config.get("end_date")

    #required props
    datatype = data_item["datatype"]
    period = data_item["period"]
    fill = data_item["fill"]

    #for updates
    #add additional key props to base set of key props
    key_fields = ["datatype", "period", "date", "fill", "station_id"] + additional_key_props

    
    #convert dates to datetimes
    if start_date is not None:
        start_date = parser.parse(start_date)
    if end_date is not None:
        end_date = parser.parse(end_date)

    for i in range(len(files)):

        #if file listed in state data is after this one skip
        if file_num >= state_data["file"]:

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
                                    "fill": fill,
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
            #finished file, move state file
            state_data["file"] += 1
        #iterate file num
        file_num += 1


state_data["complete"] = True
write_state(state_data, state_file)
