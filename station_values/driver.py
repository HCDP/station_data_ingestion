import csv
import json
from sys import stderr, argv, exit
import sys
from dateutil import parser
import signal
import sys
import os
from traceback import print_exception
import requests
from os.path import isfile

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
        driver.py <config_file> [<state_file>]
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

def uncaught_exception_handler(exctype, value, tb):
    write_state(state_data, state_file)
    print_exception(exctype, value, tb)
    exit(1)

def unraisable_exception_handler(unraisable):
    write_state(state_data, state_file)
    print_exception(unraisable.exc_type, unraisable.exc_value, unraisable.exc_traceback)
    exit(1)

sys.excepthook = uncaught_exception_handler
sys.unraisablehook = unraisable_exception_handler

def sig_handler(sig, frame):
    print("Process interrupted or terminated")
    write_state(state_data, state_file)
    exit(2)

signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)

##################################
##################################

#deconstruct config
data = config["data"]
tapis_config = config["tapis_config"]

tapis_handler = V2Handler(tapis_config)

file_num = 0

for data_item in data:
    files = data_item["files"]

    #optional props
    data_col_start = data_item.get("data_col_start") or 1
    id_col = data_item.get("id_col") or 0
    nodata = data_item.get("nodata")
    if nodata is None:
        nodata = "NA"
    additional_props = data_item.get("additional_properties") or {}
    additional_key_props = data_item.get("additional_key_properties") or []
    #inclusive at both ends
    start_date = data_item.get("start_date")
    end_date = data_item.get("end_date")

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
            #check if file exists on local system
            is_local_file = isfile(file)
            fd = None
            #if local file open and assign fd to file ref
            if is_local_file:
                fd = open(file, "r")
            #otherwise try to get remote file
            #if file does not exist or an error occurs the exception will be caught by the default exception handler and the program will terminate at the invalid file
            else:
                res = requests.get(file, stream = True)
                res.raise_for_status()
                #csv requires text not bytes, decode (assumes utf-8)
                fd = (line.decode("utf-8") for line in res.iter_lines())
            reader = csv.reader(fd)
            dates = None
            range_start = data_col_start
            range_end = None
            #current data row
            row_num = 0
            for row in reader:
                #check if header row
                if dates is None:
                    range_end = len(row)
                    dates = []
                    #transform dates
                    for i in range(len(row)):
                        #if before start index, skip
                        if i >= range_start:
                            #create date parser from current date and period
                            date_handler = DateParser(row[i], period)
                            date = date_handler.getDatetime()
                            date_s = date_handler.getISOString()
                            if date >= start_date and date <= end_date:
                                dates.append(date_s)
                                if date == start_date:
                                    range_start = i
                            elif date > end_date:
                                range_end = i
                                break
                #data rows
                else:
                    #if before the row indicated in the state object, skip
                    if row_num >= state_data["row"]:
                        station_id = row[id_col]
                        #cut values to data range
                        values = row[range_start:range_end]
                        for col in range(len(values)):
                            #if before the column indicated in the state object, skip
                            if col >= state_data["col"]:
                                value = values[col]
                                #if value is nodata skip
                                if value != nodata:
                                    #transform to numeric
                                    value_f = float(value)
                                    
                                    date = dates[col]

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
                                #increment state col
                                state_data["col"] += 1
                        #finished row, move state row, reset col
                        state_data["row"] += 1
                        state_data["col"] = 0
                    #increment current row
                    row_num += 1
            fd.close()
            #finished file, move state file, reset row and col
            state_data["file"] += 1
            state_data["row"] = 0
            state_data["col"] = 0
        #iterate file num
        file_num += 1


state_data["complete"] = True
write_state(state_data, state_file)
print("Complete!")
