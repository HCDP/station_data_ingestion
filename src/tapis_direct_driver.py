import csv
import json
from sys import stderr, argv, exit
import requests
from os.path import isfile
from time import perf_counter
import asyncio

from modules.ingestion_handler import V3Handler
from modules.date_parser import DateParser, isoToDate



######
#args#
######
async def main():
    if len(argv) < 2:
        print("""
            Invalid command line args. Must contain a config file.

            usage:
            driver.py <config_file>
        """, file = stderr)
        exit(1)

    config_file = argv[1]

    config = None
    with open(config_file) as f:
        config = json.load(f)


    #deconstruct config
    data = config["data"]
    tapis_config = config.get("tapisV3_config")

    tapis_handler = V3Handler(tapis_config)

    for data_item in data:
        files = data_item["files"]
        
        doc_name = data_item.get("doc_name")
        if doc_name is None:
            doc_name = "hcdp_station_value"

        #optional props
        replace_duplicates = data_item.get("replace_duplicates")
        if replace_duplicates is None:
            replace_duplicates = True
        data_col_start = data_item.get("data_col_start")
        if data_col_start is None:
            data_col_start = 1
        id_col = data_item.get("id_col")
        if id_col is None:
            id_col = 0
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
            start_date = isoToDate(start_date, period)
        if end_date is not None:
            end_date = isoToDate(end_date, period)
        #make sure end date is not before start date if both defined
        if start_date is not None and end_date is not None and end_date < start_date:
            raise Exception("Invalid date range")

        for i in range(len(files)):
            start_time = perf_counter()

            docs = []
            file = files[i]
            print(f"Processing file {file}")
            #check if file exists on local system
            is_local_file = isfile(file)
            
            fd = None
            res = None
            try:
                #if local file open and assign fd to file ref
                if is_local_file:
                    print("Found local file.")
                    fd = open(file, "r")
                #otherwise try to get remote file
                #if file does not exist or an error occurs the exception will be caught by the default exception handler and the program will terminate at the invalid file
                else:
                    print("Could not find local file. Attempting to open remote file.")
                    res = requests.get(file, stream = True)
                    res.raise_for_status()
                    #csv requires text not bytes, decode (assumes utf-8)
                    fd = (line.decode("utf-8") for line in res.iter_lines())
                reader = csv.reader(fd)
                dates = None
                range_start = data_col_start
                range_end = range_start
                #current data row
                row_num = 0
                
                # process header
                header = next(reader)  
                dates = []
                #transform dates
                for i in range(len(header)):
                    #if before start index, skip
                    if i >= range_start:
                        #create date parser from current date and period
                        date_handler = DateParser(header[i], period)
                        date = date_handler.getDatetime()
                        date_s = date_handler.getISOString()
                        if (start_date is None or date >= start_date) and (end_date is None or date <= end_date):
                            dates.append(date_s)
                            if date == start_date:
                                range_start = i
                            range_end = i + 1
                                
                # process data rows
                for row in reader:
                    # guard against empty rows or stubs
                    if not row or len(row) <= range_start:
                        continue
                    station_id = row[id_col]
                    #cut values to data range
                    values = row[range_start:range_end]
                    for col in range(len(values)):
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
                                "name": doc_name,
                                "value": data
                            }
                            docs.append(doc)
                    row_num +=1
            finally:
                if is_local_file and fd is not None:
                    fd.close()
                elif not is_local_file and res is not None:
                    res.close()
            
            end_time = perf_counter()
            print(f"Completed parsing file {file}: Elapsed time: {end_time - start_time:.6f} seconds")
            
            start_time = perf_counter()
            print(f"Creating Tapis documents")
            
            stats = await tapis_handler.create_docs(docs, key_fields, replace_duplicates)
            
            end_time = perf_counter()
            print(f"Completed creating Tapis documents: Elapsed time: {end_time - start_time:.6f} seconds")
            print(f"Completed processing {file}. Created: {stats['created']}, Replaced: {stats['replaced']}")
            
    print("Complete!")


if __name__ == "__main__":
    asyncio.run(main())