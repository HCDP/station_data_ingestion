import pandas as pd
from modules.date_parser import DateParser2, isoToDate
import requests
from os import environ
from sys import stderr, argv, exit
import json
from datetime import datetime

METADATA_COL_TRANSLATIONS = {
    "SKN": "skn",
    "Station.Name": "name",
    "Observer": "observer",
    "Network": "network",
    "Island": "island",
    "ELEV.m.": "elevation_m",
    "LAT": "lat",
    "LON": "lng",
    "NCEI.id": "ncei_id",
    "NWS.id": "nws_id",
    "NESDIS.id": "nesdis_id",
    "SCAN.id": "scan_id",
    "SMART_NODE_RF.id": "smart_node_rf_id"
}

HCDP_API_TOKEN = environ.get("HCDP_API_TOKEN")


def write_to_hcdp_api(type, location, values, additional_key_fields, replace):
    url = f"https://api.hcdp.ikewai.org/stations/{type}"
    headers = {
        "Authorization": f"Bearer {HCDP_API_TOKEN}"
    }
    body = {
        "location": location,
        "values": values,
        "additionalKeyFields": additional_key_fields,
        "replace": replace
    }
    res = requests.post(url, json = body, headers = headers)
    res.raise_for_status()
    return res.json()


def filter_cols(df: pd.DataFrame, period, start_date: datetime, end_date: datetime):
    date_parser = DateParser2(period)
    def translate(col):
        translation = None
        categories = ( False, False )
        if col in METADATA_COL_TRANSLATIONS:
            translation = METADATA_COL_TRANSLATIONS[col]
            categories = ( True, True ) if translation == "skn" else ( True, False )
        elif(date_parser.match(col)):
            dt = date_parser.header2date(col)
            if (start_date is None or dt >= start_date) and (end_date is None or dt <= end_date):
                translation = date_parser.date2value(dt)
                categories = ( False, True )
        return ( translation, categories )
    
    translations = {}
    metadata_cols = []
    value_cols = []
    for col in df.columns:
        translation, categories = translate(col)
        include_metadata, include_values = categories
        if translation is not None: 
            translations[col] = translation
        if include_metadata:
            metadata_cols.append(translation)
        if include_values:
            value_cols.append(translation)
    
    translated = df.rename(columns = translations)
    
    metadata: pd.DataFrame = translated[metadata_cols]
    values: pd.DataFrame = translated[value_cols]
    return ( metadata, values )




def main():
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

    for data_item in data:
        files = data_item["files"]

        
        #optional props
        write_metadata = data_item.get("write_metadata", True)
        replace_duplicates = data_item.get("replace_duplicates", True)
        nodata = data_item.get("nodata", "NA")
        additional_props = data_item.get("additional_properties", {})

        # Temp method for getting location
        location = additional_props.get("location", "hawaii")
        
        additional_key_props = data_item.get("additional_key_properties", [])
        #inclusive at both ends
        start_date = data_item.get("start_date")
        end_date = data_item.get("end_date")
        
        period = data_item["period"]
        static_properties = {
            "datatype": data_item["datatype"],
            "period": period,
            "fill": data_item["fill"],
            **additional_props
        }
        
        #convert dates to datetimes
        if start_date is not None:
            start_date = isoToDate(start_date, period)
        if end_date is not None:
            end_date = isoToDate(end_date, period)
        #make sure end date is not before start date if both defined
        if start_date is not None and end_date is not None and end_date < start_date:
            raise Exception("Invalid date range")

        for i in range(len(files)):
            file = files[i]
            print(f"Processing file {file}")
            
            # will handle local files or URLs
            df = pd.read_csv(file, keep_default_na = False)
            metadata, values = filter_cols(df, period, start_date, end_date)
            
            # if should write metadata handle metadata
            if write_metadata:
                metadata["id_field"] = "skn"
                metadata["station_group"] = f"{location}_climate_primary"
                print(metadata)
                metadata_docs = metadata.to_dict(orient = "records")
                # cleanup nodata values in metadata
                metadata_docs = [{k: v for k, v in doc.items() if v != nodata} for doc in metadata_docs]
                
                stats = write_to_hcdp_api("metadata", location, metadata_docs, [], replace_duplicates)
                print(f"Completed writing metadata for {file}. Created {stats['created']}, Replaced {stats['replaced']}")
            
            # process values
            values = values.melt(
                id_vars = ['skn'], 
                var_name = 'date', 
                value_name = 'value'
            )
            values = values.rename(columns = {'skn': 'station_id'})
            values = values[values['value'] != nodata]
                        
            for key, value in static_properties.items():
                values[key] = value
            print(values)
            value_docs = values.to_dict(orient = "records")
            
            stats = write_to_hcdp_api("value", location, value_docs, additional_key_props, replace_duplicates)
            print(f"Completed writing values for {file}. Created {stats['created']}, Replaced {stats['replaced']}")


if __name__ == "__main__":
    main()