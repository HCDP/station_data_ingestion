
## Station Value Usage

python3 station_values/driver.py <config_file> [<state_file>]

### Config File

The config file should be a JSON file with fields as follows:

#### Root

| Field Name | Optional | Description |
|------------|----------|-------------|
| data | false | An array of Data objects containing information on how to parse a set of data files |
| tapis_config | false | A TapisConfig object containing configuration inforamtion for Tapis authentication |

#### TapisConfig

| Field Name | Optional | Description |
|------------|----------|-------------|
| tenant_url | false | The tapis tenant URL to use |
| token | false | A Tapis authentication token with access to the tenant's metadata API |
| retry | false | The number of times to retry an operation on error |

#### Data

| Field Name | Optional | Description |
|------------|----------|-------------|
| files | false | An array of files to process. These files should be csv files. Accepts local or remote files. |
| datatype | false | The datatype of the data in the file (e.g. "rainfall" or "temperature") |
| period | false | The period of the data in the file (e.g. "day" or "month") |
| fill | false | The fill type of the data in the file (e.g. "partial" or "raw") |
| additional_properties | true | A JSON object containing additional properties to be included in the document (e.g. {"aggregation": "min"} for min temperature data). Default value {} |
| additional_key_properties | true | An array of additional properties that should be treated as part of the document key for replacement (e.g. ["aggregation"] for temperature data). Default value [] |
| data_col_start | true | The column the data starts on. Default value 1 |
| id_col | true | The column the station ID is found in. Default value 0 |
| nodata | true | The nodata value for the table. Default value "NA" |
| start_date | true | The date to start processing at (inclusive). Dates before this will be ignored. |
| end_date | true | The date to end processing at (inclusive). Dates after this will be ignored. |

### State File

This file will store state information about the run. On failure or completion the current file position will be stored in this file if provided. If the provided state file already exists, execution will resume from the recorded point.
