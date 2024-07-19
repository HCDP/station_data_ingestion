#!/bin/bash
echo "[task.sh] Starting Execution."
cd /home/hcdp_tapis_ingestor/station_values

echo "[task.sh] [1/3] Downloading ingestion config."
wget $INGESTION_CONFIG_URL -O config_temp.json

echo "[task.sh] [2/3] Updating date strings in config if requested."
python3 /actor/update_date_string_in_config.py config_temp.json config.json $CUSTOM_DATE

echo "[task.sh] [3/3] Ingesting station values."
python3 driver.py config.json

echo "[task.sh] All done!"