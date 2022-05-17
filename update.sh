#!/bin/bash

source $VIRTUAL_ENV/bin/activate
cd /usr/src/app/src

DATE=$(date '+%Y-%m-%d')

URL_METADATA="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json"

modified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | sed -E 's/.*"modified":([0-9]+)000.*/\1/')
modified=$(date -d "@$modified" '+%Y-%m-%d')
if [[ "$DATE" != "$modified" ]]; then
  echo "Updated data for $DATE does not yet exist (modified date: $modified)"
  exit 1
fi
modified=$(cat ../Fallzahlen/RKI_COVID19_meta.json 2>&1 | sed -E 's/.*"modified": ([0-9]+)000.*/\1/')
modified=$(date -d "@$modified" '+%Y-%m-%d')
if [[ "$DATE" = "$modified" ]]; then
  echo "modified data already downloaded for $DATE (modified date: $modified)"
  exit 1
fi
if [ -f /tmp/update.pid ]; then
  echo "update is still in progress!"
  exit 1
fi
# do the action
touch /tmp/update.pid
echo "Start update"
python schedule.py
python process_RKI_Covid_update.py
python schedule_meta.py
rm /tmp/update.pid
echo "update finished"

