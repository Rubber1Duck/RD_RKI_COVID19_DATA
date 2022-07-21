#!/bin/bash

# first check if this script is already running
if [ -f /tmp/update.pid ]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2: Update is still in progress!"
  exit 1
fi

#set pythons virtual env
source $VIRTUAL_ENV/bin/activate

# set worling directory to ./src
cd /usr/src/app/src

#get todays date
DATE=$(date '+%Y-%m-%d')

# URL for meta data on RKI server
URL_METADATA="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json"

#get last modified date from RKI server
lastModified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | sed -E 's/.*"modified":([0-9]+)000.*/\1/')
lastModified=$(date -d "@$lastModified" '+%Y-%m-%d')

# if todays date not equal to lastModified date from RKI server the new data is not (yet) availible, print message and exit
if [[ "$DATE" != "$lastModified" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2: Updated data for $DATE does not yet exist (modified date: $lastModified)"
  # set new crontab to run update1.sh every 15 minutes
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2: set crontab to crontab1.file"
  crontab /usr/src/app/crontab1.file
  crontab -l
  exit 1
fi

#get last modified date from local meta file
lastModifiedLocal=$(cat ../dataStore/meta/meta.json 2>&1 | sed -E 's/.*"modified": ([0-9]+)000.*/\1/')
lastModifiedLocal=$(date -d "@$lastModifiedLocal" '+%Y-%m-%d')

# if todays date equal to local last modified date, then the update is allready done, print message and exit
if [[ "$DATE" == "$lastModifiedLocal" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2: data is already updated for $DATE (local modified date: $lastModifiedLocal)"
  # set new crontab to run update1.sh every 15 minutes
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2: set crontab to crontab1.file"
  crontab /usr/src/app/crontab1.file
  crontab -l
  exit 1
fi

# do the action

# cron starts this script every 15 minutes,
# if a update task is running more then 15 minutes, cron starts a new updatejob!
# to prevent this touch the file /tmp/update.pid. This file is checked at the beginning of this script
touch /tmp/update.pid

# print starting message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2: Start update"

# Print message, crate new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2: executing python process_update_docker.py"
python update.py

# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2: executing python download_meta.py"
python download_meta.py

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2: Update finished"

# update is done, delete /tmp/update.pid
rm /tmp/update.pid

# set new crontab to run update2.sh at 1 o'clock (GMT)
crontab /usr/src/app/crontab2.file
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2: crontab set to crontab2.file"
crontab -l
