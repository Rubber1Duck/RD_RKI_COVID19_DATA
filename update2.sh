#!/bin/bash

# first check if one update script is already running
if [ -f /tmp/update.pid ]; then
  exit 1
fi

#set pythons virtual env
source $VIRTUAL_ENV/bin/activate

# set working directory to ./src
cd /usr/src/app/src

#get todays date
DATE=$(date '+%Y-%m-%d')

#get last modified date from local meta file
lastModifiedLocal=$(cat ../dataStore/meta/meta.json 2>&1 | sed -E 's/.*"modified": ([0-9]+)000.*/\1/')
lastModifiedLocal=$(date -d "@$lastModifiedLocal" '+%Y-%m-%d')

# if todays date equal to local last modified date, then the update is allready done, print message and exit
if [[ "$DATE" == "$lastModifiedLocal" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : data is already updated for $DATE (local modified date: $lastModifiedLocal)"
  # set new crontab to run update1.sh every 15 minutes
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : set crontab to crontab1.file"
  crontab /usr/src/app/crontab1.file
  exit 1
fi

# URL for meta data on RKI server
URL_METADATA="https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/main/Metadaten/zenodo.json"

#get last modified date from RKI Git Hub
lastModified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | jq -r '.version')

# if todays date not equal to lastModified date from RKI server the new data is not (yet) availible, print message and exit
if [[ "$DATE" != "$lastModified" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : Updated data for $DATE does not yet exist (modified date: $lastModified)"
  # set new crontab to run update1.sh every 15 minutes
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : set crontab to crontab1.file"
  crontab /usr/src/app/crontab1.file
  exit 1
fi

# do the action

# touch /tmp/update.pid to signal other update scripts that the update is still running
touch /tmp/update.pid

# print starting message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update"

#Print message, check/update Bevoelkerung.csv
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python calc_population.py"
python calc_population.py

# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python download_meta.py"
python download_meta.py

# Print message, crate new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python update.py"
python update.py

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Update finished"

/bin/mv -f /usr/src/app/dataStore/meta/meta_new.json /usr/src/app/dataStore/meta/meta.json

# start compress RKI_COVID19_$DATE.csv
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : start compressing RKI_COVID19_$DATE.csv"
/usr/bin/xz -zT0  "/usr/src/app/data/RKI_COVID19_$DATE.csv"
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : finished compressing RKI_COVID19_$DATE.csv"

# update is done, delete /tmp/update.pid
rm /tmp/update.pid

# set new crontab to run update2.sh at 1 o'clock (GMT)
crontab /usr/src/app/crontab2.file
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : crontab set to crontab2.file"
