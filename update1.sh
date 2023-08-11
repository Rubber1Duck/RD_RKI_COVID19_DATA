#!/bin/bash

# first check if this script is already running
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
  # set new crontab to run update2.sh at 1 o'clock (GMT)
  crontab /usr/src/app/crontab2.file
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : crontab set to crontab2.file"
  exit 1
fi

#try RKI Git Hub Archiv
URL_METAARCHIVE="https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/main/Metadaten/zenodo.json"
lastModifiedArchive=$(curl -s -X GET -H "Accept: application/json" "$URL_METAARCHIVE" 2>&1 | jq -r '.version')
if [[ "$lastModifiedArchive" == "" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : lastModifiedArchive is empty! exit now"
  exit 1
fi
# if todays date not equal to lastModified date from RKI server the new data is not (yet) availible, print message and exit
if [[ "$DATE" != "$lastModifiedArchive" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : Updated data for $DATE in archive data does not yet exist (modified date: $lastModifiedArchive)"
  # URL for meta data on RKI server
  URL_METADATA="https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/main/Metadaten/zenodo.json"
  lastModified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | jq -r '.version')
  if [[ "$lastModified" == "" ]]; then
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    echo "$DATE2 : lastModified is empty! exit now"
    exit 1
  fi
  if [[ "$DATE" != "$lastModified" ]]; then
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    echo "$DATE2 : Updated data for $DATE in actual data does not yet exist (modified date: $lastModified)"
    exit 1
  else
    SOURCEDATA="actual"
  fi
else
  SOURCEDATA="archive"
fi

# do the action

# touch /tmp/update.pid to signal other update scripts that the update is still running
touch /tmp/update.pid

# print starting message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
if [[ "$SOURCEDATA" == "actual" ]]; then
  echo "$DATE2 : Start update with actual data (last modified: $lastModified)"
elif [[ "$SOURCEDATA" == "archive" ]]; then
  echo "$DATE2 : Start update with archive data (last modified: $lastModifiedArchive)"
fi

#Print message, check/update Bevoelkerung.csv
#DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
#echo "$DATE2 : executing python calc_population.py"
#python calc_population.py

# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
if [[ "$SOURCEDATA" == "actual" ]]; then
  echo "$DATE2 : executing python download_meta.py"
  python download_meta.py
elif [[ "$SOURCEDATA" == "archive" ]]; then
  echo "$DATE2 : executing python download_meta_archive.py"
  python download_meta_archive.py
fi

# Print message, create new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python update.py"
python update.py

/bin/mv -f /usr/src/app/dataStore/meta/meta_new.json /usr/src/app/dataStore/meta/meta.json

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Update finished"

# start compress RKI_COVID19_$DATE.csv
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
SIZE1=$(stat -c%s /usr/src/app/data/RKI_COVID19_$DATE.csv)
echo "$DATE2 : start compressing RKI_COVID19_$DATE.csv ($SIZE1 bytes)"
../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "/usr/src/app/data/RKI_COVID19_$DATE.csv.xz" "/usr/src/app/data/RKI_COVID19_$DATE.csv"
SIZE2=$(stat -c%s /usr/src/app/data/RKI_COVID19_$DATE.csv.xz)
QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : finished compressing RKI_COVID19_$DATE.csv. New Size: $SIZE2 = $QUOTE %"

# update is done, delete /tmp/update.pid
rm /tmp/update.pid

# set new crontab to run update2.sh at 1 o'clock (GMT)
crontab /usr/src/app/crontab2.file
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : crontab set to crontab2.file"
