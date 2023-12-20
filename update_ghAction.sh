#!/bin/bash

# set working directory to ./src
cd ./src

#get todays date
DATE=$(date '+%Y-%m-%d')

#get last modified date from local meta file
lastModifiedLocal=$(cat ../dataStore/meta/meta.json 2>&1 | sed -E 's/.*"modified": ([0-9]+)000.*/\1/')
lastModifiedLocal=$(date -d "@$lastModifiedLocal" '+%Y-%m-%d')

# if todays date equal to local last modified date, then the update is allready done, print message and exit
if [[ "$DATE" == "$lastModifiedLocal" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : data is already updated for $DATE (local modified date: $lastModifiedLocal)"
  exit 1
fi

#try RKI Git Hub Archiv
#URL_METAARCHIV="https://raw.githubusercontent.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/main/Metadaten/zenodo.json"
#lastModifiedArchive=$(curl -s -X GET -H "Accept: application/json" "$URL_METAARCHIV" 2>&1 | jq -r '.version')
#if [[ "$lastModifiedArchive" == "" ]]; then
#  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
#  echo "$DATE2 : lastModifiedArchive is empty! exit now"
#  exit 1
#fi
# if todays date not equal to lastModified date from RKI server the new data is not (yet) availible, print message and exit
#if [[ "$DATE" != "$lastModifiedArchive" ]]; then
#  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
#  echo "$DATE2 : Updated data for $DATE in archive data does not yet exist (modified date: $lastModifiedArchive)"
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
#else
#  SOURCEDATA="archive"
#fi

# print starting message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
if [[ "$SOURCEDATA" == "actual" ]]; then
  echo "$DATE2 : Start update with actual data (last modified: $lastModified)"
elif [[ "$SOURCEDATA" == "archive" ]]; then
  echo "$DATE2 : Start update with archive data (last modified: $lastModifiedArchive)"
fi

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
echo "$DATE2 : executing python update_github-action.py"
python update_github-action.py

# Print message, update Fallzahlen
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python fallzahlen_update.py"
python fallzahlen_update.py

# Print message, overwriting meta.json
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : overwriting meta.json with meta_new.json"
/bin/mv -f ../dataStore/meta/meta_new.json ../dataStore/meta/meta.json

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
cd ../
VERSION7ZIP="2301"
./get7Zip.sh ${VERSION7ZIP}

# start compress RKI_COVID19_$DATE.csv
cd ./data
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
SIZE1=$(stat -c%s RKI_COVID19_$DATE.csv)
echo "$DATE2 : start compressing RKI_COVID19_$DATE.csv ($SIZE1 bytes)"
../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "RKI_COVID19_$DATE.csv.xz" "RKI_COVID19_$DATE.csv"
SIZE2=$(stat -c%s RKI_COVID19_$DATE.csv.xz)
QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : finished compressing RKI_COVID19_$DATE.csv. New Size: $SIZE2 = $QUOTE %"
rm -rf ../7zzs

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Update finished"
