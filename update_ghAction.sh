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
URL_API_ARCHIV="https://api.github.com/repos/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/commits/main"
COMMIT_JSON=$(curl -s -X GET -H "Accept: application/json" "$URL_API_ARCHIV" 2>&1)
COMMIT_MESSAGE=$(echo $COMMIT_JSON | jq -r '.commit.message')
# check if COMMIT_MESSAGE is not empty
if [[ "$COMMIT_MESSAGE" == "" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : COMMIT_MESSAGE is empty! exit now"
  exit 1
fi
COMMIT_DATE_FULL=$(echo $COMMIT_JSON | jq -r '.commit.committer.date')
COMMIT_DATE=${COMMIT_DATE_FULL:0:10}
# if todays date not equal to lastModified date from RKI server the new data is not (yet) availible, print message and exit
if [[ "Update $DATE" != "$COMMIT_MESSAGE" ]]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : Updated data for $DATE in archive data does not yet exist (modified date: $COMMIT_DATE_FULL)"
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


# print starting message
STARTTIME=`date +%s`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
if [[ "$SOURCEDATA" == "actual" ]]; then
  echo "$DATE2 : Start update with actual data (last modified: $lastModified)"
elif [[ "$SOURCEDATA" == "archive" ]]; then
  echo "$DATE2 : Start update with archive data (last modified: $COMMIT_DATE)"
fi

# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
if [[ "$SOURCEDATA" == "actual" ]]; then
  echo "$DATE2 : executing python download_meta.py"
  python download_meta.py
elif [[ "$SOURCEDATA" == "archive" ]]; then
  echo "$DATE2 : executing python build_archive_meta.py"
  python build_archive_meta.py $COMMIT_DATE_FULL
fi

# Print message, create new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python update_github_action.py"
python update_github_action.py

# Print message, overwriting meta.json
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : overwriting meta.json with meta_new.json"
/bin/mv -f ../dataStore/meta/meta_new.json ../dataStore/meta/meta.json

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
cd ../
VERSION7ZIP="2409"
./get7Zip.sh ${VERSION7ZIP}

# start compress RKI_COVID19_$DATE.csv
cd ./data
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
SIZE1=$(stat -c%s RKI_COVID19_$DATE.csv)
echo -n "$DATE2 : compressing RKI_COVID19_$DATE.csv ($SIZE1 bytes); "
mv "RKI_COVID19_$DATE.csv" "temp.csv"
sort -t',' -n -k 1,1 -k 2,2 -k 3,3 -k 10,10 temp.csv > sort -t',' > "RKI_COVID19_$DATE.csv"
rm -f temp.csv
../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "RKI_COVID19_$DATE.csv.xz" "RKI_COVID19_$DATE.csv"
SIZE2=$(stat -c%s RKI_COVID19_$DATE.csv.xz)
QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
echo "New Size: $SIZE2 bytes = $QUOTE %"
# compress json files in history
cd ../dataStore/history
rm -f ./*.xz
for file in ./*.json
  do 
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    SIZE1=$(stat -c%s $file)
    echo -n "$DATE2 : compressing $file ($SIZE1 bytes); "
    ../../7zzs a -txz -mmt4 -mx=7 -sdel -stl -bso0 -bsp0 "$file.xz" "$file"
    SIZE2=$(stat -c%s $file.xz)
    QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
    echo "New Size: $SIZE2 = $QUOTE %"
  done
rm -rf ../../7zzs

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
ENDTIME=`date +%s`
TOTALSEC=`expr $ENDTIME - $STARTTIME`
TIME=`date -d@$TOTALSEC -u +%H:%M:%S`
echo "$DATE2 : Update finished. Total execution time $TIME ."
