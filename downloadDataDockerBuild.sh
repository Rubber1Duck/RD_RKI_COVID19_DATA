#!/bin/bash

# date of last monday
lastMonday=$(date -dlast-monday +%Y-%m-%d)

# rubber1duck has daily created json files 
SOURCE="https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master"
URL_METADATA="https://raw.githubusercontent.com/Rubber1Duck/RD_RKI_COVID19_DATA/master/dataStore/meta/meta.json"

# set maxDate to last modified date from rubber1ducks meta data 
maxDate=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | sed -E 's/.*"modified": ([0-9]+)000.*/\1/')
if [ $? -ne 0 ]; then
  echo "unable to get meta.json"
  exit 1
fi
maxDate=$(date -d "@$maxDate" '+%Y-%m-%d')
maxDate=$(date -I -d "$maxDate + 1 day")

# set startDate to lastMonday
startDate=$lastMonday
while [[ "$startDate" != "$maxDate" ]]; do
  # get BL and LK JSON files for the date, store it in ./dataStore/frozen-incidence, print message
  wget -q "$SOURCE/dataStore/frozen-incidence/frozen-incidence_"$startDate"_BL.json" -O "./dataStore/frozen-incidence/frozen-incidence_"$startDate"_BL.json"
  echo "./dataStore/frozen-incidence/frozen-incidence_"$startDate"_BL.json saved"
  wget -q "$SOURCE/dataStore/frozen-incidence/frozen-incidence_"$startDate"_LK.json" -O "./dataStore/frozen-incidence/frozen-incidence_"$startDate"_LK.json"
  echo "./dataStore/frozen-incidence/frozen-incidence_"$startDate"_LK.json saved"
  # increase startDate by 1 day
  startDate=$(date -I -d "$startDate + 1 day")
done
wget -q "$SOURCE/dataStore/new/districts.json" -O "./dataStore/new/districts.json"
echo "./dataStore/new/districts.json saved"
wget -q "$SOURCE/dataStore/new/states.json" -O "./dataStore/new/states.json"
echo "./dataStore/new/states.json saved"
wget -q "$SOURCE/dataStore/accumulated/districts.json" -O "./dataStore/accumulated/districts.json"
echo "./dataStore/accumulated/districts.json saved"
wget -q "$SOURCE/dataStore/accumulated/states.json" -O "./dataStore/accumulated/states.json"
echo "./dataStore/accumulated/states.json saved"
wget -q "$SOURCE/dataStore/history/districts.json" -O "./dataStore/history/districts.json"
echo "./dataStore/history/districts.json saved"
wget -q "$SOURCE/dataStore/history/states.json" -O "./dataStore/history/states.json"
echo "./dataStore/history/states.json saved"
wget -q "$SOURCE/dataStore/meta/meta.json" -O "./dataStore/meta/meta.json"
echo "./dataStore/meta/meta.json saved"