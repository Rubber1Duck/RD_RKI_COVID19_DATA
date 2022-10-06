#!/bin/bash

# print starting message
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update"

#Print message, check/update Bevoelkerung.csv
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python calc_population.py"
python ./src/calc_population.py

# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python download_meta.py"
python ./src/download_meta.py

# Print message, create new json files for date
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : executing python update_github-action.py"
python ./src/update_github-action.py

# Print message, overwriting meta.json
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : overwriting meta.json with meta_new.json"
/bin/mv -f ./dataStore/meta/meta_new.json ./dataStore/meta/meta.json

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Update finished"
