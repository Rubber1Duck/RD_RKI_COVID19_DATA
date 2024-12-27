#!/bin/bash

# set working directory to ./src
cd ./src

#get todays date
DATE=$(date -d $1 '+%Y-%m-%d')

STARTTIME=`date +%s`
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : Start update with archive data"


# Print message, download and modify meta data from RKI server
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
python build_archive_meta.py $1

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
# compress json files in history
cd ../dataStore/history
rm -f ./*.xz
for file in ./*.json
  do 
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    SIZE1=$(stat -c%s $file)
    echo "$DATE2 : start compressing $file ($SIZE1 bytes)"
    ../../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 "$file.xz" "$file"
    SIZE2=$(stat -c%s $file.xz)
    QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
    DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
    echo "$DATE2 : finished compressing $file. New Size: $SIZE2 = $QUOTE %"
  done
rm -rf ../../7zzs

# print message update finished
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
ENDTIME=`date +%s`
TOTALSEC=`expr $ENDTIME - $STARTTIME`
TIME=`date -d@$TOTALSEC -u +%H:%M:%S`
echo "$DATE2 : Update finished. Total execution time $TIME ."

git add ':/*.csv'
git add ':/*.json'
git add ':/*.feather'
git add ':/*.xz'
git status -s
git commit -m "update on $1"
git tag -a "v1.9.$(date -d $1 '+%Y%m%d')" -m "v1.9.$(date -d $1 '+%Y%m%d') release"
git push
git push origin tag $2

