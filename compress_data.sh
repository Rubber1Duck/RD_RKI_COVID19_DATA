#!/bin/bash

# first check if the update script is running
if [ -f /tmp/update.pid ]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : Update is still in progress! Will wait with compressing data."
  exit 1
fi

cd /usr/src/app/data

if [ ! -f *.csv ]; then
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : No csv file to compress"
  exit 1
fi

DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE : Start compressing csv files"
/usr/bin/xz -zT0  *.csv
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : All csv files are compressed"
