#!/bin/bash

# download static 7zip
DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
echo "$DATE2 : download static 7zip"
VERSION7ZIP="2301"
./get7Zip.sh ${VERSION7ZIP}
PUSHLIST=""

# start recompress all RKI_COVID19_*.csv.zx
cd ./data
for file in ./RKI_COVID19_*.csv.xz
do
  SIZE1=$(stat -c%s $file)
  ../7zzs e -bso0 -bsp0 $file
  rm -f $file
  CSV="${file%.*}"
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  echo "$DATE2 : start recompressing $file ($SIZE1 bytes)"
  ../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 $file $CSV
  SIZE2=$(stat -c%s $file)
  QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZE2 / $SIZE1 * 100;}")
  DATE2=$(date '+%Y-%m-%dT%H:%M:%SZ')
  if [[ $SIZE2 -lt $SIZE1 ]]; then
    PUSHLIST="./data/$file $PUSHLIST"
    echo "$DATE2 : finished recompressing $file. New Size: $SIZE2 = $QUOTE %. Added to pushlist!"
    echo "$DATE2 : pushlist now: $PUSHLIST"
  else
    echo "$DATE2 : finished recompressing $file. New Size: $SIZE2 = $QUOTE %. Not added to pushlist!"
  fi
done
rm -rf ../7zzs
if [[ $PUSHLIST -eq "" ]]; then
  exit 1
fi
echo "pushlist=$PUSHLIST" >> $GITHUB_OUTPUT