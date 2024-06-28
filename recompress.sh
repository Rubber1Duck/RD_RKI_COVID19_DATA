#!/bin/bash

# download static 7zip
echo "$(date '+%Y-%m-%dT%H:%M:%SZ') : download static 7zip"
VERSION7ZIP="2301"
./get7Zip.sh ${VERSION7ZIP}
PUSHLIST=""

# start recompress all RKI_COVID19_*.csv.zx
cd ./data
for file in ./RKI_COVID19_*.csv.xz
do
  SIZEBEVOR=$(stat -c%s $file)
  ../7zzs e -bso0 -bsp0 $file
  rm -f $file
  CSVFILE="${file%.*}"
  SIZECSV=$(stat -c%s $CSVFILE)
  echo "$(date '+%Y-%m-%dT%H:%M:%SZ') : recompressing $CSVFILE $SIZECSV bytes (old compressed size was $SIZEBEVOR bytes)"
  ../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 $file $CSV
  SIZEAFTER=$(stat -c%s $file)
  QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZEAFTER / $SIZEBEVOR * 100;}")
  if [[ $SIZEAFTER -nq $SIZEBEVOR ]]; then
    file=$(basename $file)
    PUSHLIST="./data/$file $PUSHLIST"
    echo "$(date '+%Y-%m-%dT%H:%M:%SZ') : done recompressing $file. New Size: $SIZEAFTER = $QUOTE %. Added to pushlist!"
  else
    echo "$(date '+%Y-%m-%dT%H:%M:%SZ') : done recompressing $file. New Size: $SIZEAFTER = $QUOTE %. Not added to pushlist!"
  fi
done
rm -rf ../7zzs
if [[ $PUSHLIST == "" ]]; then
  exit 1
fi
echo "pushlist=$PUSHLIST" >> $GITHUB_OUTPUT