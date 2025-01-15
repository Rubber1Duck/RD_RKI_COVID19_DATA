#!/bin/bash

# download static 7zip
echo "$(date '+%Y-%m-%dT%H:%M:%SZ') : download static 7zip"
VERSION7ZIP="2409"
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
  echo -n "$(date '+%Y-%m-%dT%H:%M:%SZ') : recompress $CSVFILE $SIZECSV bytes (old compressed size was $SIZEBEVOR bytes); "
  mv $CSVFILE temp.csv
  sort -t',' -n -k 1,1 -k 2,2 -k 3,3 -k 10,10 temp.csv > sort -t',' > $CSVFILE
  rm -f temp.csv
  ../7zzs a -txz -mmt4 -mx=9 -sdel -stl -bso0 -bsp0 $file $CSVFILE
  SIZEAFTER=$(stat -c%s $file)
  QUOTE=$(gawk "BEGIN {OFMT=\"%.4f\"; print $SIZEAFTER / $SIZEBEVOR * 100;}")
  if [[ $SIZEAFTER -ne $SIZEBEVOR ]]; then
    file=$(basename $file)
    PUSHLIST="./data/$file $PUSHLIST"
    echo "New Size: $SIZEAFTER = $QUOTE %. Added to pushlist!"
  else
    echo "New Size: $SIZEAFTER = $QUOTE %. Not added to pushlist!"
  fi
done
rm -rf ../7zzs
if [[ $PUSHLIST == "" ]]; then
  exit 1
fi
echo "pushlist=$PUSHLIST" >> $GITHUB_OUTPUT
