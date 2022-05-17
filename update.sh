export LC_NUMERIC="en_US.UTF-8"
DATE=$(date '+%Y-%m-%d')

URL_METADATA="https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json"

modified=$(curl -s -X GET -H "Accept: application/json" "$URL_METADATA" 2>&1 | sed -E 's/.*"modified":([0-9]+)000.*/\1/')
modified=$(date -d "@$modified" '+%Y-%m-%d')
if [[ "$DATE" != "$modified" ]]; then
  echo "Updated data for $date does not yet exist (modified date: $modified)"
  exit 1
fi
modified=$(cat Fallzahlen/RKI_COVID19_meta.json 2>&1 | sed -E 's/.*"modified":([0-9]+)000.*/\1/')
modified=$(date -d "@$modified" '+%Y-%m-%d')
if [[ "$DATE" = "$modified" ]]; then
  echo "modified data already downloaded for $date (modified date: $modified)"
  exit 1
fi
# do the action
source $VIRTUAL_ENV/bin/activate
cd /usr/src/app/src
python schedule.py
python process_RKI_Covid_update.py
python schedule_meta.py
