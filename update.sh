source $VIRTUAL_ENV/bin/activate
cd src
python schedule.py
python process_RKI_Covid_update.py
python schedule_meta.py
