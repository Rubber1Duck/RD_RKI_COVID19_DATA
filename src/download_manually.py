from download_pkg import *
import os
from datetime import datetime, date, time
import holidays
import pytz

t_now = datetime.now(pytz.timezone('Europe/Berlin')).time()
today = datetime.now(pytz.timezone('Europe/Berlin')).date()
day_of_week = today.isoweekday()
de_holidays = holidays.CountryHoliday('DE')
t_14 = time(hour=14)
t_22 = time(hour=22)
print(f"Starting at {t_now}")

download_RKI_Impfquotenmonitoring()