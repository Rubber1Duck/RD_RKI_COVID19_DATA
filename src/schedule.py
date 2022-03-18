from download_pkg import *
from datetime import datetime
import pytz

t_now = datetime.now(pytz.timezone('Europe/Berlin')).time()
print(f"Starting at {t_now}")

# %% each day
try:
    print("Downloading daily RKI Covid Data...")
    download_RKI_COVID19()
except Exception as e:
    print(e)
