import os
import re
from datetime import *
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from repo_tools_pkg.file_tools import find_latest_file
import pytz

t_now = datetime.now(pytz.timezone('Europe/Berlin')).time()
print(f"Starting at {t_now}")

# %%
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
path_bevoelkerung_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Bevoelkerung',
                               'Bevoelkerung.csv')

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
pattern = 'RKI_COVID19'
dtypes_fallzahlen_LK = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'IdLandkreis': 'Int32',
                     'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32',
                     'AnzahlTodesfall_neu': 'Int32', 'AnzahlFall_7d': 'Int32', 'report_date': 'object',
                     'meldedatum_max': 'object', 'population': 'Int32', 'incidence_7d': 'float64'}
dtypes_fallzahlen_BL = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'AnzahlFall': 'Int32',
                     'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32', 'AnzahlTodesfall_neu': 'Int32',
                     'AnzahlFall_7d': 'Int32', 'report_date': 'object', 'meldedatum_max': 'object',
                     'population': 'Int32', 'incidence_7d': 'float64'}
dtypes_bevoelkerung = {'AGS': 'Int32', 'Name': 'object', 'GueltigAb': 'object', 'GueltigBis': 'object', 'Einwohner': 'Int32'}
dtypes_covid = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'IdLandkreis': 'Int32', 'NeuerFall': 'Int8',
                'NeuerTodesfall': 'Int8', 'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'Meldedatum': 'object'}
key_list_LK = ['Datenstand', 'IdBundesland', 'IdLandkreis']
key_list_BL = ['Datenstand', 'IdBundesland']

# %% open bevoelkerung.csv
bevoelkerung_df = pd.read_csv(path_bevoelkerung_csv, usecols=dtypes_bevoelkerung.keys(), dtype=dtypes_bevoelkerung)
bevoelkerung_df['GueltigAb'] = pd.to_datetime(bevoelkerung_df['GueltigAb'])
bevoelkerung_df['GueltigBis'] = pd.to_datetime(bevoelkerung_df['GueltigBis'])

# %% read covid latest
covid_path_latest, date_latest = find_latest_file(os.path.join(path), file_pattern=pattern)
covid_df_LK = pd.read_csv(covid_path_latest, usecols=dtypes_covid.keys(), dtype=dtypes_covid)

# %% eval fallzahlen new
print(date_latest)
covid_df_LK['Meldedatum'] = pd.to_datetime(covid_df_LK['Meldedatum']).dt.date
meldedatum_max = covid_df_LK['Meldedatum'].max()
covid_df_LK['AnzahlFall_neu'] = np.where(covid_df_LK['NeuerFall'].isin([-1, 1]), covid_df_LK['AnzahlFall'], 0)
covid_df_LK['AnzahlFall'] = np.where(covid_df_LK['NeuerFall'].isin([0, 1]), covid_df_LK['AnzahlFall'], 0)
covid_df_LK['AnzahlFall_7d'] = np.where(covid_df_LK['Meldedatum'] > (meldedatum_max - timedelta(days=7)),
                                     covid_df_LK['AnzahlFall'], 0)
covid_df_LK['AnzahlTodesfall_neu'] = np.where(covid_df_LK['NeuerTodesfall'].isin([-1, 1]), covid_df_LK['AnzahlTodesfall'], 0)
covid_df_LK['AnzahlTodesfall'] = np.where(covid_df_LK['NeuerTodesfall'].isin([0, 1]), covid_df_LK['AnzahlTodesfall'], 0)
datenstand = pd.to_datetime(covid_df_LK['Datenstand'].iloc[0], format='%d.%m.%Y, %H:%M Uhr')
covid_df_LK['Datenstand'] = datenstand.date()
covid_df_LK.drop(['NeuerFall', 'NeuerTodesfall'], inplace=True, axis=1)
covid_df_BL = covid_df_LK.copy()
covid_df_BL['IdLandkreis'] = 0
covid_df_GER = covid_df_BL.copy()
covid_df_GER['IdBundesland'] = 0
agg_key = {
    c: 'max' if c in ['Meldedatum', 'Datenstand'] else 'sum'
    for c in covid_df_LK.columns
    if c not in key_list_LK
}
covid_df_LK = covid_df_LK.groupby(key_list_LK, as_index=False).agg(agg_key)
covid_df_BL = covid_df_BL.groupby(key_list_LK, as_index=False).agg(agg_key)
covid_df_GER = covid_df_GER.groupby(key_list_LK, as_index=False).agg(agg_key)
covid_df_BL = pd.concat([covid_df_GER, covid_df_BL])
covid_df_BL.reset_index(inplace=True, drop=True)
covid_df_BL.drop(['IdLandkreis'], inplace=True, axis=1)
covid_df_LK.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
covid_df_BL.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
covid_df_LK['report_date'] = date_latest
covid_df_BL['report_date'] = date_latest
path_date_csv_LK = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                               'FixFallzahlen_' + date_latest.strftime("%Y%m%d") + '_LK.csv')
path_date_csv_BL = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                               'FixFallzahlen_' + date_latest.strftime("%Y%m%d") + '_BL.csv')
covid_df_LK.sort_values(by=key_list_LK, inplace=True)
covid_df_BL.sort_values(by=key_list_BL, inplace=True)
df_mask = (bevoelkerung_df['AGS'].isin(covid_df_LK['IdLandkreis'])) & (bevoelkerung_df['GueltigAb'] <= datenstand) & (bevoelkerung_df['GueltigBis'] >= datenstand)
population_df = bevoelkerung_df[df_mask]
population_df.reset_index(inplace=True, drop=True)
covid_df_LK['population'] = population_df['Einwohner']
covid_df_LK['AnzahlFall_7d'] = covid_df_LK['AnzahlFall_7d'].astype(int)
covid_df_LK['incidence_7d'] = covid_df_LK['AnzahlFall_7d'] / covid_df_LK['population'] * 100000
df_mask = (bevoelkerung_df['AGS'].isin(covid_df_BL['IdBundesland'])) & (bevoelkerung_df['GueltigAb'] <= datenstand) & (bevoelkerung_df['GueltigBis'] >= datenstand)
population_df = bevoelkerung_df[df_mask]
population_df.reset_index(inplace=True, drop=True)
covid_df_BL['population'] = population_df['Einwohner']
covid_df_BL['AnzahlFall_7d'] = covid_df_BL['AnzahlFall_7d'].astype(int)
covid_df_BL['incidence_7d'] = covid_df_BL['AnzahlFall_7d'] / covid_df_BL['population'] * 100000
with open(path_date_csv_LK, 'wb') as csvfile:
    covid_df_LK.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8',
                          date_format='%Y-%m-%d', columns=dtypes_fallzahlen_LK.keys())
with open(path_date_csv_BL, 'wb') as csvfile:
    covid_df_BL.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8',
                          date_format='%Y-%m-%d', columns=dtypes_fallzahlen_BL.keys())
# %% limit files to the last 7 days
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen')

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path)
file_list.sort(reverse=False)
pattern = 'FixFallzahlen'

all_files = []
for file in file_list:
    file_path_full = os.path.join(path, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = date(int(re_search.group(1)), int(re_search.group(3)), int(re_search.group(4)))
            all_files.append((file_path_full, report_date))

day_range = pd.date_range(end=datetime.today(), periods=7).tolist()
day_range_str = []
for datum in day_range:
    day_range_str.append(datum.strftime('%Y%m%d'))
for file_path_full, report_date in all_files:
    if report_date.strftime('%Y%m%d') not in day_range_str:
        os.remove(file_path_full)
