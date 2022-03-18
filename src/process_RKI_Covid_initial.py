import os
import re
from datetime import date, timedelta

import numpy as np
import pandas as pd

# %%
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..')
path_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'Fallzahlen',
                        'RKI_COVID19_Fallzahlen_temp.csv')

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path)
file_list.sort(reverse=False)
pattern = 'RKI_COVID19'
dfs = []
key_list = ['IdBundesland', 'IdLandkreis']
dtypes_new = {'IdBundesland': 'Int32', 'IdLandkreis': 'Int32', 'NeuerFall': 'Int8',
              'NeuerTodesfall': 'Int8', 'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'Meldedatum': 'object',
              'Datenstand': 'object'}
dtypes_new_2 = {'IdBundesland': 'Int32', 'IdLandkreis': 'Int32', 'NeuerFall': 'Int8',
                'NeuerTodesfall': 'Int8', 'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'Meldedatum2': 'object',
                'Datenstand': 'object'}
dtypes_old = {'IdBundesland': 'Int32', 'Landkreis ID': 'Int32', 'Neuer Fall': 'Int8',
              'Neuer Todesfall': 'Int8', 'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'Meldedatum': 'object',
              'Datenstand': 'object'}
dtypes_fallzahlen = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'IdLandkreis': 'Int32',
                     'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32',
                     'AnzahlTodesfall_neu': 'Int32', 'AnzahlFall_7d': 'Int32', 'report_date': 'object',
                     'meldedatum_max': 'object'}
# %%
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

count = 0
for file_path_full, report_date in all_files:
    if report_date >= date(2020, 3, 24):
        count += 1
        print(report_date)
        if report_date == date(2020, 3, 25):
            # Sonderfall, falscher Datentyp in Zeile
            df = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[10572])
        elif report_date == date(2020, 3, 26):
            # Sonderfall, falscher Datentyp in Zeile
            df = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[1001])
        else:
            try:
                df = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new)
            except UnicodeDecodeError:
                try:
                    df = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new,
                                     encoding='cp1252')
                except ValueError:
                    df = pd.read_csv(file_path_full, usecols=dtypes_new_2.keys(), dtype=dtypes_new_2,
                                     encoding='cp1252')
            except ValueError:
                df = pd.read_csv(file_path_full, usecols=dtypes_old.keys(), dtype=dtypes_old)

            df.rename(columns={'Neuer Fall': 'NeuerFall', 'Neuer Todesfall': 'NeuerTodesfall',
                               'Landkreis ID': 'IdLandkreis', 'Meldedatum2': 'Meldedatum'}, inplace=True)
        try:
            df['Meldedatum'] = pd.to_datetime(df['Meldedatum']).dt.date
        except:
            df['Meldedatum'] = pd.to_datetime(df['Meldedatum'], unit='ms').dt.date
        try:
            datenstand = pd.to_datetime(df['Datenstand'].iloc[0])
        except:
            datenstand = pd.to_datetime(df['Datenstand'].iloc[0], format='%d.%m.%Y, %H:%M Uhr')
        df['AnzahlFall_neu'] = np.where(df['NeuerFall'].isin([-1, 1]), df['AnzahlFall'], 0)
        df['AnzahlFall'] = np.where(df['NeuerFall'].isin([0, 1]), df['AnzahlFall'], 0)
        df['AnzahlFall_7d'] = np.where(df['Meldedatum'] > (datenstand - timedelta(days=8)),
                                       df['AnzahlFall'], 0)
        df['AnzahlTodesfall_neu'] = np.where(df['NeuerTodesfall'].isin([-1, 1]), df['AnzahlTodesfall'], 0)
        df['AnzahlTodesfall'] = np.where(df['NeuerTodesfall'].isin([0, 1]), df['AnzahlTodesfall'], 0)
        df.drop(['NeuerFall', 'NeuerTodesfall', 'Datenstand'], inplace=True, axis=1)
        agg_key = {}
        for c in df.columns:
            if c not in key_list:
                if c in ['Meldedatum']:
                    agg_key[c] = 'max'
                else:
                    agg_key[c] = 'sum'
        df = df.groupby(key_list, as_index=False).agg(agg_key)
        df['report_date'] = report_date
        df.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
        df['Datenstand'] = datenstand
        dfs.append(df)
        # if count>30: break

# %%
covid_df = pd.concat(dfs)
covid_df.sort_values(by=['report_date', 'IdLandkreis'], inplace=True)

# %% dedup and write csv
covid_df_clean = covid_df.copy()
# covid_df.drop_duplicates(subset=['IdLandkreis','meldedatum_max'], keep='last',inplace=True)
with open(path_csv, 'wb') as csvfile:
    covid_df.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8', date_format='%Y-%m-%d',
                    columns=dtypes_fallzahlen.keys())
