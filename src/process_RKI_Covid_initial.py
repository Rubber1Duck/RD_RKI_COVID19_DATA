import os
import re
from datetime import *
from time import *
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

# %%
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
path_bevoelkerung_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Bevoelkerung',
                               'Bevoelkerung.csv')

iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path)
file_list.sort(reverse=False)
pattern = 'RKI_COVID19'
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
dtypes_bevoelkerung = {'AGS': 'Int32', 'Name': 'object', 'GueltigAb': 'object', 'GueltigBis': 'object', 'Einwohner': 'Int32'}
dtypes_fallzahlen_LK = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'IdLandkreis': 'Int32',
                     'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32',
                     'AnzahlTodesfall_neu': 'Int32', 'AnzahlFall_7d': 'Int32', 'report_date': 'object',
                     'meldedatum_max': 'object', 'population': 'Int32', 'incidence_7d': 'float64'}
dtypes_fallzahlen_BL = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'AnzahlFall': 'Int32',
                     'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32', 'AnzahlTodesfall_neu': 'Int32',
                     'AnzahlFall_7d': 'Int32', 'report_date': 'object', 'meldedatum_max': 'object',
                     'population': 'Int32', 'incidence_7d': 'float64'}

# %% open bevoelkerung.csv
bevoelkerung_df = pd.read_csv(path_bevoelkerung_csv, usecols=dtypes_bevoelkerung.keys(), dtype=dtypes_bevoelkerung)
bevoelkerung_df['GueltigAb'] = pd.to_datetime(bevoelkerung_df['GueltigAb'])
bevoelkerung_df['GueltigBis'] = pd.to_datetime(bevoelkerung_df['GueltigBis'])

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

for file_path_full, report_date in all_files:
    if report_date >= date(2020, 3, 24):
        t1 = process_time()
        print(report_date)
        path_date_csv_LK = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                               'FixFallzahlen_' + report_date.strftime("%Y%m%d") + '_LK.csv')
        path_date_csv_BL = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                               'FixFallzahlen_' + report_date.strftime("%Y%m%d") + '_BL.csv')
        if report_date == date(2020, 3, 25):
            # Sonderfall, falscher Datentyp in Zeile
            df_LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[10572])
        elif report_date == date(2020, 3, 26):
            # Sonderfall, falscher Datentyp in Zeile
            df_LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[1001])
        else:
            try:
                df_LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new)
            except UnicodeDecodeError:
                try:
                    df_LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new,
                                     encoding='cp1252')
                except ValueError:
                    df_LK = pd.read_csv(file_path_full, usecols=dtypes_new_2.keys(), dtype=dtypes_new_2,
                                     encoding='cp1252')
            except ValueError:
                df_LK = pd.read_csv(file_path_full, usecols=dtypes_old.keys(), dtype=dtypes_old)

            df_LK.rename(columns={'Neuer Fall': 'NeuerFall', 'Neuer Todesfall': 'NeuerTodesfall',
                               'Landkreis ID': 'IdLandkreis', 'Meldedatum2': 'Meldedatum'}, inplace=True)
        try:
            df_LK['Meldedatum'] = pd.to_datetime(df_LK['Meldedatum']).dt.date
        except:
            df_LK['Meldedatum'] = pd.to_datetime(df_LK['Meldedatum'], unit='ms').dt.date
        try:
            datenstand = pd.to_datetime(df_LK['Datenstand'].iloc[0])
        except:
            datenstand = pd.to_datetime(df_LK['Datenstand'].iloc[0], format='%d.%m.%Y, %H:%M Uhr')
        df_LK['AnzahlFall_neu'] = np.where(df_LK['NeuerFall'].isin([-1, 1]), df_LK['AnzahlFall'], 0)
        df_LK['AnzahlFall'] = np.where(df_LK['NeuerFall'].isin([0, 1]), df_LK['AnzahlFall'], 0)
        df_LK['AnzahlFall_7d'] = np.where(df_LK['Meldedatum'] > (datenstand - timedelta(days=8)),
                                       df_LK['AnzahlFall'], 0)
        df_LK['AnzahlTodesfall_neu'] = np.where(df_LK['NeuerTodesfall'].isin([-1, 1]), df_LK['AnzahlTodesfall'], 0)
        df_LK['AnzahlTodesfall'] = np.where(df_LK['NeuerTodesfall'].isin([0, 1]), df_LK['AnzahlTodesfall'], 0)
        df_LK.drop(['NeuerFall', 'NeuerTodesfall', 'Datenstand'], inplace=True, axis=1)
        df_BL = df_LK.copy()
        df_BL['IdLandkreis'] = 0
        df_GER = df_BL.copy()
        df_GER['IdBundesland'] = 0
        agg_key = {}
        for c in df_LK.columns:
            if c not in key_list:
                if c in ['Meldedatum']:
                    agg_key[c] = 'max'
                else:
                    agg_key[c] = 'sum'
        df_LK = df_LK.groupby(key_list, as_index=False).agg(agg_key)
        df_BL = df_BL.groupby(key_list, as_index=False).agg(agg_key)
        df_GER = df_GER.groupby(key_list, as_index=False).agg(agg_key)
        df_BL = pd.concat([df_GER, df_BL])
        df_BL.reset_index(inplace=True, drop=True)
        df_BL.drop(['IdLandkreis'], inplace=True, axis=1)
        df_LK['report_date'] = report_date
        df_BL['report_date'] = report_date
        df_LK.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
        df_BL.rename(columns={'Meldedatum': 'meldedatum_max'}, inplace=True)
        df_LK['Datenstand'] = datenstand
        df_BL['Datenstand'] = datenstand
        df_LK.sort_values(by=['report_date', 'IdBundesland', 'IdLandkreis'], inplace=True)
        df_BL.sort_values(by=['report_date', 'IdBundesland'], inplace=True)
        df_mask = (bevoelkerung_df['AGS'].isin(df_LK['IdLandkreis'])) & (bevoelkerung_df['GueltigAb'] <= datenstand) & (bevoelkerung_df['GueltigBis'] >= datenstand)
        population_df = bevoelkerung_df[df_mask]
        population_df.reset_index(inplace=True, drop=True)
        df_LK['population'] = population_df['Einwohner']
        df_LK['AnzahlFall_7d'] = df_LK['AnzahlFall_7d'].astype(int)
        df_LK['incidence_7d'] = df_LK['AnzahlFall_7d'] / df_LK['population'] * 100000
        df_mask = (bevoelkerung_df['AGS'].isin(df_BL['IdBundesland'])) & (bevoelkerung_df['GueltigAb'] <= datenstand) & (bevoelkerung_df['GueltigBis'] >= datenstand)
        population_df = bevoelkerung_df[df_mask]
        population_df.reset_index(inplace=True, drop=True)
        df_BL['population'] = population_df['Einwohner']
        df_BL['AnzahlFall_7d'] = df_BL['AnzahlFall_7d'].astype(int)
        df_BL['incidence_7d'] = df_BL['AnzahlFall_7d'] / df_BL['population'] * 100000
        with open(path_date_csv_LK, 'wb') as csvfile:
            df_LK.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8', date_format='%Y-%m-%d',
                    columns=dtypes_fallzahlen_LK.keys())
        with open(path_date_csv_BL, 'wb') as csvfile:
            df_BL.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8', date_format='%Y-%m-%d',
                    columns=dtypes_fallzahlen_BL.keys())
        t2 = process_time()
        t = t2 - t1
        print('Rechenzeit: ', t)
