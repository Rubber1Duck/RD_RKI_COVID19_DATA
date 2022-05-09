import os
import re
from datetime import *
from time import *
import numpy as np
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd

# %%
Tges1 = process_time()
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
BV_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Bevoelkerung',
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
BV_dtypes = {'AGS': 'Int32', 'Name': 'object', 'GueltigAb': 'object', 'GueltigBis': 'object', 'Einwohner': 'Int32'}
LK_dtypes = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'IdLandkreis': 'Int32', 'Landkreis': 'object',
                     'AnzahlFall': 'Int32', 'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32',
                     'AnzahlTodesfall_neu': 'Int32', 'AnzahlFall_7d': 'Int32', 'incidence_7d': 'float64'}
BL_dtypes = {'Datenstand': 'object', 'IdBundesland': 'Int32', 'Bundesland': 'object', 'AnzahlFall': 'Int32',
                     'AnzahlTodesfall': 'Int32', 'AnzahlFall_neu': 'Int32', 'AnzahlTodesfall_neu': 'Int32',
                     'AnzahlFall_7d': 'Int32', 'incidence_7d': 'float64'}

# %% open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV['GueltigAb'] = pd.to_datetime(BV['GueltigAb'])
BV['GueltigBis'] = pd.to_datetime(BV['GueltigBis'])

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
        print(report_date)
        if report_date == date(2020, 3, 25):
            # Sonderfall, falscher Datentyp in Zeile
            LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[10572])
        elif report_date == date(2020, 3, 26):
            # Sonderfall, falscher Datentyp in Zeile
            LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new, skiprows=[1001])
        else:
            try:
                LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new)
            except UnicodeDecodeError:
                try:
                    LK = pd.read_csv(file_path_full, usecols=dtypes_new.keys(), dtype=dtypes_new,
                                     encoding='cp1252')
                except ValueError:
                    LK = pd.read_csv(file_path_full, usecols=dtypes_new_2.keys(), dtype=dtypes_new_2,
                                     encoding='cp1252')
            except ValueError:
                LK = pd.read_csv(file_path_full, usecols=dtypes_old.keys(), dtype=dtypes_old)

            LK.rename(columns={'Neuer Fall': 'NeuerFall', 'Neuer Todesfall': 'NeuerTodesfall',
                               'Landkreis ID': 'IdLandkreis', 'Meldedatum2': 'Meldedatum'}, inplace=True)
        try:
            LK['Meldedatum'] = pd.to_datetime(LK['Meldedatum']).dt.date
        except:
            LK['Meldedatum'] = pd.to_datetime(LK['Meldedatum'], unit='ms').dt.date
        try:
            datenstand = pd.to_datetime(LK['Datenstand'].iloc[0])
        except:
            datenstand = pd.to_datetime(LK['Datenstand'].iloc[0], format='%d.%m.%Y, %H:%M Uhr')
        meldedatum_max = LK['Meldedatum'].max()
        LK['AnzahlFall_neu'] = np.where(LK['NeuerFall'].isin([-1, 1]), LK['AnzahlFall'], 0)
        LK['AnzahlFall'] = np.where(LK['NeuerFall'].isin([0, 1]), LK['AnzahlFall'], 0)
        LK['AnzahlFall_7d'] = np.where(LK['Meldedatum'] > (meldedatum_max - timedelta(days=7)),
                                       LK['AnzahlFall'], 0)
        LK['AnzahlTodesfall_neu'] = np.where(LK['NeuerTodesfall'].isin([-1, 1]), LK['AnzahlTodesfall'], 0)
        LK['AnzahlTodesfall'] = np.where(LK['NeuerTodesfall'].isin([0, 1]), LK['AnzahlTodesfall'], 0)
        LK.drop(['NeuerFall', 'NeuerTodesfall', 'Datenstand'], inplace=True, axis=1)
        BL = LK.copy()
        BL['IdLandkreis'] = 0
        ID0 = BL.copy()
        ID0['IdBundesland'] = 0
        agg_key = {}
        for c in LK.columns:
            if c not in key_list:
                if c in ['Meldedatum']:
                    agg_key[c] = 'max'
                else:
                    agg_key[c] = 'sum'
        LK = LK.groupby(key_list, as_index=False).agg(agg_key)
        BL = BL.groupby(key_list, as_index=False).agg(agg_key)
        ID0 = ID0.groupby(key_list, as_index=False).agg(agg_key)
        BL = pd.concat([ID0, BL])
        BL.reset_index(inplace=True, drop=True)
        BL.drop(['IdLandkreis', 'Meldedatum'], inplace=True, axis=1)
        LK.drop(['Meldedatum'], inplace=True, axis=1)
        LK['Datenstand'] = datenstand
        BL['Datenstand'] = datenstand
        LK.sort_values(by=['Datenstand', 'IdBundesland', 'IdLandkreis'], inplace=True)
        BL.sort_values(by=['Datenstand', 'IdBundesland'], inplace=True)
        LK_pop_mask = (BV['AGS'].isin(LK['IdLandkreis'])) & (BV['GueltigAb'] <= datenstand) & (BV['GueltigBis'] >= datenstand)
        LK_pop = BV[LK_pop_mask]
        LK_pop.reset_index(inplace=True, drop=True)
        LK.insert(loc=3, column='Landkreis', value=LK_pop['Name'])
        LK['population'] = LK_pop['Einwohner']
        LK['AnzahlFall_7d'] = LK['AnzahlFall_7d'].astype(int)
        LK['incidence_7d'] = LK['AnzahlFall_7d'] / LK['population'] * 100000
        LK.drop(['population'], inplace=True, axis=1)
        BL_pop_mask = (BV['AGS'].isin(BL['IdBundesland'])) & (BV['GueltigAb'] <= datenstand) & (BV['GueltigBis'] >= datenstand)
        BL_pop = BV[BL_pop_mask]
        BL_pop.reset_index(inplace=True, drop=True)
        BL.insert(loc=2, column='Bundesland', value=BL_pop['Name'])
        BL['population'] = BL_pop['Einwohner']
        BL['AnzahlFall_7d'] = BL['AnzahlFall_7d'].astype(int)
        BL['incidence_7d'] = BL['AnzahlFall_7d'] / BL['population'] * 100000
        BL.drop(['population'], inplace=True, axis=1)
        LK_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                                'FixFallzahlen_' + report_date.strftime("%Y-%m-%d") + '_LK.csv')
        LK_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                                'FixFallzahlen_' + report_date.strftime("%Y-%m-%d") + '_LK.json')
        BL_csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                                'FixFallzahlen_' + report_date.strftime("%Y-%m-%d") + '_BL.csv')
        BL_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'Fallzahlen',
                                'FixFallzahlen_' + report_date.strftime("%Y-%m-%d") + '_BL.json')
        with open(LK_csv_path, 'wb') as csvfile:
            LK.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8', date_format='%Y-%m-%d',
                    columns=LK_dtypes.keys())
        with open(BL_csv_path, 'wb') as csvfile:
            BL.to_csv(csvfile, index=False, header=True, line_terminator='\n', encoding='utf-8', date_format='%Y-%m-%d',
                    columns=BL_dtypes.keys())
        LK['IdLandkreisStr'] = LK['IdLandkreis']
        LK['IdLandkreisStr'] = LK['IdLandkreisStr'].astype(str)
        LK['IdLandkreisStr'] = LK['IdLandkreisStr'].str.zfill(5)
        LK.set_index(['IdLandkreisStr'], inplace=True, drop=True)
        LK.to_json(LK_json_path, orient="index", date_format="iso", force_ascii=False)
        BL['IdBundeslandStr'] = BL['IdBundesland']
        BL.set_index(['IdBundeslandStr'], inplace=True, drop=True)
        BL.to_json(BL_json_path, orient="index", date_format="iso", force_ascii=False)
