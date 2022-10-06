import os
import re
import requests as r
import datetime as dt
import numpy as np
import pandas as pd
import json

# %%
url = "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"
meta_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'meta')
filename_meta = "meta_new.json"
BV_csv_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'Bevoelkerung',
    'Bevoelkerung.csv')
LK_dtypes = {
    'Datenstand': 'object',
    'IdLandkreis': 'str',
    'Landkreis': 'str',
    'AnzahlFall_7d': 'Int32',
    'incidence_7d': 'float64'}
BL_dtypes = {
    'Datenstand': 'object',
    'IdBundesland': 'str',
    'Bundesland': 'str',
    'AnzahlFall_7d': 'Int32',
    'incidence_7d': 'float64'}
BV_dtypes = {
    'AGS': 'str',
    'Altersgruppe': 'str',
    'Name': 'str',
    'GueltigAb': 'object',
    'GueltigBis': 'object',
    'Einwohner': 'Int32',
    'männlich': 'Int32',
    'weiblich': 'Int32'}
CV_dtypes = {
    'Datenstand': 'object',
    'IdBundesland': 'str',
    'Bundesland': 'str',
    'IdLandkreis': 'str',
    'Landkreis': 'str',
    'Altersgruppe': 'str',
    'Geschlecht': 'str',
    'NeuerFall': 'Int32',
    'NeuerTodesfall': 'Int32',
    'NeuGenesen': 'Int32',
    'AnzahlFall': 'Int32',
    'AnzahlTodesfall': 'Int32',
    'AnzahlGenesen': 'Int32',
    'Meldedatum': 'object'}

# open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV['GueltigAb'] = pd.to_datetime(BV['GueltigAb'])
BV['GueltigBis'] = pd.to_datetime(BV['GueltigBis'])

# load covid latest from web
with open(meta_path + "/" + filename_meta, 'r', encoding ='utf8') as file:
    metaObj = json.load(file)
fileName = metaObj['name']
fileSize = metaObj['size']
fileSizeMb = round(fileSize / 1024 / 1024, 1)
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(
    aktuelleZeit,
    ": loading",
    fileName,
    "(size:",
    fileSize,
    "bytes =",
    fileSizeMb,
    "MegaByte) from RKI server to dataframe ...")
#path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
#testfile = os.path.join(path, 'RKI_COVID19_2022-10-05.csv')
#data_Base = pd.read_csv(testfile, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
data_Base = pd.read_csv(url, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
data_Base['IdBundesland'] = data_Base['IdBundesland'].str.zfill(2)
data_Base['Meldedatum'] = pd.to_datetime(data_Base['Meldedatum']).dt.date
datenstand = pd.to_datetime(data_Base['Datenstand'].iloc[0], format='%d.%m.%Y, %H:%M Uhr')
data_Base['Datenstand'] = datenstand.date()
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": complete.")

# ageGroup Data
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": calculating age-group data ...")
LK = data_Base.copy()

# Altergruppe und Geschlecht wird jetzt nicht mehr gebraucht
data_Base.drop(['Altersgruppe', 'Geschlecht'], inplace=True, axis=1)

# delete datasets with Altergruppe = unbekannt 
LK.drop(LK[LK['Altersgruppe'] == 'unbekannt'].index, axis= 0, inplace= True)
LK.reset_index (inplace=True, drop=True)
# delete datasets with Geschlecht = unbekannt
LK.drop(LK[(LK['Geschlecht'] == 'unbekannt')].index, axis= 0, inplace= True)
LK.reset_index(inplace=True, drop=True)
LK.drop(['Bundesland', 'Landkreis', 'Meldedatum', 'Datenstand'], inplace=True, axis=1)

# used keylists
key_list_LK_age = ['IdLandkreis', 'Altersgruppe']
key_list_BL_age = ['IdBundesland', 'Altersgruppe']
key_list_ID0_age = ['Altersgruppe']

# calculate the age group data
LK['AnzahlFall'] = np.where(LK['NeuerFall'].isin([1, 0]), LK['AnzahlFall'], 0)
LK['AnzahlFallM'] = np.where(LK['Geschlecht'] == "M", LK['AnzahlFall'], 0)
LK['AnzahlFallW'] = np.where(LK['Geschlecht'] == "W", LK['AnzahlFall'], 0)
LK['AnzahlTodesfall'] = np.where(LK['NeuerTodesfall'].isin([1, 0, -9]), LK['AnzahlTodesfall'], 0)
LK['AnzahlTodesfallM'] = np.where(LK['Geschlecht'] == "M", LK['AnzahlTodesfall'], 0)
LK['AnzahlTodesfallW'] = np.where(LK['Geschlecht'] == "W", LK['AnzahlTodesfall'], 0)
LK.drop([
    'NeuGenesen',
    'NeuerFall',
    'NeuerTodesfall',
    'AnzahlFall',
    'AnzahlTodesfall',
    'AnzahlGenesen',
    'Geschlecht'], inplace=True, axis=1)
agg_key = {
    c: 'max' if c in ['IdBundesland'] else 'sum'
    for c in LK.columns
    if c not in key_list_LK_age}
LK = LK.groupby(key_list_LK_age, as_index=False).agg(agg_key)
LK.reset_index(inplace=True, drop=True)
LK_pop_mask = (
    (BV['AGS'].isin(LK['IdLandkreis'])) &
    (BV['Altersgruppe'].isin(LK['Altersgruppe'])) &
    (BV['GueltigAb'] <= datenstand) &
    (BV['GueltigBis'] >= datenstand))
LK_pop = BV[LK_pop_mask]
LK_pop.reset_index(inplace=True, drop=True)
LK['populationM'] = LK_pop['männlich']
LK['populationW'] = LK_pop['weiblich']
LK['AnzahlFallM'] = LK['AnzahlFallM'].astype(int)
LK['AnzahlFallW'] = LK['AnzahlFallW'].astype(int)
LK['AnzahlTodesfallM'] = LK['AnzahlTodesfallM'].astype(int)
LK['AnzahlTodesfallW'] = LK['AnzahlTodesfallW'].astype(int)
LK['cases100kM'] = round(LK['AnzahlFallM'] / LK['populationM'] * 100000, 1)
LK['cases100kW'] = round(LK['AnzahlFallW'] / LK['populationW'] * 100000, 1)
LK['deaths100kM'] = round(LK['AnzahlTodesfallM'] / LK['populationM'] * 100000, 1)
LK['deaths100kW'] = round(LK['AnzahlTodesfallW'] / LK['populationW'] * 100000, 1)
agg_key = {
    c: 'max' if c in ['IdLandkreis']  else 'sum'
    for c in LK.columns
    if c not in key_list_BL_age}
BL = LK.groupby(key_list_BL_age, as_index=False).agg(agg_key).copy()
BL['cases100kM'] = round(BL['AnzahlFallM'] / BL['populationM'] * 100000, 1)
BL['cases100kW'] = round(BL['AnzahlFallW'] / BL['populationW'] * 100000, 1)
BL['deaths100kM'] = round(BL['AnzahlTodesfallM'] / BL['populationM'] * 100000, 1)
BL['deaths100kW'] = round(BL['AnzahlTodesfallW'] / BL['populationW'] * 100000, 1)
BL.drop(['IdLandkreis'],inplace=True, axis=1)
agg_key = {
    c: 'max' if c in ['IdBundesland', 'IdLandkreis']  else 'sum'
    for c in BL.columns
    if c not in key_list_ID0_age}
ID0 = BL.groupby(key_list_ID0_age, as_index=False).agg(agg_key).copy()
ID0['IdBundesland'] ='00'
ID0['cases100kM'] = round(ID0['AnzahlFallM'] / ID0['populationM'] * 100000, 1)
ID0['cases100kW'] = round(ID0['AnzahlFallW'] / ID0['populationW'] * 100000, 1)
ID0['deaths100kM'] = round(ID0['AnzahlTodesfallM'] / ID0['populationM'] * 100000, 1)
ID0['deaths100kW'] = round(ID0['AnzahlTodesfallW'] / ID0['populationW'] * 100000, 1)
LK.drop(['populationM', 'populationW', 'IdBundesland'], inplace=True, axis=1)
BL.drop(['populationM', 'populationW'], inplace=True, axis=1)
ID0.drop(['populationM', 'populationW'], inplace=True, axis=1)
LK.rename(columns={
    'AnzahlFallM': 'casesMale',
    'AnzahlFallW': 'casesFemale',
    'AnzahlTodesfallM': 'deathsMale',
    'AnzahlTodesfallW': 'deathsFemale',
    'cases100kM': 'casesMalePer100k',
    'cases100kW': 'casesFemalePer100k',
    'deaths100kM': 'deathsMalePer100k',
    'deaths100kW': 'deathsFemalePer100k'}, inplace=True)
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)
BL.rename(columns={
    'AnzahlFallM': 'casesMale',
    'AnzahlFallW': 'casesFemale',
    'AnzahlTodesfallM': 'deathsMale',
    'AnzahlTodesfallW': 'deathsFemale',
    'cases100kM': 'casesMalePer100k',
    'cases100kW': 'casesFemalePer100k',
    'deaths100kM': 'deathsMalePer100k',
    'deaths100kW': 'deathsFemalePer100k'}, inplace=True)

# store as gz compresed json
path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'agegroup')
LK_json_path = os.path.join(path, 'districts.json.gz')
BL_json_path = os.path.join(path, 'states.json.gz')
LK.to_json(LK_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
BL.to_json(BL_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": complete.")

# accumulated and new cases, deaths, recovered, casesPerWeek, deathsPerWeek
# add country column
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": calculating new and acumulated data ...")
data_Base.insert(loc=0, column='IdStaat', value='00')

LK = data_Base.copy()

# used keylists
key_list_LK_cases = ['IdStaat', 'IdBundesland', 'IdLandkreis']
key_list_BL_cases = ['IdStaat', 'IdBundesland']
key_list_ID0_cases = ['IdStaat']

# calculate the values
LK['AnzahlFallAccu'] = np.where(LK['NeuerFall'].isin([1, 0]), LK['AnzahlFall'], 0)
LK['AnzahlFallNeu'] = np.where(LK['NeuerFall'].isin([1, -1]), LK['AnzahlFall'], 0)
LK['AnzahlFall7dAccu'] = np.where(LK['Meldedatum'] > (datenstand.date() - dt.timedelta(days=8)), LK['AnzahlFallAccu'], 0)
LK['AnzahlFall7dNeu'] = np.where(LK['Meldedatum'] > (datenstand.date() - dt.timedelta(days=8)), LK['AnzahlFallNeu'], 0)
LK['AnzahlTodesfallAccu'] = np.where(LK['NeuerTodesfall'].isin([1, 0, -9]), LK['AnzahlTodesfall'], 0)
LK['AnzahlTodesfallNeu'] = np.where(LK['NeuerTodesfall'].isin([1, -1]), LK['AnzahlTodesfall'], 0)
LK['AnzahlTodesfall7dAccu'] = np.where(LK['Meldedatum'] > (datenstand.date() - dt.timedelta(days=8)), LK['AnzahlTodesfallAccu'], 0)
LK['AnzahlTodesfall7dNeu'] = np.where(LK['Meldedatum'] > (datenstand.date() - dt.timedelta(days=8)), LK['AnzahlTodesfallNeu'], 0)
LK['AnzahlGenesenAccu'] = np.where(LK['NeuGenesen'].isin([1, 0]), LK['AnzahlGenesen'], 0)
LK['AnzahlGenesenNeu'] = np.where(LK['NeuGenesen'].isin([1, -1]), LK['AnzahlGenesen'], 0)
LK.drop(['NeuGenesen', 'NeuerFall', 'NeuerTodesfall', 'AnzahlFall', 'AnzahlTodesfall', 'AnzahlGenesen'], inplace=True, axis=1)
LK.rename(columns={
    'AnzahlFallAccu': 'accuCases',
    'AnzahlFallNeu': 'newCases',
    'AnzahlFall7dAccu': 'accuCasesPerWeek',
    'AnzahlFall7dNeu': 'newCasesPerWeek',
    'AnzahlTodesfallAccu': 'accuDeaths',
    'AnzahlTodesfallNeu': 'newDeaths',
    'AnzahlTodesfall7dAccu': 'accuDeathsPerWeek',
    'AnzahlTodesfall7dNeu': 'newDeathsPerWeek',
    'AnzahlGenesenAccu': 'accuRecovered',
    'AnzahlGenesenNeu': 'newRecovered' }, inplace=True)
agg_key = {
    c: 'max' if c in ['Meldedatum', 'Datenstand', 'Landkreis', 'Bundesland'] else 'sum'
    for c in LK.columns
    if c not in key_list_LK_cases}
LK = LK.groupby(key_list_LK_cases, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['Meldedatum', 'Datenstand', 'Bundesland', 'IdLandkreis', 'Landkreis'] else 'sum'
    for c in LK.columns
    if c not in key_list_BL_cases}
BL = LK.groupby(key_list_BL_cases, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['Meldedatum', 'Datenstand', 'Bundesland', 'IdLandkreis', 'Landkreis', 'IdBundesland'] else 'sum'
    for c in BL.columns
    if c not in key_list_ID0_cases}
ID0 = BL.groupby(key_list_ID0_cases, as_index=False).agg(agg_key)
LK.drop(['IdStaat', 'IdBundesland'], inplace=True, axis=1)
LK_pop_mask = (
    (BV['AGS'].isin(LK['IdLandkreis'])) &
    (BV['Altersgruppe'] == "A00+") &
    (BV['GueltigAb'] <= datenstand) &
    (BV['GueltigBis'] >= datenstand))
LK_pop = BV[LK_pop_mask]
LK_pop.reset_index(inplace=True, drop=True)
LK['population'] = LK_pop['Einwohner']
BL.drop(['IdStaat', 'IdLandkreis', 'Landkreis'], inplace=True, axis=1)
ID0.drop(['IdStaat', 'IdLandkreis', 'Landkreis'], inplace=True, axis=1)
ID0['IdBundesland'] = '00'
ID0['Bundesland'] = 'Bundesgebiet'
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)
BL_pop_mask = (
    (BV['AGS'].isin(BL['IdBundesland'])) &
    (BV['Altersgruppe'] == "A00+") &
    (BV['GueltigAb'] <= datenstand) &
    (BV['GueltigBis'] >= datenstand))
BL_pop = BV[BL_pop_mask]
BL_pop.reset_index(inplace=True, drop=True)
BL['population'] = BL_pop['Einwohner']

# store as gz compressed json
path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dataStore', 'cases')
LK_json_path = os.path.join(path, 'districts.json.gz')
BL_json_path = os.path.join(path, 'states.json.gz')
LK.to_json(LK_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
BL.to_json(BL_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": complete.")

# History
# DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
# StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": calculating history data ...")
LK = data_Base.copy()

# used keylists
key_list_LK_hist = ['IdStaat', 'IdBundesland', 'IdLandkreis', 'Meldedatum']
key_list_BL_hist = ['IdStaat', 'IdBundesland', 'Meldedatum']
key_list_ID0_hist = ['IdStaat', 'Meldedatum']

LK['AnzahlFall'] = np.where(LK['NeuerFall'].isin([1, 0]), LK['AnzahlFall'], 0)
LK['AnzahlTodesfall'] = np.where(LK['NeuerTodesfall'].isin([1, 0, -9]), LK['AnzahlTodesfall'], 0)
LK['AnzahlGenesen'] = np.where(LK['NeuGenesen'].isin([1, 0, -9]), LK['AnzahlGenesen'], 0)
LK.drop(['NeuerFall','NeuerTodesfall','NeuGenesen'], inplace=True, axis=1)
LK.rename(columns={'AnzahlFall': 'cases', 'AnzahlTodesfall': 'deaths', 'AnzahlGenesen': 'recovered'}, inplace=True)
agg_key = {
    c: 'max' if c in ['Datenstand', 'Landkreis', 'Bundesland'] else 'sum'
    for c in LK.columns
    if c not in key_list_LK_hist}
LK = LK.groupby(key_list_LK_hist, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['IdLandkreis', 'Datenstand', 'Landkreis', 'Bundesland', ] else 'sum'
    for c in LK.columns
    if c not in key_list_BL_hist}
BL = LK.groupby(key_list_BL_hist, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['IdBundesland', 'IdLandkreis', 'Datenstand', 'Bundesland', 'Landkreis'] else 'sum'
    for c in BL.columns
    if c not in key_list_ID0_hist}
ID0 = BL.groupby(key_list_ID0_hist, as_index=False).agg(agg_key)
LK.drop(['IdStaat', 'IdBundesland', 'Bundesland'], inplace=True, axis=1)
BL.drop(['IdStaat', 'IdLandkreis', 'Landkreis'], inplace=True, axis=1)
ID0.drop(['IdStaat', 'IdLandkreis', 'Landkreis'], inplace=True, axis=1)
ID0['IdBundesland'] = '00'
ID0['Bundesland'] = 'Bundesgebiet'
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)

# store gz compressed json
path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'history')
LK_json_path = os.path.join(path, 'districts.json.gz')
BL_json_path = os.path.join(path, 'states.json.gz')
LK.to_json(LK_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
BL.to_json(BL_json_path, orient="records", date_format="iso", force_ascii=False, compression='gzip')
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": complete.")

# fixed-incidence
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": calculating fixed-incidence data ...")
LK = data_Base.copy()

# used keylists
key_list_LK_fix = ['IdStaat', 'IdBundesland', 'IdLandkreis' ]
key_list_BL_fix = ['IdStaat', 'IdBundesland']
key_list_ID0_fix = ['IdStaat']

LK['AnzahlFall'] = np.where(LK['NeuerFall'].isin([0, 1]), LK['AnzahlFall'], 0)
LK['AnzahlFall_7d'] = np.where(LK['Meldedatum'] > (datenstand.date() - dt.timedelta(days=8)), LK['AnzahlFall'], 0)
LK['Datenstand'] = datenstand.date()
LK.drop([
    'Meldedatum',
    'NeuerFall',
    'NeuerTodesfall',
    'AnzahlFall',
    'AnzahlTodesfall',
    'Landkreis',
    'Bundesland',
    'NeuGenesen',
    'AnzahlGenesen'], inplace=True, axis=1)
agg_key = {
    c: 'max' if c in ['Datenstand'] else 'sum'
    for c in LK.columns
    if c not in key_list_LK_fix}
LK = LK.groupby(key_list_LK_fix, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['IdLandkreis', 'Datenstand'] else 'sum'
    for c in LK.columns
    if c not in key_list_BL_fix}
BL = LK.groupby(key_list_BL_fix, as_index=False).agg(agg_key)
agg_key = {
    c: 'max' if c in ['IdBundesland', 'IdLandkreis', 'Datenstand'] else 'sum'
    for c in BL.columns
    if c not in key_list_ID0_fix}
ID0 = BL.groupby(key_list_ID0_fix, as_index=False).agg(agg_key)
LK.drop(['IdStaat', 'IdBundesland'], inplace=True, axis=1)
BL.drop(['IdStaat', 'IdLandkreis'], inplace=True, axis=1)
ID0.drop(['IdStaat', 'IdLandkreis'], inplace=True, axis=1)
ID0['IdBundesland'] = '00'
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)
LK_pop_mask = (
    (BV['AGS'].isin(LK['IdLandkreis'])) &
    (BV['Altersgruppe'] == "A00+") &
    (BV['GueltigAb'] <= datenstand) &
    (BV['GueltigBis'] >= datenstand))
LK_pop = BV[LK_pop_mask]
LK_pop.reset_index(inplace=True, drop=True)
LK['population'] = LK_pop['Einwohner']
LK.insert(loc=0, column='Landkreis', value=LK_pop['Name'])
LK['AnzahlFall_7d'] = LK['AnzahlFall_7d'].astype(int)
LK['incidence_7d'] = LK['AnzahlFall_7d'] / LK['population'] * 100000
LK.drop(['population'], inplace=True, axis=1)
BL_pop_mask = (
    (BV['AGS'].isin(BL['IdBundesland'])) &
    (BV['Altersgruppe'] == "A00+") &
    (BV['GueltigAb'] <= datenstand) &
    (BV['GueltigBis'] >= datenstand))
BL_pop = BV[BL_pop_mask]
BL_pop.reset_index(inplace=True, drop=True)
BL['population'] = BL_pop['Einwohner']
BL.insert(loc=0, column='Bundesland', value=BL_pop['Name'])
BL['AnzahlFall_7d'] = BL['AnzahlFall_7d'].astype(int)
BL['incidence_7d'] = BL['AnzahlFall_7d'] / BL['population'] * 100000
BL.drop(['population'], inplace=True, axis=1)

# store csv files, i need this csv files for personal reasons! they are not nessasary for the api!
path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'frozen-incidence')
path_csv = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'frozen-incidence',
    'csv')
LK_csv_path = os.path.join(path_csv, 'frozen-incidence_' + datenstand.date().strftime('%Y-%m-%d') + '_LK.csv')
BL_csv_path = os.path.join(path_csv, 'frozen-incidence_' + datenstand.date().strftime('%Y-%m-%d') + '_BL.csv')
with open(LK_csv_path, 'wb') as csvfile:
    LK.to_csv(
        csvfile,
        index=False,
        header=True,
        lineterminator='\n',
        encoding='utf-8',
        date_format='%Y-%m-%d',
        columns=LK_dtypes.keys())
with open(BL_csv_path, 'wb') as csvfile:
    BL.to_csv(
        csvfile,
        index=False,
        header=True,
        lineterminator='\n',
        encoding='utf-8',
        date_format='%Y-%m-%d',
        columns=BL_dtypes.keys())

# limit frozen-incidence csv files to the last 30 days
iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path_csv)
file_list.sort(reverse=False)
pattern = 'frozen-incidence'
all_files = []
for file in file_list:
    file_path_full = os.path.join(path_csv, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = dt.date(
                int(re_search.group(1)),
                int(re_search.group(3)),
                int(re_search.group(4))).strftime('%Y-%m-%d')
            all_files.append((file_path_full, report_date))
today = dt.date.today()
day_range = pd.date_range(end=today, periods=30).tolist()
day_range_str = []
for datum in day_range:
    day_range_str.append(datum.strftime('%Y-%m-%d'))
for file_path_full, report_date in all_files:
    if report_date not in day_range_str:
        os.remove(file_path_full)

# store compressed json files
LK.set_index(['IdLandkreis'], inplace=True, drop=True)
BL.set_index(['IdBundesland'], inplace=True, drop=True)
LK_json_path = os.path.join(path, 'frozen-incidence_' + datenstand.date().strftime('%Y-%m-%d') + '_LK.json.gz')
BL_json_path = os.path.join(path, 'frozen-incidence_' + datenstand.date().strftime('%Y-%m-%d') + '_BL.json.gz')
LK.to_json(LK_json_path, orient="index", date_format="iso", force_ascii=False, compression='gzip')
BL.to_json(BL_json_path, orient="index", date_format="iso", force_ascii=False, compression='gzip')

# limit frozen-incidence json files to from last modified Excel Date to today
iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(path)
file_list.sort(reverse=False)
pattern = 'frozen-incidence'
all_files = []
for file in file_list:
    file_path_full = os.path.join(path, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = dt.date(
                int(re_search.group(1)),
                int(re_search.group(3)),
                int(re_search.group(4))).strftime('%Y-%m-%d')
            all_files.append((file_path_full, report_date))
url = "https://www.rki.de/DE/Content/InfAZ/N/Neuartiges_Coronavirus/Daten/Fallzahlen_Kum_Tab_aktuell.xlsx?__blob=publicationFile"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0",
    "Accept-Encoding": "*",
    "Connection": "keep-alive"}
excelResonse = r.get(url, headers=headers)
lastModifiedStr = excelResonse.headers["last-modified"]
lastModified = pd.to_datetime(lastModifiedStr, format='%a, %d %b %Y %H:%M:%S %Z').date()
day_range = pd.date_range(start=lastModified, end=today).tolist()
day_range_str = []
for datum in day_range:
    day_range_str.append(datum.strftime('%Y-%m-%d'))
for file_path_full, report_date in all_files:
    if report_date not in day_range_str:
        os.remove(file_path_full)
aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": complete.")