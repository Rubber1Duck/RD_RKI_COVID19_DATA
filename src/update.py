import os
import re
import datetime as dt
import numpy as np
import pandas as pd
import json
import utils as ut
import gc

startTime = dt.datetime.now()
base_path = os.path.dirname(os.path.abspath(__file__))
meta_path = os.path.join(base_path, "..", "dataStore", "meta")
filename_meta = "meta_new.json"
feather_path = os.path.join(base_path, "..", "dataBase.feather")
BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
LK_dtypes = {
    "Datenstand": "object",
    "IdLandkreis": "str",
    "Landkreis": "str",
    "AnzahlFall_7d": "Int32",
    "incidence_7d": "float64",
}
BL_dtypes = {
    "Datenstand": "object",
    "IdBundesland": "str",
    "Bundesland": "str",
    "AnzahlFall_7d": "Int32",
    "incidence_7d": "float64",
}
BV_dtypes = {
    "AGS": "str",
    "Altersgruppe": "str",
    "Name": "str",
    "GueltigAb": "object",
    "GueltigBis": "object",
    "Einwohner": "Int32",
    "männlich": "Int32",
    "weiblich": "Int32",
}
CV_dtypes = {
    "IdLandkreis": "str",
    "Altersgruppe": "str",
    "Geschlecht": "str",
    "NeuerFall": "Int32",
    "NeuerTodesfall": "Int32",
    "NeuGenesen": "Int32",
    "AnzahlFall": "Int32",
    "AnzahlTodesfall": "Int32",
    "AnzahlGenesen": "Int32",
    "Meldedatum": "object",
}

# open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

# ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
BV = ut.squeeze_dataframe(BV)

# load covid latest from web
with open(meta_path + "/" + filename_meta, "r", encoding="utf8") as file:
    metaObj = json.load(file)
fileNameOrig = metaObj["filename"]
fileSize = int(metaObj["size"])
url = metaObj["url"]
timeStamp = metaObj["modified"]
Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
filedate = (
    dt.datetime.fromtimestamp(metaObj["modified"] / 1000).date().strftime("%Y-%m-%d")
)
fileSizeMb = round(fileSize / 1024 / 1024, 1)
fileNameRoot = "RKI_COVID19_"
fileName = fileNameRoot + filedate + ".csv"
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(
    aktuelleZeit,
    ": loading",
    fileNameOrig,
    "(size:",
    fileSize,
    "bytes =",
    fileSizeMb,
    "MegaByte) from RKI github to dataframe ...",
)

dataBase = pd.read_csv(url, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
dataBase.sort_values(
    by=["IdLandkreis", "Altersgruppe", "Geschlecht", "Meldedatum"],
    axis=0,
    inplace=True,
    ignore_index=True,
)
dataBase.reset_index(drop=True, inplace=True)

# ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
dataBase = ut.squeeze_dataframe(dataBase)

aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

data_path = os.path.join(base_path, "..", "data")
fileNameXz = fileName + ".xz"
full_path = os.path.join(base_path, data_path, fileName)
full_pathXz = os.path.join(base_path, data_path, fileNameXz)
data_path = os.path.normpath(data_path)
full_path = os.path.normpath(full_path)
istDatei = os.path.isfile(full_path)
istDateiXz = os.path.isfile(full_pathXz)
if not (istDatei | istDateiXz):
    print(aktuelleZeit, ": writing DataFrame to", fileName, "...")
    with open(full_path, "wb") as csvfile:
        dataBase.to_csv(
            csvfile,
            index=False,
            header=True,
            lineterminator="\n",
            encoding="utf-8",
            date_format="%Y-%m-%d",
            columns=CV_dtypes.keys(),
        )
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ": done.")
else:
    if istDatei:
        fileExists = fileName
    else:
        fileExists = fileNameXz
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(aktuelleZeit, ":", fileExists, "already exists.")

# limit RKI_COVID19 Data files to the last 30 days
print(aktuelleZeit, ": cleanup data files ...")
iso_date_re = "([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])"
file_list = os.listdir(data_path)
file_list.sort(reverse=False)
pattern = "RKI_COVID19"
all_files = []
for file in file_list:
    file_path_full = os.path.join(data_path, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = dt.date(
                int(re_search.group(1)),
                int(re_search.group(3)),
                int(re_search.group(4)),
            ).strftime("%Y-%m-%d")
            all_files.append((file_path_full, report_date))
today = dt.date.today()
day_range = pd.date_range(end=today, periods=30).tolist()
day_range_str = []
for datum in day_range:
    day_range_str.append(datum.strftime("%Y-%m-%d"))
for file_path_full, report_date in all_files:
    if report_date not in day_range_str:
        os.remove(file_path_full)
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

print(aktuelleZeit, ": add missing columns ...")

dataBase["IdLandkreis"] = dataBase["IdLandkreis"].astype(str)
dataBase["IdLandkreis"] = dataBase["IdLandkreis"].str.zfill(5)
dataBase.insert(
    loc=0, column="IdBundesland", value=dataBase["IdLandkreis"].str[:-3].copy()
)
dataBase["Meldedatum"] = pd.to_datetime(dataBase["Meldedatum"]).dt.date
dataBase.insert(loc=0, column="Datenstand", value=Datenstand.date())

# add Bundesland und Landkreis
dataBase.insert(loc=2, column="Bundesland", value="")
dataBase.insert(loc=4, column="Landkreis", value="")
BV_mask = (
    (BV["AGS"].isin(dataBase["IdBundesland"]))
    & (BV["Altersgruppe"] == "A00+")
    & (BV["GueltigAb"] <= Datenstand)
    & (BV["GueltigBis"] >= Datenstand)
)
BV_masked = BV[BV_mask].copy()
BV_masked.drop(
    ["GueltigAb", "GueltigBis", "Altersgruppe", "Einwohner", "männlich", "weiblich"],
    inplace=True,
    axis=1,
)
ID = dataBase["IdBundesland"].copy()
ID = pd.merge(
    left=ID, right=BV_masked, left_on="IdBundesland", right_on="AGS", how="left"
)
dataBase["Bundesland"] = ID["Name"].copy()
BV_mask = (
    (BV["AGS"].isin(dataBase["IdLandkreis"]))
    & (BV["Altersgruppe"] == "A00+")
    & (BV["GueltigAb"] <= Datenstand)
    & (BV["GueltigBis"] >= Datenstand)
)
BV_masked = BV[BV_mask].copy()
BV_masked.drop(
    ["GueltigAb", "GueltigBis", "Altersgruppe", "Einwohner", "männlich", "weiblich"],
    inplace=True,
    axis=1,
)
ID = dataBase["IdLandkreis"].copy()
ID = pd.merge(
    left=ID, right=BV_masked, left_on="IdLandkreis", right_on="AGS", how="left"
)
dataBase["Landkreis"] = ID["Name"].copy()
ID = pd.DataFrame()
del ID
gc.collect()
dataBase.insert(loc=0, column="IdStaat", value="00")

# ----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
dataBase = ut.squeeze_dataframe(dataBase)

# store dataBase to feather file to save memory
ut.write_file(df=dataBase, fn=feather_path, compression="lz4")
dataBase = pd.DataFrame()
del dataBase
gc.collect()
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

# ageGroup Data
print(aktuelleZeit, ": calculating age-group data ...")
LK = ut.read_file(fn=feather_path)

# kopiere dataBase ohne unbekannte Altersgruppen oder unbekannte Geschlechter
LK = LK[LK["Altersgruppe"] != "unbekannt"].copy()
LK = LK[LK["Geschlecht"] != "unbekannt"].copy()

# korrigiere Kategorien Altersgruppe und Geschlecht
LK["Geschlecht"] = LK["Geschlecht"].cat.remove_unused_categories()
LK["Altersgruppe"] = LK["Altersgruppe"].cat.remove_unused_categories()
LK.reset_index(inplace=True, drop=True)

# lösche alle nicht benötigten Spalten
LK.drop(
    ["Bundesland", "Landkreis", "Meldedatum", "Datenstand", "IdStaat"],
    inplace=True,
    axis=1,
)

# used keylists
key_list_LK_age = ["IdLandkreis", "Altersgruppe"]
key_list_BL_age = ["IdBundesland", "Altersgruppe"]
key_list_ID0_age = ["Altersgruppe"]

# calculate the age group data
LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(
    int
)
LK["AnzahlFallM"] = np.where(LK["Geschlecht"] == "M", LK["AnzahlFall"], 0).astype(int)
LK["AnzahlFallW"] = np.where(LK["Geschlecht"] == "W", LK["AnzahlFall"], 0).astype(int)
LK["AnzahlTodesfall"] = np.where(
    LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
).astype(int)
LK["AnzahlTodesfallM"] = np.where(
    LK["Geschlecht"] == "M", LK["AnzahlTodesfall"], 0
).astype(int)
LK["AnzahlTodesfallW"] = np.where(
    LK["Geschlecht"] == "W", LK["AnzahlTodesfall"], 0
).astype(int)
LK.drop(
    [
        "NeuGenesen",
        "NeuerFall",
        "NeuerTodesfall",
        "AnzahlFall",
        "AnzahlTodesfall",
        "AnzahlGenesen",
        "Geschlecht",
    ],
    inplace=True,
    axis=1,
)
agg_key = {
    c: "max" if c in ["IdBundesland"] else "sum"
    for c in LK.columns
    if c not in key_list_LK_age
}
LK = LK.groupby(by=key_list_LK_age, as_index=False).agg(
    agg_key,
)
LK.reset_index(inplace=True, drop=True)
LK_pop_mask = (
    (BV["AGS"].isin(LK["IdLandkreis"]))
    & (BV["Altersgruppe"].isin(LK["Altersgruppe"]))
    & (BV["GueltigAb"] <= Datenstand)
    & (BV["GueltigBis"] >= Datenstand)
)
LK_pop = BV[LK_pop_mask]
LK_pop.reset_index(inplace=True, drop=True)
LK["populationM"] = LK_pop["männlich"].astype(int)
LK["populationW"] = LK_pop["weiblich"].astype(int)
LK["AnzahlFallM"] = LK["AnzahlFallM"].astype(int)
LK["AnzahlFallW"] = LK["AnzahlFallW"].astype(int)
LK["AnzahlTodesfallM"] = LK["AnzahlTodesfallM"].astype(int)
LK["AnzahlTodesfallW"] = LK["AnzahlTodesfallW"].astype(int)
LK["cases100kM"] = round(LK["AnzahlFallM"] / LK["populationM"] * 100000, 1)
LK["cases100kW"] = round(LK["AnzahlFallW"] / LK["populationW"] * 100000, 1)
LK["deaths100kM"] = round(LK["AnzahlTodesfallM"] / LK["populationM"] * 100000, 1)
LK["deaths100kW"] = round(LK["AnzahlTodesfallW"] / LK["populationW"] * 100000, 1)
agg_key = {
    c: "max" if c in ["IdLandkreis"] else "sum"
    for c in LK.columns
    if c not in key_list_BL_age
}
BL = LK.groupby(by=key_list_BL_age, as_index=False).agg(agg_key).copy()
BL["cases100kM"] = round(BL["AnzahlFallM"] / BL["populationM"] * 100000, 1)
BL["cases100kW"] = round(BL["AnzahlFallW"] / BL["populationW"] * 100000, 1)
BL["deaths100kM"] = round(BL["AnzahlTodesfallM"] / BL["populationM"] * 100000, 1)
BL["deaths100kW"] = round(BL["AnzahlTodesfallW"] / BL["populationW"] * 100000, 1)
BL.drop(["IdLandkreis"], inplace=True, axis=1)
agg_key = {
    c: "max" if c in ["IdBundesland", "IdLandkreis"] else "sum"
    for c in BL.columns
    if c not in key_list_ID0_age
}
ID0 = BL.groupby(by=key_list_ID0_age, as_index=False).agg(agg_key).copy()
ID0["IdBundesland"] = "00"
ID0["cases100kM"] = round(ID0["AnzahlFallM"] / ID0["populationM"] * 100000, 1)
ID0["cases100kW"] = round(ID0["AnzahlFallW"] / ID0["populationW"] * 100000, 1)
ID0["deaths100kM"] = round(ID0["AnzahlTodesfallM"] / ID0["populationM"] * 100000, 1)
ID0["deaths100kW"] = round(ID0["AnzahlTodesfallW"] / ID0["populationW"] * 100000, 1)
LK.drop(["populationM", "populationW", "IdBundesland"], inplace=True, axis=1)
BL.drop(["populationM", "populationW"], inplace=True, axis=1)
ID0.drop(["populationM", "populationW"], inplace=True, axis=1)
LK.rename(
    columns={
        "AnzahlFallM": "casesMale",
        "AnzahlFallW": "casesFemale",
        "AnzahlTodesfallM": "deathsMale",
        "AnzahlTodesfallW": "deathsFemale",
        "cases100kM": "casesMalePer100k",
        "cases100kW": "casesFemalePer100k",
        "deaths100kM": "deathsMalePer100k",
        "deaths100kW": "deathsFemalePer100k",
    },
    inplace=True,
)
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)
BL.rename(
    columns={
        "AnzahlFallM": "casesMale",
        "AnzahlFallW": "casesFemale",
        "AnzahlTodesfallM": "deathsMale",
        "AnzahlTodesfallW": "deathsFemale",
        "cases100kM": "casesMalePer100k",
        "cases100kW": "casesFemalePer100k",
        "deaths100kM": "deathsMalePer100k",
        "deaths100kW": "deathsFemalePer100k",
    },
    inplace=True,
)

# store json
path = os.path.join(base_path, "..", "dataStore", "agegroup")
LK_json_path = os.path.join(path, "districts.json")
BL_json_path = os.path.join(path, "states.json")
LK.to_json(
    path_or_buf=LK_json_path, orient="records", date_format="iso", force_ascii=False
)
BL.to_json(
    path_or_buf=BL_json_path, orient="records", date_format="iso", force_ascii=False
)
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

# accumulated and new cases, deaths, recovered, casesPerWeek, deathsPerWeek
# add country column
print(aktuelleZeit, ": calculating new and accumulated data ...")
LK = ut.read_file(fn=feather_path)

# used keylists
key_list_LK_cases = ["IdLandkreis"]
key_list_BL_cases = ["IdBundesland"]
key_list_ID0_cases = ["IdStaat"]

# calculate the values
LK["AnzahlFallAccu"] = np.where(
    LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
).astype(int)
LK["AnzahlFallNeu"] = np.where(
    LK["NeuerFall"].isin([1, -1]), LK["AnzahlFall"], 0
).astype(int)
LK["AnzahlFall7dAccu"] = np.where(
    LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
    LK["AnzahlFallAccu"],
    0,
).astype(int)
LK["AnzahlFall7dNeu"] = np.where(
    LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
    LK["AnzahlFallNeu"],
    0,
).astype(int)
LK["AnzahlTodesfallAccu"] = np.where(
    LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
).astype(int)
LK["AnzahlTodesfallNeu"] = np.where(
    LK["NeuerTodesfall"].isin([1, -1]), LK["AnzahlTodesfall"], 0
).astype(int)
LK["AnzahlTodesfall7dAccu"] = np.where(
    LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
    LK["AnzahlTodesfallAccu"],
    0,
).astype(int)
LK["AnzahlTodesfall7dNeu"] = np.where(
    LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
    LK["AnzahlTodesfallNeu"],
    0,
).astype(int)
LK["AnzahlGenesenAccu"] = np.where(
    LK["NeuGenesen"].isin([1, 0]), LK["AnzahlGenesen"], 0
).astype(int)
LK["AnzahlGenesenNeu"] = np.where(
    LK["NeuGenesen"].isin([1, -1]), LK["AnzahlGenesen"], 0
).astype(int)
LK.drop(
    [
        "NeuGenesen",
        "NeuerFall",
        "NeuerTodesfall",
        "AnzahlFall",
        "AnzahlTodesfall",
        "AnzahlGenesen",
        "Altersgruppe",
        "Geschlecht",
    ],
    inplace=True,
    axis=1,
)
LK.rename(
    columns={
        "AnzahlFallAccu": "accuCases",
        "AnzahlFallNeu": "newCases",
        "AnzahlFall7dAccu": "accuCasesPerWeek",
        "AnzahlFall7dNeu": "newCasesPerWeek",
        "AnzahlTodesfallAccu": "accuDeaths",
        "AnzahlTodesfallNeu": "newDeaths",
        "AnzahlTodesfall7dAccu": "accuDeathsPerWeek",
        "AnzahlTodesfall7dNeu": "newDeathsPerWeek",
        "AnzahlGenesenAccu": "accuRecovered",
        "AnzahlGenesenNeu": "newRecovered",
    },
    inplace=True,
)
agg_key = {
    c: "max"
    if c
    in [
        "IdStaat",
        "IdBundesland",
        "Meldedatum",
        "Datenstand",
        "Landkreis",
        "Bundesland",
    ]
    else "sum"
    for c in LK.columns
    if c not in key_list_LK_cases
}
LK = LK.groupby(by=key_list_LK_cases, as_index=False).agg(agg_key)
agg_key = {
    c: "max"
    if c
    in ["IdStaat", "Meldedatum", "Datenstand", "Bundesland", "IdLandkreis", "Landkreis"]
    else "sum"
    for c in LK.columns
    if c not in key_list_BL_cases
}
BL = LK.groupby(by=key_list_BL_cases, as_index=False).agg(agg_key)
agg_key = {
    c: "max"
    if c
    in [
        "Meldedatum",
        "Datenstand",
        "Bundesland",
        "IdLandkreis",
        "Landkreis",
        "IdBundesland",
    ]
    else "sum"
    for c in BL.columns
    if c not in key_list_ID0_cases
}
ID0 = BL.groupby(by=key_list_ID0_cases, as_index=False).agg(agg_key)
LK.drop(["IdStaat", "IdBundesland"], inplace=True, axis=1)
LK_pop_mask = (
    (BV["AGS"].isin(LK["IdLandkreis"]))
    & (BV["Altersgruppe"] == "A00+")
    & (BV["GueltigAb"] <= Datenstand)
    & (BV["GueltigBis"] >= Datenstand)
)
LK_pop = BV[LK_pop_mask]
LK_pop.reset_index(inplace=True, drop=True)
LK["population"] = LK_pop["Einwohner"]
BL.drop(["IdStaat", "IdLandkreis", "Landkreis"], inplace=True, axis=1)
ID0.drop(["IdStaat", "IdLandkreis", "Landkreis"], inplace=True, axis=1)
ID0["IdBundesland"] = "00"
ID0["Bundesland"] = "Bundesgebiet"
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)
BL_pop_mask = (
    (BV["AGS"].isin(BL["IdBundesland"]))
    & (BV["Altersgruppe"] == "A00+")
    & (BV["GueltigAb"] <= Datenstand)
    & (BV["GueltigBis"] >= Datenstand)
)
BL_pop = BV[BL_pop_mask]
BL_pop.reset_index(inplace=True, drop=True)
BL["population"] = BL_pop["Einwohner"]

# store json
path = os.path.join(base_path, "..", "dataStore", "cases")
LK_json_path = os.path.join(path, "districts.json")
BL_json_path = os.path.join(path, "states.json")
LK.to_json(
    path_or_buf=LK_json_path, orient="records", date_format="iso", force_ascii=False
)
BL.to_json(
    path_or_buf=BL_json_path, orient="records", date_format="iso", force_ascii=False
)
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

# History
# DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
# StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
print(aktuelleZeit, ": calculating history data ...")
LK = ut.read_file(fn=feather_path)
os.remove(path=feather_path)
LK.drop("IdStaat", inplace=True, axis=1)

# used keylists
key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
key_list_BL_hist = ["IdBundesland", "Meldedatum"]
key_list_ID0_hist = ["Meldedatum"]

LK["AnzahlFall"] = np.where(LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0).astype(
    int
)
LK["AnzahlTodesfall"] = np.where(
    LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
).astype(int)
LK["AnzahlGenesen"] = np.where(
    LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0
).astype(int)
LK.drop(
    ["NeuerFall", "NeuerTodesfall", "NeuGenesen", "Altersgruppe", "Geschlecht"],
    inplace=True,
    axis=1,
)
LK.rename(
    columns={
        "AnzahlFall": "cases",
        "AnzahlTodesfall": "deaths",
        "AnzahlGenesen": "recovered",
    },
    inplace=True,
)
agg_key = {
    c: "max"
    if c in ["IdBundesland", "Datenstand", "Landkreis", "Bundesland"]
    else "sum"
    for c in LK.columns
    if c not in key_list_LK_hist
}
LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
agg_key = {
    c: "max"
    if c
    in [
        "IdLandkreis",
        "Datenstand",
        "Landkreis",
        "Bundesland",
    ]
    else "sum"
    for c in LK.columns
    if c not in key_list_BL_hist
}
BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
agg_key = {
    c: "max"
    if c in ["IdBundesland", "IdLandkreis", "Datenstand", "Bundesland", "Landkreis"]
    else "sum"
    for c in BL.columns
    if c not in key_list_ID0_hist
}
ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
LK.drop(["IdBundesland", "Bundesland"], inplace=True, axis=1)
BL.drop(["IdLandkreis", "Landkreis"], inplace=True, axis=1)
ID0.drop(["IdLandkreis", "Landkreis"], inplace=True, axis=1)
ID0["IdBundesland"] = "00"
ID0["Bundesland"] = "Bundesgebiet"
BL = pd.concat([ID0, BL])
BL.reset_index(inplace=True, drop=True)

# store json
path = os.path.join(base_path, "..", "dataStore", "history")
LK_json_path = os.path.join(path, "districts.json")
BL_json_path = os.path.join(path, "states.json")
LK.to_json(
    path_or_buf=LK_json_path, orient="records", date_format="iso", force_ascii=False
)
BL.to_json(
    path_or_buf=BL_json_path, orient="records", date_format="iso", force_ascii=False
)
aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": done.")

endTime = dt.datetime.now()
aktuelleZeit = endTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": total time:", endTime - startTime)
