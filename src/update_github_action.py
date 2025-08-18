import os
import datetime as dt
import time
import numpy as np
import pandas as pd
import json
import utils as ut
import gc
import fallzahlen_update
from multiprocess_pandas import applyparallel

if __name__ == "__main__":
    startTime = dt.datetime.now()
    base_path = os.path.dirname(os.path.abspath(__file__))
    meta_path = os.path.join(base_path, "..", "dataStore", "meta")
    filename_meta = "meta_new.json"

    BV_csv_path = os.path.join(base_path, "..", "Bevoelkerung", "Bevoelkerung.csv")
    LK_dtypes = {
        "Datenstand": "object",
        "IdLandkreis": "str",
        "Landkreis": "str",
        "incidence_7d": "float64",
    }
    LK_dtypes_single_files = {
        "Datenstand": "object",
        "IdLandkreis": "str",
        "Landkreis": "str",
        "AnzahlFall_7d": "int32",
        "incidence_7d": "float64",
    }
    BL_dtypes = {
        "Datenstand": "object",
        "IdBundesland": "str",
        "Bundesland": "str",
        "incidence_7d": "float64",
    }
    kum_dtypes = {"D": "object", "I": "str", "T": "str", "i": "float64"}
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

    BV_BL = BV[BV["AGS"].str.len() == 2].copy()
    BV_BL.reset_index(inplace=True, drop=True)

    BV_BL_A00 = BV_BL[BV_BL["Altersgruppe"] == "A00+"].copy()
    BV_BL_A00.reset_index(inplace=True, drop=True)

    BV_LK = BV[BV["AGS"].str.len() == 5].copy()
    BV_LK.reset_index(inplace=True, drop=True)

    BV_LK_A00 = BV_LK[BV_LK["Altersgruppe"] == "A00+"].copy()
    BV_LK_A00.reset_index(inplace=True, drop=True)

    # load covid latest from web
    t1 = time.time()
    with open(meta_path + "/" + filename_meta, "r", encoding="utf8") as file:
        metaObj = json.load(file)
    fileNameOrig = metaObj["filename"]
    fileSize = int(metaObj["size"])
    url = metaObj["url"]
    timeStamp = metaObj["modified"]
    Datenstand = dt.datetime.fromtimestamp(timeStamp / 1000)
    Datenstand = Datenstand.replace(hour=0, minute=0, second=0, microsecond=0)
    filedate = (
        dt.datetime.fromtimestamp(metaObj["modified"] / 1000)
        .date()
        .strftime("%Y-%m-%d")
    )
    fileSizeMb = round(fileSize / 1024 / 1024, 1)
    fileNameRoot = "RKI_COVID19_"
    fileName = fileNameRoot + filedate + ".csv"
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        f"{aktuelleZeit} : load {fileNameOrig} (size: {fileSize} bytes => {fileSizeMb} MegaByte) to dataframe ...",
        end="",
    )

    # for testing or fixing uncommend the following lines and set the values
    # path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
    # testfile = os.path.join(path, '2023-12-26_Deutschland_SarsCov2_Infektionen.csv.xz')
    # LK = pd.read_csv(testfile, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    # Datenstand = dt.datetime(year=2023, month=12, day=26, hour=0, minute=0, second=0, microsecond=0)
    # fileName = "RKI_COVID19_2023-12-26.csv"
    # url = os.path.join(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data'), 'RKI_COVID19_2024-02-23.csv')

    LK = pd.read_csv(url, engine="pyarrow", usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    # ----- Squeeze the dataframe
    LK = ut.squeeze_dataframe(LK)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" {LK.shape[0]} rows. done in {round((t2 - t1), 3)} secs.")
    t1 = time.time()
    data_path = os.path.join(base_path, "..", "data")
    fileNameXz = fileName + ".xz"
    full_path = os.path.join(base_path, data_path, fileName)
    full_pathXz = os.path.join(base_path, data_path, fileNameXz)
    data_path = os.path.normpath(data_path)
    full_path = os.path.normpath(full_path)
    istDatei = os.path.isfile(full_path)
    istDateiXz = os.path.isfile(full_pathXz)
    if (istDatei):
        os.remove(full_path)
    if (istDateiXz):
        os.remove(full_pathXz)
    print(f"{aktuelleZeit} : writing DataFrame to {fileName} ...", end="")
    LK.to_csv(
        full_path,
        index=False,
        header=True,
        lineterminator="\n",
        encoding="utf-8",
        date_format="%Y-%m-%d",
        columns=CV_dtypes.keys(),
    )
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")
    
    print(f"{aktuelleZeit} : add missing columns ...", end="")
    t1 = time.time()
    LK["IdLandkreis"] = LK["IdLandkreis"].map("{:0>5}".format)
    LK.insert(loc=0, column="IdBundesland", value=LK["IdLandkreis"].str.slice(0, 2))
    LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    LK.insert(loc=0, column="Datenstand", value=Datenstand.date())

    # ----- Squeeze the dataframe
    LK = ut.squeeze_dataframe(LK)
    feather_path = os.path.join(data_path, fileNameRoot + filedate + ".feather")
    # store dataBase to feather file to save memory
    ut.write_file(df=LK, fn=feather_path, compression="lz4")
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")

    # ageGroup Data
    print(f"{aktuelleZeit} : calculating age-group data ...", end="")
    t1 = time.time()
    # kopiere dataBase ohne unbekannte Altersgruppen oder unbekannte Geschlechter
    LK = LK[LK["Altersgruppe"] != "unbekannt"].copy()
    LK = LK[LK["Geschlecht"] != "unbekannt"].copy()

    # korrigiere Kategorien Altersgruppe und Geschlecht
    LK["Geschlecht"] = LK["Geschlecht"].cat.remove_unused_categories()
    LK["Altersgruppe"] = LK["Altersgruppe"].cat.remove_unused_categories()

    # lösche alle nicht benötigten Spalten
    LK.drop(["Meldedatum", "Datenstand"], inplace=True, axis=1)

    # used keylists
    key_list_LK_age = ["IdLandkreis", "Altersgruppe"]
    key_list_BL_age = ["IdBundesland", "Altersgruppe"]
    key_list_ID0_age = ["Altersgruppe"]

    # calculate the age group data
    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["casesMale"] = np.where(LK["Geschlecht"] == "M", LK["AnzahlFall"], 0).astype(int)
    LK["casesFemale"] = np.where(LK["Geschlecht"] == "W", LK["AnzahlFall"], 0).astype(
        int
    )
    LK["AnzahlTodesfall"] = np.where(
        LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["deathsMale"] = np.where(
        LK["Geschlecht"] == "M", LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["deathsFemale"] = np.where(
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
    LK = LK.groupby(by=key_list_LK_age, as_index=False, observed=False).agg(agg_key)
    BV_mask = (
        (BV["AGS"].isin(LK["IdLandkreis"]))
        & (BV["Altersgruppe"].isin(LK["Altersgruppe"]))
        & (BV["GueltigAb"] <= Datenstand)
        & (BV["GueltigBis"] >= Datenstand)
    )
    BV_masked = BV[BV_mask].copy()
    BV_masked.drop(["GueltigAb", "GueltigBis", "Einwohner"], inplace=True, axis=1)
    BV_masked.rename(columns={"AGS": "IdLandkreis"}, inplace=True)

    LK = LK.merge(right=BV_masked, on=["IdLandkreis", "Altersgruppe"], how="left")
    LK = LK[LK["Name"].notna()]
    LK.drop(["Name"], inplace=True, axis=1)

    LK["casesMale"] = LK["casesMale"].astype(int)
    LK["casesFemale"] = LK["casesFemale"].astype(int)
    LK["deathsMale"] = LK["deathsMale"].astype(int)
    LK["deathsFemale"] = LK["deathsFemale"].astype(int)
    LK["casesMalePer100k"] = round(LK["casesMale"] / LK["männlich"] * 100000, 1)
    LK["casesFemalePer100k"] = round(LK["casesFemale"] / LK["weiblich"] * 100000, 1)
    LK["deathsMalePer100k"] = round(LK["deathsMale"] / LK["männlich"] * 100000, 1)
    LK["deathsFemalePer100k"] = round(LK["deathsFemale"] / LK["weiblich"] * 100000, 1)

    agg_key = {
        c: "max" if c in ["IdLandkreis"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_age
    }
    BL = (
        LK.groupby(by=key_list_BL_age, as_index=False, observed=False)
        .agg(agg_key)
        .copy()
    )
    BL["casesMalePer100k"] = round(BL["casesMale"] / BL["männlich"] * 100000, 1)
    BL["casesFemalePer100k"] = round(BL["casesFemale"] / BL["weiblich"] * 100000, 1)
    BL["deathsMalePer100k"] = round(BL["deathsMale"] / BL["männlich"] * 100000, 1)
    BL["deathsFemalePer100k"] = round(BL["deathsFemale"] / BL["weiblich"] * 100000, 1)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_age
    }
    ID0 = (
        BL.groupby(by=key_list_ID0_age, as_index=False, observed=False)
        .agg(agg_key)
        .copy()
    )
    ID0["IdBundesland"] = "00"
    ID0["casesMalePer100k"] = round(ID0["casesMale"] / ID0["männlich"] * 100000, 1)
    ID0["casesFemalePer100k"] = round(ID0["casesFemale"] / ID0["weiblich"] * 100000, 1)
    ID0["deathsMalePer100k"] = round(ID0["deathsMale"] / ID0["männlich"] * 100000, 1)
    ID0["deathsFemalePer100k"] = round(
        ID0["deathsFemale"] / ID0["weiblich"] * 100000, 1
    )
    LK.drop(["männlich", "weiblich", "IdBundesland"], inplace=True, axis=1)
    BL.drop(["männlich", "weiblich"], inplace=True, axis=1)
    ID0.drop(["männlich", "weiblich"], inplace=True, axis=1)

    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)

    # store as gz compresed json
    path = os.path.join(base_path, "..", "dataStore", "agegroup")
    ut.write_json(LK, "districts.json.xz", path)
    ut.write_json(BL, "states.json.xz", path)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")

    # accumulated and new cases, deaths, recovered, casesPerWeek, deathsPerWeek
    print(f"{aktuelleZeit} : calculating new and accumulated data ...", end="")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)

    # used keylists
    key_list_LK_cases = ["IdLandkreis"]
    key_list_BL_cases = ["IdBundesland"]
    key_list_ID0_cases = ["IdStaat"]

    # calculate the values
    LK["accuCases"] = np.where(
        LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["newCases"] = np.where(
        LK["NeuerFall"].isin([1, -1]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["accuCasesPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["accuCases"],
        0,
    ).astype(int)
    LK["newCasesPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)), LK["newCases"], 0
    ).astype(int)
    LK["accuDeaths"] = np.where(
        LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["newDeaths"] = np.where(
        LK["NeuerTodesfall"].isin([1, -1]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["accuDeathsPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["accuDeaths"],
        0,
    ).astype(int)
    LK["newDeathsPerWeek"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["newDeaths"],
        0,
    ).astype(int)
    LK["accuRecovered"] = np.where(
        LK["NeuGenesen"].isin([1, 0]), LK["AnzahlGenesen"], 0
    ).astype(int)
    LK["newRecovered"] = np.where(
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
    agg_key = {
        c: "max" if c in ["IdBundesland", "Meldedatum", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_cases
    }
    LK = LK.groupby(by=key_list_LK_cases, as_index=False, observed=False).agg(agg_key)
    agg_key = {
        c: "max" if c in ["Meldedatum", "Datenstand", "IdLandkreis"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_cases
    }
    BL = LK.groupby(by=key_list_BL_cases, as_index=False, observed=False).agg(agg_key)
    BL.insert(loc=0, column="IdStaat", value="00")
    agg_key = {
        c: (
            "max"
            if c
            in ["Meldedatum", "Datenstand", "Bundesland", "IdLandkreis", "IdBundesland"]
            else "sum"
        )
        for c in BL.columns
        if c not in key_list_ID0_cases
    }
    ID0 = BL.groupby(by=key_list_ID0_cases, as_index=False, observed=False).agg(agg_key)

    BL.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)

    mask = (
        (BV["AGS"].isin(LK["IdLandkreis"]))
        & (BV["Altersgruppe"] == "A00+")
        & (BV["GueltigAb"] <= Datenstand)
        & (BV["GueltigBis"] >= Datenstand)
    )
    masked = BV[mask].copy()
    masked.drop(
        ["GueltigAb", "GueltigBis", "männlich", "weiblich", "Altersgruppe"],
        inplace=True,
        axis=1,
    )
    masked.rename(
        columns={"AGS": "IdLandkreis", "Einwohner": "population", "Name": "Landkreis"},
        inplace=True,
    )
    LK = LK.merge(right=masked, on="IdLandkreis", how="left")
    mask = (
        (BV["AGS"].isin(BL["IdBundesland"]))
        & (BV["Altersgruppe"] == "A00+")
        & (BV["GueltigAb"] <= Datenstand)
        & (BV["GueltigBis"] >= Datenstand)
    )
    masked = BV[mask].copy()
    masked.drop(
        ["GueltigAb", "GueltigBis", "männlich", "weiblich", "Altersgruppe"],
        inplace=True,
        axis=1,
    )
    masked.rename(
        columns={
            "AGS": "IdBundesland",
            "Einwohner": "population",
            "Name": "Bundesland",
        },
        inplace=True,
    )
    BL = BL.merge(right=masked, on="IdBundesland", how="left")
    masked.drop(["population"], inplace=True, axis=1)
    LK = LK.merge(right=masked, on="IdBundesland", how="left")
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK["Datenstand"] = LK["Datenstand"].astype(str)
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    BL["Datenstand"] = BL["Datenstand"].astype(str)
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    # store as gz compressed json
    path = os.path.join(base_path, "..", "dataStore", "cases")
    ut.write_json(LK, "districts.json.xz", path)
    ut.write_json(BL, "states.json.xz", path)
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round((t2 - t1), 3)} secs.")

    # History
    # DistrictCasesHistory, DistrictDeathsHistory, DistrictRecoveredHistory
    # StateCasesHistory, StateDeathsHistory, StateRecoveredHistory
    print(f"{aktuelleZeit} : calculating history data ...")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)

    # used keylists
    key_list_LK_hist = ["IdLandkreis", "Meldedatum"]
    key_list_BL_hist = ["IdBundesland", "Meldedatum"]
    key_list_ID0_hist = ["Meldedatum"]

    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([1, 0]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["AnzahlTodesfall"] = np.where(
        LK["NeuerTodesfall"].isin([1, 0, -9]), LK["AnzahlTodesfall"], 0
    ).astype(int)
    LK["AnzahlGenesen"] = np.where(
        LK["NeuGenesen"].isin([1, 0, -9]), LK["AnzahlGenesen"], 0
    ).astype(int)
    LK.drop(
        [
            "NeuerFall",
            "NeuerTodesfall",
            "NeuGenesen",
            "Altersgruppe",
            "Geschlecht",
            "Datenstand",
        ],
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
        c: "max" if c in ["IdBundesland", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_hist
    }
    LK = LK.groupby(by=key_list_LK_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdLandkreis", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_hist
    }
    BL = LK.groupby(by=key_list_BL_hist, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis", "Datenstand"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_hist
    }
    ID0 = BL.groupby(by=key_list_ID0_hist, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdBundesland"], inplace=True, axis=1)
    LK.sort_values(by=key_list_LK_hist, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    BL.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.sort_values(by=key_list_BL_hist, inplace=True)
    BL.reset_index(inplace=True, drop=True)

    # fill dates for every region
    allDates = pd.DataFrame(
        pd.date_range(
            end=(Datenstand - dt.timedelta(days=1)), start="2019-12-26"
        ).strftime("%Y-%m-%d"),
        columns=["Meldedatum"],
    )
    BLDates = pd.DataFrame(
        pd.unique(BL["IdBundesland"]).copy(), columns=["IdBundesland"]
    )
    LKDates = pd.DataFrame(pd.unique(LK["IdLandkreis"]).copy(), columns=["IdLandkreis"])
    # add Bundesland, Landkreis and Einwohner
    BV_mask = (
        (BV_BL_A00["AGS"].isin(BLDates["IdBundesland"]))
        & (BV_BL_A00["GueltigAb"] <= Datenstand)
        & (BV_BL_A00["GueltigBis"] >= Datenstand)
    )
    BV_masked = BV_BL_A00[BV_mask].copy()
    BV_masked.drop(
        ["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BV_masked.rename(
        columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True
    )
    BLDates = BLDates.merge(right=BV_masked, on=["IdBundesland"], how="left")

    BV_mask = (
        (BV_LK_A00["AGS"].isin(LK["IdLandkreis"]))
        & (BV_LK_A00["GueltigAb"] <= Datenstand)
        & (BV_LK_A00["GueltigBis"] >= Datenstand)
    )
    BV_masked = BV_LK_A00[BV_mask].copy()
    BV_masked.drop(
        ["GueltigAb", "GueltigBis", "Altersgruppe", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BV_masked.rename(columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True)
    LKDates = LKDates.merge(right=BV_masked, on="IdLandkreis", how="left")

    BLDates = BLDates.merge(allDates, how="cross")
    BLDates = ut.squeeze_dataframe(BLDates)
    LKDates = LKDates.merge(allDates, how="cross")
    LKDates = ut.squeeze_dataframe(LKDates)
    BL["Meldedatum"] = BL["Meldedatum"].astype(str)
    LK["Meldedatum"] = LK["Meldedatum"].astype(str)
    BL = BL.merge(BLDates, how="right", on=["IdBundesland", "Meldedatum"])
    LK = LK.merge(LKDates, how="right", on=["IdLandkreis", "Meldedatum"])

    # clear unneeded data
    ID0 = pd.DataFrame()
    allDates = pd.DataFrame()
    BLDates = pd.DataFrame()
    LKDates = pd.DataFrame()
    BV_mask = pd.DataFrame()
    BV_masked = pd.DataFrame()
    del ID0
    del allDates
    del BLDates
    del LKDates
    del BV_mask
    del BV_masked
    gc.collect()

    # fill nan with 0
    BL["cases"] = BL["cases"].fillna(0).astype(int)
    BL["deaths"] = BL["deaths"].fillna(0).astype(int)
    BL["recovered"] = BL["recovered"].fillna(0).astype(int)

    LK["cases"] = LK["cases"].fillna(0).astype(int)
    LK["deaths"] = LK["deaths"].fillna(0).astype(int)
    LK["recovered"] = LK["recovered"].fillna(0).astype(int)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        f"{aktuelleZeit} :   |-calculating BL incidence {BL.shape[0]} rows ...", end=""
    )
    t11 = time.time()
    BL = BL.groupby(["IdBundesland"], observed=True).apply_parallel(ut.calc_incidence)
    t12 = time.time()
    print(f" Done in {round(t12 - t11, 3)} sec.")
    BL.reset_index(inplace=True, drop=True)
    BL["incidence7d"] = (BL["cases7d"] / BL["Einwohner"] * 100000).round(5)
    BL.drop(["Einwohner"], inplace=True, axis=1)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        f"{aktuelleZeit} :   |-calculating LK incidence {LK.shape[0]} rows ...", end=""
    )
    t11 = time.time()
    LK = LK.groupby(["IdLandkreis"], observed=True).apply_parallel(ut.calc_incidence)
    t12 = time.time()
    print(f" Done in {round(t12-t11, 3)} sec.")
    LK.reset_index(inplace=True, drop=True)
    LK["incidence7d"] = (LK["cases7d"] / LK["Einwohner"] * 100000).round(5)
    LK.drop(["Einwohner"], inplace=True, axis=1)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(
        f"{aktuelleZeit} : history data calculation done in {round((t2 - t1), 3)} secs."
    )

    print(
        f"{aktuelleZeit} : write data file in format feather for DATA5 to disk ...",
        end="",
    )
    pathBL = os.path.join(
        base_path, "..", "dataStore", "historychanges", "BL_BaseData.feather"
    )
    pathLK = os.path.join(
        base_path, "..", "dataStore", "historychanges", "LK_BaseData.feather"
    )
    ut.write_file(BL, pathBL, "lz4")
    ut.write_file(LK, pathLK, "lz4")
    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(" done.")

    print(f"{aktuelleZeit} : write files to disk ...", end="")
    t1 = time.time()
    # store gz compressed json
    path = os.path.join(base_path, "..", "dataStore", "history")
    # complete districts (cases, deaths, recovered. incidence)
    ut.write_json(LK, "districts.json", path)
    # complete states (cases, deaths, recovered. incidence)
    ut.write_json(BL, "states.json", path)

    # complete districts (cases, deaths, recovered. incidence) short
    LK.rename(
        columns={
            "IdLandkreis": "i",
            "Landkreis": "t",
            "Meldedatum": "m",
            "cases": "c",
            "deaths": "d",
            "recovered": "r",
            "cases7d": "c7",
            "incidence7d": "i7",
        },
        inplace=True,
    )
    ut.write_json(LK, "districts_new.json", path)
    # complete states (cases, deaths, recovered. incidence) short
    BL.rename(
        columns={
            "IdBundesland": "i",
            "Bundesland": "t",
            "Meldedatum": "m",
            "cases": "c",
            "deaths": "d",
            "recovered": "r",
            "cases7d": "c7",
            "incidence7d": "i7",
        },
        inplace=True,
    )
    ut.write_json(BL, "states_new.json", path)
    # single districts json files per category short
    # cases
    out = LK[["i", "m", "c"]].copy()
    ut.write_json(out, "d_cases_short.json", path)
    # deaths
    out = LK[["i", "m", "d"]].copy()
    ut.write_json(out, "d_deaths_short.json", path)
    # recovered
    out = LK[["i", "m", "r"]].copy()
    ut.write_json(out, "d_recovered_short.json", path)
    # incidence (and cases per week)
    out = LK[["i", "m", "c7", "i7"]].copy()
    ut.write_json(out, "d_incidence_short.json", path)
    # single states json files per category short
    # cases
    out = BL[["i", "m", "c"]].copy()
    ut.write_json(out, "s_cases_short.json", path)
    # deaths
    out = BL[["i", "m", "d"]].copy()
    ut.write_json(out, "s_deaths_short.json", path)
    # recovered
    out = BL[["i", "m", "r"]].copy()
    ut.write_json(out, "s_recovered_short.json", path)
    # incidence
    out = BL[["i", "m", "c7", "i7"]].copy()
    ut.write_json(out, "s_incidence_short.json", path)

    out = pd.DataFrame()
    del out
    gc.collect

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round(t2 - t1, 3)} secs.")

    # *******************
    # * fixed-incidence *
    # *******************
    print(f"{aktuelleZeit} : calculating fixed-incidence data ...", end="")
    t1 = time.time()
    LK = ut.read_file(fn=feather_path)
    LK["IdStaat"] = "00"

    # used keylists
    key_list_LK_fix = ["IdStaat", "IdBundesland", "IdLandkreis"]
    key_list_BL_fix = ["IdStaat", "IdBundesland"]
    key_list_ID0_fix = ["IdStaat"]

    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([0, 1]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["AnzahlFall_7d"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["AnzahlFall"],
        0,
    ).astype(int)
    LK.drop(
        [
            "Meldedatum",
            "NeuerFall",
            "NeuerTodesfall",
            "AnzahlFall",
            "AnzahlTodesfall",
            "NeuGenesen",
            "AnzahlGenesen",
            "Altersgruppe",
            "Geschlecht",
        ],
        inplace=True,
        axis=1,
    )
    agg_key = {
        c: "max" if c in ["Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_LK_fix
    }
    LK = LK.groupby(by=key_list_LK_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdLandkreis", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in key_list_BL_fix
    }
    BL = LK.groupby(by=key_list_BL_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: "max" if c in ["IdBundesland", "IdLandkreis", "Datenstand"] else "sum"
        for c in BL.columns
        if c not in key_list_ID0_fix
    }
    ID0 = BL.groupby(by=key_list_ID0_fix, as_index=False, observed=True).agg(agg_key)
    LK.drop(["IdStaat", "IdBundesland"], inplace=True, axis=1)
    BL.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0.drop(["IdStaat", "IdLandkreis"], inplace=True, axis=1)
    ID0["IdBundesland"] = "00"
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    LK_BV_valid = BV[
        (
            (BV["Altersgruppe"] == "A00+")
            & (BV["GueltigAb"] <= Datenstand)
            & (BV["GueltigBis"] >= Datenstand)
            & (BV["AGS"].str.len() == 5)
        )
    ].copy()
    LK_BV_valid.drop(
        ["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    LK_BV_valid.rename(
        columns={"AGS": "IdLandkreis", "Name": "Landkreis"}, inplace=True
    )
    LK = LK.merge(LK_BV_valid, how="right", on="IdLandkreis")
    LK["AnzahlFall_7d"] = np.where(
        LK["AnzahlFall_7d"].isnull(), 0, LK["AnzahlFall_7d"]
    ).astype(int)
    LK["Datenstand"] = np.where(
        LK["Datenstand"].isnull(), Datenstand.date(), LK["Datenstand"]
    )
    LK["incidence_7d"] = LK["AnzahlFall_7d"] / LK["Einwohner"] * 100000
    LK.drop(["Einwohner"], inplace=True, axis=1)

    BL_BV_valid = BV[
        (
            (BV["Altersgruppe"] == "A00+")
            & (BV["GueltigAb"] <= Datenstand)
            & (BV["GueltigBis"] >= Datenstand)
            & (BV["AGS"].str.len() == 2)
        )
    ].copy()
    BL_BV_valid.drop(
        ["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    BL_BV_valid.rename(
        columns={"AGS": "IdBundesland", "Name": "Bundesland"}, inplace=True
    )
    BL = BL.merge(BL_BV_valid, how="right", on="IdBundesland")
    BL["AnzahlFall_7d"] = np.where(
        BL["AnzahlFall_7d"].isnull(), 0, BL["AnzahlFall_7d"]
    ).astype(int)
    BL["Datenstand"] = np.where(
        BL["Datenstand"].isnull(), Datenstand.date(), BL["Datenstand"]
    )
    BL["incidence_7d"] = BL["AnzahlFall_7d"] / BL["Einwohner"] * 100000
    BL.drop(["Einwohner"], inplace=True, axis=1)

    LK.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    BL.drop(["AnzahlFall_7d"], inplace=True, axis=1)
    # open existing kum files
    path = os.path.join(base_path, "..", "dataStore", "frozen-incidence")

    LK_kum_old = ut.read_json(fn="LK.json.xz", dtype=LK_dtypes, path=path)
    BL_kum_old = ut.read_json(fn="BL.json.xz", dtype=BL_dtypes, path=path)

    LK_kum_new = ut.read_json(fn="LK_complete.json.xz", dtype=kum_dtypes, path=path)
    BL_kum_new = ut.read_json(fn="BL_complete.json.xz", dtype=kum_dtypes, path=path)

    LK_kum_old["Datenstand"] = pd.to_datetime(LK_kum_old["Datenstand"]).dt.date
    BL_kum_old["Datenstand"] = pd.to_datetime(BL_kum_old["Datenstand"]).dt.date

    LK_kum_new["D"] = pd.to_datetime(LK_kum_new["D"]).dt.date
    BL_kum_new["D"] = pd.to_datetime(BL_kum_new["D"]).dt.date

    key_list_LK = ["Datenstand", "IdLandkreis"]
    key_list_BL = ["Datenstand", "IdBundesland"]
    key_list_kum = ["D", "I"]

    Datenstand2 = Datenstand.date()

    LK_kum_old = LK_kum_old[LK_kum_old["Datenstand"] != Datenstand2]
    BL_kum_old = BL_kum_old[BL_kum_old["Datenstand"] != Datenstand2]

    LK_kum_new = LK_kum_new[LK_kum_new["D"] != Datenstand2]
    BL_kum_new = BL_kum_new[BL_kum_new["D"] != Datenstand2]

    LK["Datenstand"] = pd.to_datetime(LK["Datenstand"]).dt.date
    BL["Datenstand"] = pd.to_datetime(BL["Datenstand"]).dt.date

    LK_kum_old = pd.concat([LK_kum_old, LK])
    LK_kum_old.sort_values(by=key_list_LK, inplace=True)
    BL_kum_old = pd.concat([BL_kum_old, BL])
    BL_kum_old.sort_values(by=key_list_BL, inplace=True)

    LK_kum_old["Datenstand"] = LK_kum_old["Datenstand"].astype(str)
    BL_kum_old["Datenstand"] = BL_kum_old["Datenstand"].astype(str)

    ut.write_json(df=LK_kum_old, fn="LK.json.xz", pt=path)
    ut.write_json(df=BL_kum_old, fn="BL.json.xz", pt=path)

    LK.rename(
        columns={
            "Datenstand": "D",
            "IdLandkreis": "I",
            "Landkreis": "T",
            "incidence_7d": "i",
        },
        inplace=True,
    )
    BL.rename(
        columns={
            "Datenstand": "D",
            "IdBundesland": "I",
            "Bundesland": "T",
            "incidence_7d": "i",
        },
        inplace=True,
    )
    LK_kum_new = pd.concat([LK_kum_new, LK])
    LK_kum_new.sort_values(by=key_list_kum, inplace=True)
    BL_kum_new = pd.concat([BL_kum_new, BL])
    BL_kum_new.sort_values(by=key_list_kum, inplace=True)

    LK_kum_new["D"] = LK_kum_new["D"].astype(str)
    BL_kum_new["D"] = BL_kum_new["D"].astype(str)

    ut.write_json(df=LK_kum_new, fn="LK_complete.json.xz", pt=path)
    ut.write_json(df=BL_kum_new, fn="BL_complete.json.xz", pt=path)

    aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
    t2 = time.time()
    print(f" done in {round(t2 - t1, 3)} secs.")
    print(f"{aktuelleZeit} : Fallzahlen update ...", end="")
    t1 = time.time()
    fallzahlen_update.fallzahlen_update(feather_path)
    t2 = time.time()
    endTime = dt.datetime.now()
    os.remove(feather_path)
    aktuelleZeit = endTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(f" done in {round(t2 - t1, 3)} secs.")
    print(f"{aktuelleZeit} : total python time: {endTime - startTime} .")
