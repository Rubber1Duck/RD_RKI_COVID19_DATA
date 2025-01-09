import os
from datetime import timedelta
import numpy as np
import pandas as pd
import utils as ut


def fallzahlen_update(dataBaseFeatherFilePath):
    path_fallzahlen = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Fallzahlen", "RKI_COVID19_Fallzahlen.csv")
    path_fallzahlen_BL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Fallzahlen", "RKI_COVID19_Fallzahlen_BL.csv")
    path_fallzahlen_00 = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Fallzahlen", "RKI_COVID19_Fallzahlen_00.csv")

    dtypes_fallzahlen = {
        "Datenstand": "object",
        "IdBundesland": "Int32",
        "IdLandkreis": "Int32",
        "AnzahlFall": "Int32",
        "AnzahlTodesfall": "Int32",
        "AnzahlFall_neu": "Int32",
        "AnzahlTodesfall_neu": "Int32",
        "AnzahlFall_7d": "Int32",
        "report_date": "object",
        "meldedatum_max": "object",
    }
    dtypes_fallzahlen_BL = {
        "Datenstand": "object",
        "IdBundesland": "Int32",
        "AnzahlFall": "Int32",
        "AnzahlTodesfall": "Int32",
        "AnzahlFall_neu": "Int32",
        "AnzahlTodesfall_neu": "Int32",
        "AnzahlFall_7d": "Int32",
        "report_date": "object",
        "meldedatum_max": "object",
    }
    dtypes_fallzahlen_00 = {
        "Datenstand": "object",
        "AnzahlFall": "Int32",
        "AnzahlTodesfall": "Int32",
        "AnzahlFall_neu": "Int32",
        "AnzahlTodesfall_neu": "Int32",
        "AnzahlFall_7d": "Int32",
        "report_date": "object",
        "meldedatum_max": "object",
    }
    key_list = ["Datenstand", "IdBundesland", "IdLandkreis"]
    key_list_BL = ["Datenstand", "IdBundesland"]
    key_list_00 = ["Datenstand"]

    # read covid latest
    covid_df = ut.read_file(fn=dataBaseFeatherFilePath)

    date_latest = covid_df["Datenstand"].max()
    # read fallzahlen current
    fallzahlen_df = pd.read_csv(path_fallzahlen, engine="pyarrow", usecols=dtypes_fallzahlen.keys(), dtype=dtypes_fallzahlen)
    fallzahlen_df_BL = pd.read_csv(path_fallzahlen_BL, engine="pyarrow", usecols=dtypes_fallzahlen_BL.keys(), dtype=dtypes_fallzahlen_BL)
    fallzahlen_df_00 = pd.read_csv(path_fallzahlen_00, engine="pyarrow", usecols=dtypes_fallzahlen_00.keys(), dtype=dtypes_fallzahlen_00)

    # eval fallzahlen new
    covid_df["Meldedatum"] = pd.to_datetime(covid_df["Meldedatum"]).dt.date
    covid_df["AnzahlFall_neu"] = np.where(covid_df["NeuerFall"].isin([-1, 1]), covid_df["AnzahlFall"], 0)
    covid_df["AnzahlFall"] = np.where(covid_df["NeuerFall"].isin([0, 1]), covid_df["AnzahlFall"], 0)
    covid_df["AnzahlFall_7d"] = np.where(covid_df["Meldedatum"] > (date_latest - timedelta(days=8)), covid_df["AnzahlFall"], 0)
    covid_df["AnzahlTodesfall_neu"] = np.where(covid_df["NeuerTodesfall"].isin([-1, 1]), covid_df["AnzahlTodesfall"], 0)
    covid_df["AnzahlTodesfall"] = np.where(covid_df["NeuerTodesfall"].isin([0, 1]), covid_df["AnzahlTodesfall"], 0)
    covid_df.drop(["NeuerFall", "NeuerTodesfall", "Altersgruppe", "Geschlecht", "NeuGenesen", "AnzahlGenesen"], inplace=True, axis=1)
    agg_key = {
        c: "max" if c in ["Meldedatum", "Datenstand"] else "sum"
        for c in covid_df.columns
        if c not in key_list
    }
    covid_df = covid_df.groupby(key_list, as_index=False, observed=True).agg(agg_key)
    covid_copy = covid_df.copy()

    covid_copy.drop("IdLandkreis", axis=1, inplace=True)
    agg_key = {
        c: "max" if c in ["Meldedatum", "Datenstand"] else "sum"
        for c in covid_copy.columns
        if c not in key_list_BL
    }
    covid_df_BL = covid_copy.groupby(key_list_BL, as_index=False, observed=True).agg(agg_key)

    covid_copy.drop("IdBundesland", axis=1, inplace=True)
    agg_key = {
        c: "max" if c in ["Meldedatum", "Datenstand"] else "sum"
        for c in covid_copy.columns
        if c not in key_list_00
    }
    covid_df_00 = covid_df.groupby(key_list_00, as_index=False, observed=True).agg(agg_key)

    covid_df.rename(columns={"Meldedatum": "meldedatum_max"}, inplace=True)
    covid_df_BL.rename(columns={"Meldedatum": "meldedatum_max"}, inplace=True)
    covid_df_00.rename(columns={"Meldedatum": "meldedatum_max"}, inplace=True)
    covid_df["report_date"] = date_latest
    covid_df_BL["report_date"] = date_latest
    covid_df_00["report_date"] = date_latest
    covid_df["IdBundesland"] = covid_df["IdBundesland"].astype(int)
    covid_df_BL["IdBundesland"] = covid_df_BL["IdBundesland"].astype(int)
    covid_df["IdLandkreis"] = covid_df["IdLandkreis"].astype(int)

    # concat and dedup
    fallzahlen_df = fallzahlen_df[fallzahlen_df["Datenstand"] != date_latest]
    fallzahlen_df_BL = fallzahlen_df_BL[fallzahlen_df_BL["Datenstand"] != date_latest]
    fallzahlen_df_00 = fallzahlen_df_00[fallzahlen_df_00["Datenstand"] != date_latest]
    fallzahlen_new = pd.concat([fallzahlen_df, covid_df])
    fallzahlen_new_BL = pd.concat([fallzahlen_df_BL, covid_df_BL])
    fallzahlen_new_00 = pd.concat([fallzahlen_df_00, covid_df_00])

    fallzahlen_new.sort_values(by=key_list, inplace=True)
    fallzahlen_new_BL.sort_values(by=key_list_BL, inplace=True)
    fallzahlen_new_00.sort_values(by=key_list_00, inplace=True)
    fallzahlen_new.reset_index(drop=True, inplace=True)
    fallzahlen_new_BL.reset_index(drop=True, inplace=True)
    fallzahlen_new_00.reset_index(drop=True, inplace=True)

    # write csv
    with open(path_fallzahlen, "wb") as csvfile:
        fallzahlen_new.to_csv(
            csvfile,
            index=False,
            header=True,
            lineterminator="\n",
            encoding="utf-8",
            date_format="%Y-%m-%d",
            columns=dtypes_fallzahlen.keys(),
        )

    # write csv
    with open(path_fallzahlen_BL, "wb") as csvfile:
        fallzahlen_new_BL.to_csv(
            csvfile,
            index=False,
            header=True,
            lineterminator="\n",
            encoding="utf-8",
            date_format="%Y-%m-%d",
            columns=dtypes_fallzahlen_BL.keys(),
        )

    # write csv
    with open(path_fallzahlen_00, "wb") as csvfile:
        fallzahlen_new_00.to_csv(
            csvfile,
            index=False,
            header=True,
            lineterminator="\n",
            encoding="utf-8",
            date_format="%Y-%m-%d",
            columns=dtypes_fallzahlen_00.keys(),
        )
