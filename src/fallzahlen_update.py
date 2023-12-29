import os
from datetime import timedelta
import numpy as np
import pandas as pd
import utils as ut


path_fallzahlen = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Fallzahlen", "RKI_COVID19_Fallzahlen.csv")
path_fallzahlen_feather = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "Fallzahlen", "RKI_COVID19_Fallzahlen.feather")
feather_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dataBase.feather")

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
key_list = ["Datenstand", "IdBundesland", "IdLandkreis"]


# read covid latest
covid_df = ut.read_file(fn=feather_path)
date_latest = covid_df["Datenstand"].max()
# read fallzahlen current
fallzahlen_df = ut.read_file(fn=path_fallzahlen_feather)

# eval fallzahlen new
covid_df["Meldedatum"] = pd.to_datetime(covid_df["Meldedatum"]).dt.date
covid_df["AnzahlFall_neu"] = np.where(covid_df["NeuerFall"].isin([-1, 1]), covid_df["AnzahlFall"], 0)
covid_df["AnzahlFall"] = np.where(covid_df["NeuerFall"].isin([0, 1]), covid_df["AnzahlFall"], 0)
covid_df["AnzahlFall_7d"] = np.where(covid_df["Meldedatum"] > (date_latest - timedelta(days=8)), covid_df["AnzahlFall"], 0)
covid_df["AnzahlTodesfall_neu"] = np.where(covid_df["NeuerTodesfall"].isin([-1, 1]), covid_df["AnzahlTodesfall"], 0)
covid_df["AnzahlTodesfall"] = np.where(covid_df["NeuerTodesfall"].isin([0, 1]), covid_df["AnzahlTodesfall"], 0)
covid_df.drop(["IdStaat", "Bundesland", "Landkreis", "NeuerFall", "NeuerTodesfall", "Altersgruppe", "Geschlecht", "NeuGenesen", "AnzahlGenesen"], inplace=True, axis=1)
agg_key = {
    c: "max" if c in ["Meldedatum", "Datenstand"] else "sum"
    for c in covid_df.columns
    if c not in key_list
}

covid_df = covid_df.groupby(key_list, as_index=False, observed=True).agg(agg_key)
covid_df.rename(columns={"Meldedatum": "meldedatum_max"}, inplace=True)
covid_df["report_date"] = date_latest
covid_df["IdBundesland"] = covid_df["IdBundesland"].astype(int)
covid_df["IdLandkreis"] = covid_df["IdLandkreis"].astype(int)

# concat and dedup
fallzahlen_df = fallzahlen_df[fallzahlen_df["Datenstand"] != date_latest]

fallzahlen_new = pd.concat([fallzahlen_df, covid_df])
fallzahlen_new.sort_values(by=key_list, inplace=True)
fallzahlen_new.reset_index(drop=True, inplace=True)

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
ut.write_file(df=fallzahlen_new, fn=path_fallzahlen_feather, compression="lz4")
os.remove(path=feather_path)
