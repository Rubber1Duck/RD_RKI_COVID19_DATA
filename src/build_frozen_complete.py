import os
import re
import datetime as dt
import numpy as np
import pandas as pd
import utils as ut

startTime = dt.datetime.now()

kum_file_LK = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "dataStore",
    "frozen-incidence",
    "LK_init.json.xz",
)

kum_file_BL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "dataStore",
    "frozen-incidence",
    "BL_init.json.xz",
)

BV_csv_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "Bevoelkerung", "Bevoelkerung.csv"
)

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
    "NeuerFall": "Int32",
    "AnzahlFall": "Int32",
    "Meldedatum": "object",
}

# used keylists
keys_LK = ["IdStaat", "IdLandkreis"]
keys_BL = ["IdStaat", "IdBundesland"]
keys_ID0 = ["IdStaat"]

# open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV["GueltigAb"] = pd.to_datetime(BV["GueltigAb"])
BV["GueltigBis"] = pd.to_datetime(BV["GueltigBis"])

# ----- Squeeze the dataframe to ideal memory size
BV = ut.squeeze_dataframe(BV)

# open existing kum files
try:
    LK_kum = pd.read_json(path_or_buf=kum_file_LK, dtype=kum_dtypes)
    BL_kum = pd.read_json(path_or_buf=kum_file_BL, dtype=kum_dtypes)
    LK_kum["D"] = pd.to_datetime(LK_kum["D"]).dt.date
    BL_kum["D"] = pd.to_datetime(BL_kum["D"]).dt.date
    kum_exists = True
except:
    kum_exists = False
keys_kum = ["D", "I"]

# find all data file
data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")
data_path = os.path.normpath(data_path)
iso_date_re = "([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])"
file_list = os.listdir(data_path)
file_list.sort(reverse=False)
pattern = "RKI_COVID19"
all_data_files = []
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
            all_data_files.append((file, file_path_full, report_date))

# step through all data files
for file, file_path_full, report_date in all_data_files:
    fileStartTime = dt.datetime.now()
    LK = pd.read_csv(file_path_full, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    Datenstand = dt.datetime.strptime(report_date, "%Y-%m-%d")
    lines = LK.shape[0]

    LK["Meldedatum"] = pd.to_datetime(LK["Meldedatum"]).dt.date
    LK.insert(loc=0, column="Datenstand", value=Datenstand.date())
    LK.insert(loc=0, column="IdStaat", value="00")
    LK.sort_values(
        by=["IdLandkreis", "Meldedatum"], axis=0, inplace=True, ignore_index=True
    )
    LK.reset_index(drop=True, inplace=True)

    # ----- Squeeze the dataframe to ideal memory size
    LK = ut.squeeze_dataframe(LK)

    LK["AnzahlFall"] = np.where(
        LK["NeuerFall"].isin([0, 1]), LK["AnzahlFall"], 0
    ).astype(int)
    LK["AnzahlFall_7d"] = np.where(
        LK["Meldedatum"] > (Datenstand.date() - dt.timedelta(days=8)),
        LK["AnzahlFall"],
        0,
    ).astype(int)
    LK.drop(["Meldedatum", "NeuerFall", "AnzahlFall"], inplace=True, axis=1)
    agg_LK = {
        c: "max" if c in ["Datenstand"] else "sum"
        for c in LK.columns
        if c not in keys_LK
    }
    LK = LK.groupby(by=keys_LK, as_index=False, observed=True).agg(agg_LK)
    LK["IdLandkreis"] = LK["IdLandkreis"].astype(str).str.zfill(5)
    LK.insert(loc=0, column="IdBundesland", value=LK["IdLandkreis"].str[:-3].copy())

    agg_BL = {
        c: "max" if c in ["IdLandkreis", "Datenstand"] else "sum"
        for c in LK.columns
        if c not in keys_BL
    }
    BL = LK.groupby(by=keys_BL, as_index=False, observed=True).agg(agg_BL)

    agg_ID0 = {
        c: "max" if c in ["IdBundesland", "IdLandkreis", "Datenstand"] else "sum"
        for c in BL.columns
        if c not in keys_ID0
    }
    ID0 = BL.groupby(by=keys_ID0, as_index=False, observed=True).agg(agg_ID0)

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
    LK_BV_valid.reset_index(inplace=True, drop=True)
    LK_BV_valid.drop(
        ["Altersgruppe", "GueltigAb", "GueltigBis", "männlich", "weiblich"],
        inplace=True,
        axis=1,
    )
    LK = LK.merge(LK_BV_valid, how="right", left_on="IdLandkreis", right_on="AGS")
    LK["AnzahlFall_7d"] = np.where(
        LK["AnzahlFall_7d"].isnull(), 0, LK["AnzahlFall_7d"]
    ).astype(int)
    LK["Datenstand"] = np.where(
        LK["Datenstand"].isnull(), Datenstand.date(), LK["Datenstand"]
    )
    LK["incidence_7d"] = LK["AnzahlFall_7d"] / LK["Einwohner"] * 100000
    LK.drop(["Einwohner", "IdLandkreis", "AnzahlFall_7d"], inplace=True, axis=1)

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
    BL_BV_valid.reset_index(inplace=True, drop=True)
    BL = BL.merge(BL_BV_valid, how="right", left_on="IdBundesland", right_on="AGS")
    BL["AnzahlFall_7d"] = np.where(
        BL["AnzahlFall_7d"].isnull(), 0, BL["AnzahlFall_7d"]
    ).astype(int)
    BL["Datenstand"] = np.where(
        BL["Datenstand"].isnull(), Datenstand.date(), BL["Datenstand"]
    )
    BL["incidence_7d"] = BL["AnzahlFall_7d"] / BL["Einwohner"] * 100000
    BL.drop(["Einwohner", "IdBundesland", "AnzahlFall_7d"], inplace=True, axis=1)

    # rename columns for shorter json files
    LK.rename(
        columns={"Datenstand": "D", "AGS": "I", "Name": "T", "incidence_7d": "i"},
        inplace=True,
    )
    BL.rename(
        columns={"Datenstand": "D", "AGS": "I", "Name": "T", "incidence_7d": "i"},
        inplace=True,
    )

    Datenstand2 = Datenstand.date()

    if kum_exists:
        LK_kum = LK_kum[LK_kum["D"] != Datenstand2]
        BL_kum = BL_kum[BL_kum["D"] != Datenstand2]
        LK_kum = pd.concat([LK_kum, LK])
        BL_kum = pd.concat([BL_kum, BL])
    else:
        LK_kum = LK.copy()
        BL_kum = BL.copy()
        kum_exists = True
    BL_kum.sort_values(by=keys_kum, inplace=True)
    LK_kum.sort_values(by=keys_kum, inplace=True)
    fileEndTime = dt.datetime.now()
    fileTimeDelta = (fileEndTime - fileStartTime).total_seconds()
    aktuelleZeit = fileEndTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
    print(
        aktuelleZeit, ":", file, "done.", "fileTime:", fileTimeDelta, " Lines:", lines
    )

LK_kum["D"] = LK_kum["D"].astype(str)
BL_kum["D"] = BL_kum["D"].astype(str)

# save LK_init.json.xz
LK_kum.to_json(
    path_or_buf=kum_file_LK,
    orient="records",
    date_format="iso",
    force_ascii=False,
    compression="infer",
)

# save BL_init.json.xz
BL_kum.to_json(
    path_or_buf=kum_file_BL,
    orient="records",
    date_format="iso",
    force_ascii=False,
    compression="infer",
)

endTime = dt.datetime.now()
aktuelleZeit = endTime.strftime(format="%Y-%m-%dT%H:%M:%SZ")
print(aktuelleZeit, ": total time:", endTime - startTime)
