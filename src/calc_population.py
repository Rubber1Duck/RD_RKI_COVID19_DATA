import os
import json
import sys
import pandas as pd
import requests as rq
import datetime as dt

try:
    today = dt.date.today()
    yesterday = today - dt.timedelta(1)
    todayStr = today.strftime("%Y-%m-%d")
    yesterdayStr = yesterday.strftime("%Y-%m-%d")
    EinwohnerUrl = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/arcgis/rest/services/RKI_Landkreisdaten/FeatureServer/0/query?where=1%3D1&outFields=RS,EWZ,last_update&returnGeometry=false&outSR=4326&f=json"
    EinwohnerData = json.loads(rq.get(EinwohnerUrl).text)
    EWZ_RKI = pd.json_normalize(EinwohnerData, record_path=["features"])
    EWZ_RKI.rename(
        columns={
            "attributes.RS": "AGS",
            "attributes.EWZ": "EWZ",
            "attributes.last_update": "Datenstand",
        },
        inplace=True,
    )
    datenstand = pd.to_datetime(
        EWZ_RKI["Datenstand"].iloc[0], format="%d.%m.%Y, %H:%M Uhr"
    )
    EWZ_RKI.drop(["Datenstand"], inplace=True, axis=1)
    EWZ_RKI.sort_values(["AGS"], axis=0, inplace=True)
    EWZ_RKI.reset_index(inplace=True, drop=True)
    LKurl1 = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_altersgruppen_hubv/FeatureServer/0/query?where=AdmUnitId%3E%3D01001+AND+AdmUnitId%3C%3D06631&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=json&token="
    LKurl2 = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_altersgruppen_hubv/FeatureServer/0/query?where=AdmUnitId%3E%3D06632+AND+AdmUnitId%3C%3D09473&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=json&token="
    LKurl3 = "https://services7.arcgis.com/mOBPykOjAyBO2ZKk/ArcGIS/rest/services/rki_altersgruppen_hubv/FeatureServer/0/query?where=AdmUnitId%3E%3D09474+AND+AdmUnitId%3C%3D16077&objectIds=&time=&resultType=none&outFields=*&returnIdsOnly=false&returnUniqueIdsOnly=false&returnCountOnly=false&returnDistinctValues=false&cacheHint=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&having=&resultOffset=&resultRecordCount=&sqlFormat=none&f=json&token="
    LKdata1 = json.loads(rq.get(LKurl1).text)
    LKdata2 = json.loads(rq.get(LKurl2).text)
    LKdata3 = json.loads(rq.get(LKurl3).text)
    LK1 = pd.json_normalize(LKdata1, record_path=["features"])
    LK2 = pd.json_normalize(LKdata2, record_path=["features"])
    LK3 = pd.json_normalize(LKdata3, record_path=["features"])
    LK = pd.concat([LK1, LK2, LK3])
    LK.drop(
        [
            "attributes.ObjectId",
            "attributes.AnzTodesfall100kW",
            "attributes.AnzTodesfall100kM",
            "attributes.AnzTodesfallW",
            "attributes.AnzTodesfallM",
        ],
        axis=1,
        inplace=True,
    )
    LK.rename(
        columns={
            "attributes.AdmUnitId": "AGS",
            "attributes.BundeslandId": "BundeslandId",
            "attributes.Altersgruppe": "Altersgruppe",
            "attributes.AnzFallM": "AnzFallM",
            "attributes.AnzFallW": "AnzFallW",
            "attributes.AnzFall100kM": "AnzFall100kM",
            "attributes.AnzFall100kW": "AnzFall100kW",
        },
        inplace=True,
    )
    LK.sort_values(["AGS", "Altersgruppe"], axis=0, inplace=True)
    LK.reset_index(inplace=True, drop=True)
    LK["BundeslandId"] = LK["BundeslandId"].astype(str).str.zfill(2)
    LK["AGS"] = LK["AGS"].astype(str).str.zfill(5)
    LK["männlich"] = round(LK["AnzFallM"] * 100000 / LK["AnzFall100kM"], 0).astype(int)
    LK["weiblich"] = round(LK["AnzFallW"] * 100000 / LK["AnzFall100kW"], 0).astype(int)
    LK.insert(loc=7, column="Einwohner", value=LK["männlich"] + LK["weiblich"])
    mask = (
        round(LK["AnzFallM"] / LK["männlich"] * 100000, 1) != LK["AnzFall100kM"]
    ) | (round(LK["AnzFallW"] / LK["weiblich"] * 100000, 1) != LK["AnzFall100kW"])
    if LK[mask].empty:
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(
            aktuelleZeit,
            ": alle berechneten Einwohnerzahlen auf Altersgruppenebene sind in der Umkehrung identisch",
        )
    else:
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(
            aktuelleZeit,
            ": Bei folgenden Altergruppen ist der crossCheck fehlgeschlagen:",
        )
        print(LK[mask])
        sys.exit("CrossCheck failed")
    keyListLKA00plus = ["AGS"]
    sumListLKA00plus = ["AnzFallM", "AnzFallW", "männlich", "weiblich", "Einwohner"]
    agg_key = {
        c: "max" if c not in sumListLKA00plus else "sum"
        for c in LK.columns
        if c not in keyListLKA00plus
    }
    LKA00plus = LK.groupby(keyListLKA00plus, as_index=False).agg(agg_key)
    LKA00plus["Altersgruppe"] = "A00+"
    LKA00plus.reset_index(inplace=True, drop=True)
    LKA00plusMask = EWZ_RKI["AGS"].isin(LKA00plus["AGS"])
    LKA00plusCheck = EWZ_RKI[LKA00plusMask]
    LKA00plusCheck.reset_index(inplace=True, drop=True)
    LKA00plusMask = LKA00plus["Einwohner"] != LKA00plusCheck["EWZ"]
    if not LKA00plus[LKA00plusMask].empty:
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(
            aktuelleZeit,
            ": in einigen Zeile/n stimmt auf Landkreisebene die berechnete Einwohnerzahl nicht mit der vom RKI verwendeten überein:",
        )
        print(LKA00plus[LKA00plusMask])
        sys.exit("CrossCheck 2 failed")
    calcBV = pd.concat([LKA00plus, LK])
    calcBV.sort_values(["AGS", "Altersgruppe"], axis=0, inplace=True)
    calcBV.reset_index(inplace=True, drop=True)
    keyListBL = ["BundeslandId", "Altersgruppe"]
    sumListBL = ["AnzFallM", "AnzFallW", "männlich", "weiblich", "Einwohner"]
    agg_key = {
        c: "max" if c not in sumListBL else "sum"
        for c in calcBV.columns
        if c not in keyListBL
    }
    BL = calcBV.groupby(keyListBL, as_index=False).agg(agg_key)
    calcBV.drop(["BundeslandId"], inplace=True, axis=1)
    BL.drop(["AGS"], inplace=True, axis=1)
    BL.rename(columns={"BundeslandId": "AGS"}, inplace=True)
    calcBV = pd.concat([calcBV, BL])
    BL["AGS"] = "00"
    keyListBL0 = ["AGS", "Altersgruppe"]
    sumListBL0 = ["AnzFallM", "AnzFallW", "männlich", "weiblich", "Einwohner"]
    agg_key = {
        c: "max" if c not in sumListBL0 else "sum"
        for c in BL.columns
        if c not in keyListBL0
    }
    BL0 = BL.groupby(keyListBL0, as_index=False).agg(agg_key)
    calcBV = pd.concat([calcBV, BL0])
    calcBV.sort_values(["AGS", "Altersgruppe"], axis=0, inplace=True)
    calcBV.reset_index(inplace=True, drop=True)
    calcBV.drop(
        ["AnzFallM", "AnzFallW", "AnzFall100kM", "AnzFall100kW"], inplace=True, axis=1
    )
    ## now open stored Bevoelkerung.csv
    BV_csv_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "Bevoelkerung"
    )
    BV_csv_file = "Bevoelkerung.csv"
    BV_csv_full = os.path.join(BV_csv_path, BV_csv_file)
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
    BV = pd.read_csv(BV_csv_full, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
    maskBV = (BV["GueltigAb"] <= datenstand.date().strftime("%Y-%m-%d")) & (
        BV["GueltigBis"] >= datenstand.date().strftime("%Y-%m-%d")
    )
    BVAktuell = BV[maskBV].copy()
    BVAktuell.sort_values(["AGS", "Altersgruppe", "GueltigBis"], axis=0, inplace=True)
    BVAktuell.reset_index(drop=True, inplace=True)
    if BVAktuell.shape[0] != calcBV.shape[0]:
        print("Die Anzahl der Datensätze neu und BV sind unterschiedlich! ABRUCH!")
        sys.exit("unterschiedliche Anzahl von Datensätzen!")
    filtered = BV.merge(BVAktuell, how="outer", indicator=True).copy()
    oldEntrys = filtered[filtered["_merge"] == "left_only"].copy()
    oldEntrys.reset_index(drop=True, inplace=True)
    oldEntrys.drop(["_merge"], axis=1, inplace=True)
    calcBV.insert(loc=2, column="Name", value=BVAktuell["Name"])
    calcBV.insert(loc=3, column="GueltigAb", value=BVAktuell["GueltigAb"])
    calcBV.insert(loc=4, column="GueltigBis", value=BVAktuell["GueltigBis"])
    innerMerge = BVAktuell.merge(calcBV, how="inner").copy()
    if innerMerge.shape[0] < BVAktuell.shape[0]:
        outerMerge = BVAktuell.merge(calcBV, how="outer", indicator=True).copy()
        newRows = outerMerge[outerMerge["_merge"] == "right_only"].copy()
        newRows.drop(["_merge"], axis=1, inplace=True)
        newRows["GueltigAb"] = todayStr
        changedRows = outerMerge[outerMerge["_merge"] == "left_only"].copy()
        changedRows.drop(["_merge"], axis=1, inplace=True)
        changedRows["GueltigBis"] = yesterdayStr
        BVNew = pd.concat([innerMerge, newRows, changedRows, oldEntrys]).copy()
        BVNew.sort_values(["AGS", "Altersgruppe", "GueltigBis"], axis=0, inplace=True)
        BVNew.reset_index(drop=True, inplace=True)
        with open(BV_csv_full, "wb") as csvfile:
            BVNew.to_csv(
                csvfile,
                index=False,
                header=True,
                lineterminator="\n",
                encoding="utf-8",
                date_format="%Y-%m-%d",
                columns=BV_dtypes.keys(),
            )
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(aktuelleZeit, ": Bevoelkerung.csv geändert")
    else:
        aktuelleZeit = dt.datetime.now().strftime(format="%Y-%m-%dT%H:%M:%SZ")
        print(aktuelleZeit, ": Keine Veränderungen an den Einwohnerzahlen!")
except Exception as e:
    print(e)
