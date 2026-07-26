import os
import pandas as pd

try:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "temp")
    dtypes = {
        "IdLandkreis": "str",
        "Altersgruppe": "str",
        "Geschlecht": "str",
        "Meldedatum": "str",
        "Refdatum": "str",
        "IstErkrankungsbeginn": "Int32",
        "NeuerFall": "int32",
        "NeuerTodesfall": "Int32",
        "NeuGenesen": "Int32",
        "AnzahlFall": "Int32",
        "AnzahlTodesfall": "Int32",
        "AnzahlGenesen": "Int32",
    }
    file_list = os.listdir(path)
    file_list.sort(reverse=False)
    for file in file_list:
        datafile = file
        sort_csv_file = file
        csvpath = os.path.join(path, datafile)
        sortpath = os.path.join(path, sort_csv_file)
        oldSize = os.path.getsize(csvpath)
        DF = pd.read_csv(csvpath, usecols=dtypes.keys(), dtype=dtypes)
        DF.sort_values(
            [
                "IdLandkreis",
                "Altersgruppe",
                "Geschlecht",
                "Meldedatum",
                "Refdatum",
                "IstErkrankungsbeginn",
                "NeuerFall",
                "NeuerTodesfall",
                "NeuGenesen",
            ],
            axis=0,
            inplace=True,
        )
        DF.reset_index(drop=True, inplace=True)
        with open(sortpath, "wb") as csvfile:
            DF.to_csv(
                csvfile,
                index=False,
                header=True,
                lineterminator="\n",
                encoding="utf-8",
                date_format="%Y-%m-%d",
                columns=dtypes.keys(),
            )
        newSize = os.path.getsize(sortpath)
        print(oldSize, newSize, newSize / oldSize)
except Exception as e:
    print(e)
