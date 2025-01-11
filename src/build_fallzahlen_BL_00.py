import os
import pandas as pd
import utils as ut

path_fallzahlen = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "Fallzahlen",
    "RKI_COVID19_Fallzahlen.csv",
)
path_fallzahlen_BL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "Fallzahlen",
    "RKI_COVID19_Fallzahlen_BL.csv",
)
path_fallzahlen_00 = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..",
    "Fallzahlen",
    "RKI_COVID19_Fallzahlen_00.csv",
)

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

key_list_BL = ["Datenstand", "IdBundesland"]
key_list_00 = ["Datenstand"]

fallzahlen_df = pd.read_csv(
    path_fallzahlen,
    engine="pyarrow",
    usecols=dtypes_fallzahlen.keys(),
    dtype=dtypes_fallzahlen,
)
ut.squeeze_dataframe(fallzahlen_df)
fallzahlen_df.drop("IdLandkreis", axis=1, inplace=True)

agg_key = {
    c: "max" if c in ["report_date", "meldedatum_max"] else "sum"
    for c in fallzahlen_df.columns
    if c not in key_list_BL
}
fallzahlen_BL = fallzahlen_df.groupby(key_list_BL, as_index=False, observed=True).agg(
    agg_key
)

fallzahlen_df.drop("IdBundesland", axis=1, inplace=True)
agg_key = {
    c: "max" if c in ["report_date", "meldedatum_max"] else "sum"
    for c in fallzahlen_df.columns
    if c not in key_list_00
}
fallzahlen_00 = fallzahlen_df.groupby(key_list_00, as_index=False, observed=True).agg(
    agg_key
)

ut.write_file(fallzahlen_BL, path_fallzahlen_BL)
ut.write_file(fallzahlen_00, path_fallzahlen_00)
