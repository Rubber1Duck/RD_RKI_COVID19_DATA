import os
import pandas as pd
from pandas.api.types import CategoricalDtype


def squeeze_dataframe(df):
    # ----- Get columns in dataframe
    cols = dict(df.dtypes)

    # ----- Check each column's type downcast or categorize as appropriate
    for col, type in cols.items():
        if type == "float64":
            df[col] = pd.to_numeric(df[col], downcast="float")
        elif type == "int64":
            df[col] = pd.to_numeric(df[col], downcast="integer")
        elif type == "object":
            df[col] = df[col].astype(CategoricalDtype(ordered=True))

    return df


def write_file(df, fn, compression="", sheet_name="data"):
    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == ".csv":
        df.to_csv(fn, index=False)

    elif fn_ext == ".feather":
        compression = "zstd" if compression == "" else compression
        df.to_feather(fn, compression=compression)

    else:
        print("oopsy in write_file()! File extension unknown:", fn_ext)
        quit(0)

    return


def write_json(df, fn, pt):
    full_fn = os.path.join(pt, fn)
    df.to_json(
        path_or_buf=full_fn,
        orient="records",
        date_format="iso",
        force_ascii=False,
        compression="infer",
    )

    return


def read_json(fn, dtype, path=""):
    full_fn = os.path.join(path, fn)
    df = pd.read_json(full_fn, dtype=dtype)

    return df


def read_file(fn):
    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == ".csv":
        df = pd.read_csv(fn, keep_default_na=False)

    elif fn_ext == ".feather":
        df = pd.read_feather(fn)

    else:
        print("oopsy in read_file()! File extension unknown:", fn_ext)
        quit(0)

    return df


def calc_incidence_BL(df, unique_ID):
    Region_I = pd.DataFrame()
    for id in unique_ID:
        RegionID = df[df["IdBundesland"] == id].copy()
        RegionID.drop(["Bundesland", "deaths", "recovered"], inplace=True, axis=1)
        indexes = RegionID.index.to_list()
        for index in indexes:
            cases7d = 0
            actual_index_of_index = indexes.index(index)
            for x in range(0, 7):
                if (actual_index_of_index - x) < 0:
                    continue
                cases7d += RegionID.at[indexes[actual_index_of_index - x], "cases"]
            RegionID.at[index, "cases7d"] = cases7d
        RegionID["incidence7d"] = RegionID["cases7d"] / RegionID["Einwohner"] * 100000
        Region_I = pd.concat([Region_I, RegionID])
    return Region_I

def calc_incidence_LK(df, unique_ID):
    Region_I = pd.DataFrame()
    for id in unique_ID:
        RegionID = df[df["IdLandkreis"] == id].copy()
        RegionID.drop(["Landkreis", "deaths", "recovered"], inplace=True, axis=1)
        indexes = RegionID.index.to_list()
        for index in indexes:
            cases7d = 0
            actual_index_of_index = indexes.index(index)
            for x in range(0, 7):
                if (actual_index_of_index - x) < 0:
                    continue
                cases7d += RegionID.at[indexes[actual_index_of_index - x], "cases"]
            RegionID.at[index, "cases7d"] = cases7d
        RegionID["incidence7d"] = RegionID["cases7d"] / RegionID["Einwohner"] * 100000
        Region_I = pd.concat([Region_I, RegionID])
    return Region_I