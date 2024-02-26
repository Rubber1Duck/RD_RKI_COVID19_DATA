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


def write_file(df, fn, compression=""):
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


def write_json(df, fn, pt, Datenstand="", archivePath=""):
    full_fn = os.path.join(pt, fn)
    if archivePath != "":
        full_archiv_fn = os.path.join(archivePath, Datenstand + "_" + fn)
        try:
            os.rename(full_fn, full_archiv_fn)
        except:
            print(full_fn, "does not exists!")

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


def calc_incidence(df):
    idIndexes = df.index.to_list()
    for index in idIndexes:
        indexPos = idIndexes.index(index)
        cases7d = 0
        for x in [y for y in range(0, 7) if indexPos - y >=0]:
            cases7d += df.at[idIndexes[indexPos - x], "cases"]
        df.at[index, "cases7d"] = cases7d
        df.at[index, "incidence7d"] = (cases7d / df.at[index, "Einwohner"] * 100000).round(5)
    df["cases7d"] = df["cases7d"].astype(int)
    return df

def copy(source, destination):
   with open(source, 'rb') as file:
       myFile = file.read()
   with open(destination, 'wb') as file:
       file.write(myFile)

def get_different_rows(source_df, new_df):
    """Returns just the rows from the new dataframe that differ from the source dataframe"""
    merged_df = source_df.merge(new_df, indicator=True, how='outer')
    changed_rows_df = merged_df[merged_df['_merge'] == 'right_only']
    return changed_rows_df.drop('_merge', axis=1)