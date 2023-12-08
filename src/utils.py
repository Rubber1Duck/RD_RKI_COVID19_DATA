import os
import pandas as pd
from pandas.api.types import CategoricalDtype

def squeeze_dataframe(df):

    #----- Get columns in dataframe
    cols = dict(df.dtypes)

    #----- Check each column's type downcast or categorize as appropriate
    for col, type in cols.items():
        if type == 'float64':
            df[col] = pd.to_numeric(df[col], downcast='float')
        elif type == 'int64':
            df[col] = pd.to_numeric(df[col], downcast='integer')
        elif type == 'object':
            df[col] = df[col].astype(CategoricalDtype(ordered=True))

    return df

def write_file(df, fn, compression='', sheet_name='data'):

    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == '.csv':
        df.to_csv(fn, index=False)

    elif fn_ext == '.feather':
        compression = 'zstd' if compression == '' else compression
        df.to_feather(fn, compression=compression)

    else:
        print('oopsy in write_file()! File extension unknown:', fn_ext)
        quit(0)

    return

def write_json(df, fn, pt):

    fullPath = os.path.join(pt, fn)
    df.to_json(path_or_buf=fullPath, orient="records", date_format="iso", force_ascii=False, compression='infer')

    return


def read_file(fn, sheet_name='data'):

    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == '.csv':
        df = pd.read_csv(fn, keep_default_na=False)

    elif fn_ext == '.feather':
        df = pd.read_feather(fn)

    else:
        print('oopsy in read_file()! File extension unknown:', fn_ext)
        quit(0)

    return df
