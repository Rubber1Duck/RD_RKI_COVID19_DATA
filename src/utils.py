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

    elif fn_ext == '.xlsx':
        writer = pd.ExcelWriter(fn, engine='xlsxwriter',)
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        #add more sheets by repeating df.to_excel() and change sheet_name
        writer.close()

    else:
        print('oopsy in write_file()! File extension unknown:', fn_ext)
        quit(0)

    return


def read_file(fn, sheet_name='data'):

    fn_ext = os.path.splitext(fn)[1]

    if fn_ext == '.csv':
        df = pd.read_csv(fn, keep_default_na=False)

    elif fn_ext == '.feather':
        df = pd.read_feather(fn)

    elif fn_ext == '.xlsx':
        df = pd.read_excel(fn, sheet_name=sheet_name, keep_default_na=False)

    else:
        print('oopsy in read_file()! File extension unknown:', fn_ext)
        quit(0)

    return df
