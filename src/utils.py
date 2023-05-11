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

def panda_mem_usage(df, detail='full'):

    dtypes = df.dtypes.reset_index(drop=False)
    memory = df.memory_usage(deep=True).reset_index(drop=False)

    df1 = pd.merge(dtypes, memory, on='index')
    df1 = df1.rename(columns = {'index': 'col', '0_x': 'type', '0_y': 'bytes'})
    total = df1['bytes'].sum()

    objects = df.select_dtypes(include=['object', 'category'])
    df_objs = objects.select_dtypes(include=['object', 'category']).describe().T.reset_index()

    if detail == 'full':
        print('')
        print('{:<15} {:<15} {:>15} {:>8} {:>8}'.format('Column', 'Data Type', 'Bytes', 'MBs', 'GBs'))
        print('{} {} {} {} {}'.format('-'*15, '-'*15, '-'*15, '-'*8, '-'*8))

        for index, row in df1.iterrows():
            print('{:<15} {:<15} {:>15,.0f} {:>8,.1f} {:>8,.2f}'.format(row['col'], str(row['type']), row['bytes'], row['bytes']/1024**2, row['bytes']/1024**3))

        print('\nTotal: {:,.0f} Rows, {:,.0f} Bytes, {:,.1f} MBs, {:,.2f} GBs\n'.format(len(df), total, total/1024**2, total/1024**3))

        print('{:<15} {:>13} {:>13}'.format('Column', 'Count', 'Unique'))
        print('{} {} {}'.format('-'*15, '-'*13, '-'*13))
        for index, row in df_objs.iterrows():
            print('{:<15} {:>13,.0f} {:>13,.0f}'.format(row['index'], row['count'], row['unique']))

    elif detail == 'return_short':
        return len(df), total

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
        writer.save()

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
