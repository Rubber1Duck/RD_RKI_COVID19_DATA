import os
import re
import datetime as dt
import numpy as np
import pandas as pd
import utils as ut

startTime = dt.datetime.now()
kum_file_LK = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'frozen-incidence',
    'LK_init.csv.xz'
)
kum_file_BL = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'frozen-incidence',
    'BL_init.csv.xz'
)
BV_csv_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'Bevoelkerung',
    'Bevoelkerung.csv'
)
LK_dtypes = {
    'Datenstand': 'object',
    'IdLandkreis': 'str',
    'Landkreis': 'str',
    'AnzahlFall_7d': 'Int32',
    'incidence_7d': 'float64'
}
BL_dtypes = {
    'Datenstand': 'object',
    'IdBundesland': 'str',
    'Bundesland': 'str',
    'AnzahlFall_7d': 'Int32',
    'incidence_7d': 'float64'
}
kum_dtypes = {
    'D': 'object',
    'I': 'str',
    'T': 'str',
    'A': 'Int32',
    'i': 'float64'
}
BV_dtypes = {
    'AGS': 'str',
    'Altersgruppe': 'str',
    'Name': 'str',
    'GueltigAb': 'object',
    'GueltigBis': 'object',
    'Einwohner': 'Int32',
    'männlich': 'Int32',
    'weiblich': 'Int32'
}
CV_dtypes = {
    'IdLandkreis': 'str',
    'NeuerFall': 'Int32',
    'AnzahlFall': 'Int32',
    'Meldedatum': 'object'
}
path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..',
    'dataStore',
    'frozen-incidence'
)

# open bevoelkerung.csv
BV = pd.read_csv(BV_csv_path, usecols=BV_dtypes.keys(), dtype=BV_dtypes)
BV['GueltigAb'] = pd.to_datetime(BV['GueltigAb'])
BV['GueltigBis'] = pd.to_datetime(BV['GueltigBis'])

#----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
BV = ut.squeeze_dataframe(BV)

# step througt all data file
data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data')
data_path = os.path.normpath(data_path)
iso_date_re = '([0-9]{4})(-?)(1[0-2]|0[1-9])\\2(3[01]|0[1-9]|[12][0-9])'
file_list = os.listdir(data_path)
file_list.sort(reverse=False)
pattern = 'RKI_COVID19'
bytes_total = 0
all_data_files = []
for file in file_list:
    file_path_full = os.path.join(data_path, file)
    if not os.path.isdir(file_path_full):
        filename = os.path.basename(file)
        re_filename = re.search(pattern, filename)
        re_search = re.search(iso_date_re, filename)
        if re_search and re_filename:
            report_date = dt.date(int(re_search.group(1)), int(re_search.group(3)), int(re_search.group(4))).strftime('%Y-%m-%d')
            file_size = os.path.getsize(file_path_full)
            all_data_files.append((file, file_size, file_path_full, report_date))
            bytes_total += file_size

# open existing kum files
LK_kum = pd.read_csv(kum_file_LK, usecols=kum_dtypes.keys(), dtype=kum_dtypes)
BL_kum = pd.read_csv(kum_file_BL, usecols=kum_dtypes.keys(), dtype=kum_dtypes)
LK_kum['D'] = pd.to_datetime(LK_kum['D']).dt.date
BL_kum['D'] = pd.to_datetime(BL_kum['D']).dt.date
keys_LK_kum = ['D', 'I']
keys_BL_kum = ['D', 'I']
bytes_prozessed = 0

for file, file_size, file_path_full, report_date in all_data_files:
    start_file_time = dt.datetime.now()
    LK = pd.read_csv(file_path_full, usecols=CV_dtypes.keys(), dtype=CV_dtypes)
    Datenstand = dt.datetime.strptime(report_date, '%Y-%m-%d')
    LK.sort_values(by=['IdLandkreis', 'Meldedatum'], axis=0, inplace=True, ignore_index=True)
    LK.reset_index(drop=True, inplace=True)

    #----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)

    LK['IdLandkreis'] = LK['IdLandkreis'].astype(str).str.zfill(5)
    LK.insert(loc=0, column='IdBundesland', value=LK['IdLandkreis'].str[:-3].copy())
    LK['Meldedatum'] = pd.to_datetime(LK['Meldedatum']).dt.date
    LK.insert(loc=0, column='Datenstand', value= Datenstand.date())
    LK.insert(loc=0, column='IdStaat', value= '00')

    #----- Squeeze the dataframe to ideal memory size (see "compressing" Medium article and run_dataframe_squeeze.py for background)
    LK = ut.squeeze_dataframe(LK)

    # *******************
    # * fixed-incidence *
    # *******************
        
    # used keylists
    keys_LK_fix = ['IdStaat', 'IdBundesland', 'IdLandkreis' ]
    keys_BL_fix = ['IdStaat', 'IdBundesland']
    keys_ID0_fix = ['IdStaat']

    LK['AnzahlFall'] = np.where(LK['NeuerFall'].isin([0, 1]), LK['AnzahlFall'], 0).astype(int)
    LK['AnzahlFall_7d'] = np.where(LK['Meldedatum'] > (Datenstand.date() - dt.timedelta(days=8)), LK['AnzahlFall'], 0).astype(int)
    LK.drop([
        'Meldedatum',
        'NeuerFall',
        'AnzahlFall'], inplace=True, axis=1
    )
    agg_key = {
        c: 'max' if c in ['Datenstand'] else 'sum'
        for c in LK.columns
        if c not in keys_LK_fix
    }
    LK = LK.groupby(by=keys_LK_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: 'max' if c in ['IdLandkreis', 'Datenstand'] else 'sum'
        for c in LK.columns
        if c not in keys_BL_fix
    }
    BL = LK.groupby(by=keys_BL_fix, as_index=False, observed=True).agg(agg_key)
    agg_key = {
        c: 'max' if c in ['IdBundesland', 'IdLandkreis', 'Datenstand'] else 'sum'
        for c in BL.columns
        if c not in keys_ID0_fix
    }
    ID0 = BL.groupby(by=keys_ID0_fix, as_index=False, observed=True).agg(agg_key)
    LK.drop(['IdStaat', 'IdBundesland'], inplace=True, axis=1)
    BL.drop(['IdStaat', 'IdLandkreis'], inplace=True, axis=1)
    ID0.drop(['IdStaat', 'IdLandkreis'], inplace=True, axis=1)
    ID0['IdBundesland'] = '00'
    BL = pd.concat([ID0, BL])
    BL.reset_index(inplace=True, drop=True)
    LK_pop_mask = (
        (BV['AGS'].isin(LK['IdLandkreis'])) &
        (BV['Altersgruppe'] == "A00+") &
        (BV['GueltigAb'] <= Datenstand) &
        (BV['GueltigBis'] >= Datenstand)
    )
    LK_pop = BV[LK_pop_mask]
    LK_pop.reset_index(inplace=True, drop=True)
    LK['population'] = LK_pop['Einwohner']
    LK.insert(loc=0, column='Landkreis', value=LK_pop['Name'])
    LK['AnzahlFall_7d'] = LK['AnzahlFall_7d'].astype(int)
    LK['incidence_7d'] = LK['AnzahlFall_7d'] / LK['population'] * 100000
    LK.drop(['population'], inplace=True, axis=1)
    BL_pop_mask = (
        (BV['AGS'].isin(BL['IdBundesland'])) &
        (BV['Altersgruppe'] == "A00+") &
        (BV['GueltigAb'] <= Datenstand) &
        (BV['GueltigBis'] >= Datenstand)
    )
    BL_pop = BV[BL_pop_mask]
    BL_pop.reset_index(inplace=True, drop=True)
    BL['population'] = BL_pop['Einwohner']
    BL.insert(loc=0, column='Bundesland', value=BL_pop['Name'])
    BL['AnzahlFall_7d'] = BL['AnzahlFall_7d'].astype(int)
    BL['incidence_7d'] = BL['AnzahlFall_7d'] / BL['population'] * 100000
    BL.drop(['population'], inplace=True, axis=1)

    # rename columns for shorter json files
    LK.rename(columns={
        'Datenstand': 'D',
        'IdLandkreis': 'I',
        'Landkreis': 'T',
        'AnzahlFall_7d': 'A',
        'incidence_7d': 'i'
        }, inplace=True)
    BL.rename(columns={
        'Datenstand': 'D',
        'IdBundesland': 'I',
        'Bundesland': 'T',
        'AnzahlFall_7d': 'A',
        'incidence_7d': 'i'
        }, inplace=True)
    
    Datenstand2 = Datenstand.date()
    LK_kum = LK_kum[LK_kum['D'] != Datenstand2]
    BL_kum = BL_kum[BL_kum['D'] != Datenstand2]
    LK['D'] = pd.to_datetime(LK['D']).dt.date
    BL['D'] = pd.to_datetime(BL['D']).dt.date
    LK_kum = pd.concat([LK_kum, LK])
    LK_kum.sort_values(by=keys_LK_kum, inplace=True)
    BL_kum = pd.concat([BL_kum, BL])
    BL_kum.sort_values(by=keys_BL_kum, inplace=True)
    aktuelleZeit = dt.datetime.now().strftime(format='%Y-%m-%dT%H:%M:%SZ')
    endtimeforfile = dt.datetime.now()
    time_used_for_file = endtimeforfile - start_file_time
    bytes_prozessed += file_size
    print(
        aktuelleZeit,
        ":",
        file,
        "done. prozessing time file:",
        time_used_for_file,
        "prozessed bytes:",
        bytes_prozessed,
        "/",
        bytes_total,
        "=",
        round(bytes_prozessed/bytes_total * 100, 4),
        "%"
    )

LK_kum.to_csv(
    path_or_buf=kum_file_LK,
    index=False,
    header=True,
    lineterminator='\n',
    encoding='utf-8',
    date_format='%Y-%m-%d',
    columns=kum_dtypes.keys(),
    compression='infer'
)
    
LK_json_path = os.path.join(
    path,
    'LK_init.json.xz'
)
    
LK_kum.to_json(
    path_or_buf=LK_json_path,
    orient='records',
    date_format='iso',
    force_ascii=False,
    compression='infer'
)
    
BL_kum.to_csv(
    path_or_buf=kum_file_BL,
    index=False,
    header=True,
    lineterminator='\n',
    encoding='utf-8',
    date_format='%Y-%m-%d',
    columns=kum_dtypes.keys(),
    compression='infer'
)
    
BL_json_path = os.path.join(
    path,
    'BL_init.json.xz'
)

BL_kum.to_json(
    path_or_buf=BL_json_path,
    orient='records',
    date_format='iso',
    force_ascii=False,
    compression='infer'
)

endTime = dt.datetime.now()
aktuelleZeit = endTime.strftime(format='%Y-%m-%dT%H:%M:%SZ')
print(aktuelleZeit, ": total time:", endTime - startTime)
