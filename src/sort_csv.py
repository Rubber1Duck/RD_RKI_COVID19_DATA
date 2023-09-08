import os
import pandas as pd

try:
    ## now open stored Bevoelkerung.csv
    path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..',
        'data')
    datafile = '2023-08-27_Deutschland_SarsCov2_Infektionen.csv'
    sort_csv_file = '2023-08-27_Deutschland_SarsCov2_Infektionen.sort.csv'
    fullpath_csv = os.path.join(path, datafile)
    sortpath = os.path.join(path, sort_csv_file)
    dtypes = {
        'IdLandkreis': 'str',
        'Altersgruppe': 'str',
        'Geschlecht': 'str',
        'Meldedatum': 'str',
        'Refdatum': 'str',
        'IstErkrankungsbeginn': 'Int32',
        'NeuerFall': 'int32',
        'NeuerTodesfall': 'Int32',
        'NeuGenesen': 'Int32',
        'AnzahlFall': 'Int32',
        'AnzahlTodesfall': 'Int32',
        'AnzahlGenesen': 'Int32'
    }
    DF = pd.read_csv(fullpath_csv, usecols=dtypes.keys(), dtype=dtypes)
    DF.sort_values(['IdLandkreis',
                    'Altersgruppe',
                    'Geschlecht',
                    'Meldedatum',
                    'Refdatum',
                    'IstErkrankungsbeginn',
                    'NeuerFall',
                    'NeuerTodesfall',
                    'NeuGenesen'], axis=0, inplace=True)
    DF.reset_index(drop=True, inplace=True)
    with open(sortpath, 'wb') as csvfile:
        DF.to_csv(
            csvfile,
            index=False,
            header=True,
            lineterminator='\n',
            encoding='utf-8',
            date_format='%Y-%m-%d',
            columns=dtypes.keys())
except Exception as e:
    print(e)
