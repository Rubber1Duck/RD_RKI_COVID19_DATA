#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from . import DownloadFile
import os

def download_RKI_COVID19():
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data')
    filename = "RKI_COVID19.csv"
    filename_meta = "meta.json" 
    url = "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data"

    url_meta = "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json"

    meta = DownloadFile(url=url_meta, filename=filename_meta, download_path=data_path, compress=False, add_date=False, add_latest=False)
    meta.write_file()
    a = DownloadFile(url=url, filename=filename, download_path=data_path, compress=True, add_date=True, add_latest=False)
    a.write_file()
