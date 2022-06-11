from download_pkg import *
import os
import json

# %% each day
try:
    meta_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dataStore', 'meta')
    filename_meta = "meta.json" 
    url_meta = "https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74?f=json"

    meta = DownloadFile(url=url_meta, filename=filename_meta, download_path=meta_path)
    meta.write_file()
    
    needed =["created", "modified", "name", "size"]

    with open(meta_path + "/" + filename_meta, 'r', encoding ='utf8') as file:
        metaObj = json.load(file)
    newObj = metaObj.copy()
    for property in newObj:
        if property not in needed:
            del metaObj[property]

    with open(meta_path + "/" + filename_meta, 'w', encoding ='utf8') as json_file:
        json.dump(metaObj, json_file, ensure_ascii = False)
except Exception as e:
    print(e)