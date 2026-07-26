import os
import requests
import json
import sys
import datetime as dt

try:
    meta_path = os.path.normpath(
        os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "dataStore", "meta"
        )
    )
    filename_meta_old = "meta.json"
    filename_meta_new = "meta_new.json"
    date = dt.datetime.fromisoformat(sys.argv[1])
    date_only_str = date.strftime("%Y-%m-%d")

    filename = date_only_str + "_Deutschland_SarsCov2_Infektionen.csv.xz"
    url_data = (
        "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/raw/main/Archiv/"
        + filename
    )

    resp = requests.head(url_data, allow_redirects=True)
    size = resp.headers["content-length"]

    with open(meta_path + "/" + filename_meta_old, "r", encoding="utf8") as file:
        metaObj = json.load(file)

    metaObj["publication_date"] = sys.argv[1]
    metaObj["version"] = date_only_str
    metaObj["size"] = size
    metaObj["filename"] = filename
    metaObj["url"] = url_data
    modified = int(dt.datetime.timestamp(date)) * 1000
    metaObj["modified"] = modified

    with open(meta_path + "/" + filename_meta_new, "w", encoding="utf8") as json_file:
        json.dump(metaObj, json_file, ensure_ascii=False)

except Exception as e:
    print(e)
