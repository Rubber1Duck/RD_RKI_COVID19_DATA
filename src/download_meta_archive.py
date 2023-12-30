from download_pkg import *
import os, requests, json
import datetime as dt

try:
    meta_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "dataStore", "meta"
    )
    filename_meta = "meta_new.json"
    today = dt.date.today().strftime("%Y-%m-%d")
    url_meta = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/raw/main/Metadaten/zenodo.json"
    url_data = (
        "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland_Archiv/raw/main/Archiv/"
        + today
        + "_Deutschland_SarsCov2_Infektionen.csv.xz"
    )
    filename = today + "_Deutschland_SarsCov2_Infektionen.csv.xz"
    resp = requests.head(url_data, allow_redirects=True)
    size = resp.headers["content-length"]

    meta = DownloadFile(url=url_meta, filename=filename_meta, download_path=meta_path)
    meta.write_file()

    needed = ["publication_date", "version"]

    with open(meta_path + "/" + filename_meta, "r", encoding="utf8") as file:
        metaObj = json.load(file)
    newObj = metaObj.copy()
    for prop in newObj:
        if prop not in needed:
            del metaObj[prop]
    metaObj["size"] = size
    metaObj["filename"] = filename
    metaObj["url"] = url_data
    pubDate = metaObj["publication_date"]
    pubDateTz = dt.datetime.fromisoformat(pubDate)
    pubDateTs = dt.datetime.timestamp(pubDateTz)
    modified = int(pubDateTs) * 1000
    metaObj["modified"] = modified
    with open(meta_path + "/" + filename_meta, "w", encoding="utf8") as json_file:
        json.dump(metaObj, json_file, ensure_ascii=False)
except Exception as e:
    print(e)
