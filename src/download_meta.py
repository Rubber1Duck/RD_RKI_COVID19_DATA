from download_pkg import *
import os, requests, json
import datetime as dt

try:
    meta_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "..", "dataStore", "meta"
    )
    filename_meta = "meta_new.json"
    today = dt.date.today().strftime("%Y-%m-%d")
    url_meta = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/raw/main/Metadaten/zenodo.json"
    url_data = "https://github.com/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/raw/main/Aktuell_Deutschland_SarsCov2_Infektionen.csv"
    url_api = "https://api.github.com/repos/robert-koch-institut/SARS-CoV-2-Infektionen_in_Deutschland/commits/main"

    # get last commit json
    api_resp = requests.get(url=url_api, allow_redirects=True)
    api_Object = json.loads(api_resp.content.decode(encoding="utf8"))

    # get filesize
    filename = "Aktuell_Deutschland_SarsCov2_Infektionen.csv"
    resp = requests.head(url_data, allow_redirects=True)
    size = resp.headers["content-length"]

    # get zenodo.json
    meta_resp = requests.get(url=url_meta, allow_redirects=True)
    metaObj = json.loads(meta_resp.content.decode(encoding="utf8"))

    # delete all what is not needed from metaObj
    needed = ["publication_date", "version"]
    newObj = metaObj.copy()
    for prop in newObj:
        if prop not in needed:
            del metaObj[prop]
    # insert all needed stuff
    metaObj["size"] = size  # size of the file to download
    metaObj["filename"] = filename  # filename of the file to download
    metaObj["url"] = url_data  # url of the file to download
    pubDate = api_Object["commit"]["committer"]["date"]  # last commit date
    pubDateTz = dt.datetime.strptime(pubDate, "%Y-%m-%dT%H:%M:%Sz")
    pubDateTs = dt.datetime.timestamp(pubDateTz)
    modified = int(pubDateTs) * 1000
    metaObj["modified"] = modified  # modified date timestamp
    # store to disc
    with open(meta_path + "/" + filename_meta, "w", encoding="utf8") as json_file:
        json.dump(metaObj, json_file, ensure_ascii=False)
except Exception as e:
    print(e)
