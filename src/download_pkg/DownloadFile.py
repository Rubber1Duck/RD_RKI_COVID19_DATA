import requests, os


class DownloadFile:
    def __init__(self, url, filename, download_path):
        self.url = url
        self.filename = filename
        self.download_path = os.path.normpath(download_path)
        self._file_name_root, self._file_extension = os.path.splitext(self.filename)
        self.content = self.get_content()
        self.full_path = self.get_full_path()

    def get_full_path(self):
        path = os.path.join(self.download_path, self._file_name_root)
        path = path + self._file_extension
        return path

    def get_content(self):
        headers = {"Pragma": "no-cache", "Cache-Control": "no-cache"}
        r = requests.get(self.url, headers=headers, allow_redirects=True, timeout=10.0)
        if r.status_code != 200:
            if self.verbose:
                print("Download failed!")
            raise ValueError(f"Download failed! File {self.full_path} was not created!")
        else:
            return r.content

    def write_file(self):
        with open(self.full_path, "wb") as file:
            file.write(self.content)
            file.close()
