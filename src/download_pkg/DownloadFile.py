import requests, os, lzma
from datetime import datetime
from shutil import copyfile
import pytz

def get_root_directory():
    os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', '..',)

class DownloadFile():
    def __init__(self,url,filename,download_path,compress=True,add_date=True,add_latest=False, verbose=True):
        self.url=url
        self.filename=filename
        self.download_path=os.path.normpath(download_path)
        self.compress=compress
        self.add_date=add_date
        self.add_latest=add_latest
        self._file_name_root, self._file_extension =os.path.splitext(self.filename)
        self.content=self.get_content()
        self.full_path = self.get_full_path()
        self.full_path_latest=self.get_full_path_latest()
        self.verbose=verbose

    def get_full_path(self):
        path = os.path.join(self.download_path,self._file_name_root)
        if self.add_date:
            DATE_STR = datetime.now(pytz.timezone('Europe/Berlin')).date().strftime('%Y-%m-%d')
            path=path+"_"+DATE_STR
        path = path+self._file_extension
        if self.compress:
            path = path+".xz"
        return path

    def get_full_path_latest(self):
        path = os.path.join(self.download_path,self._file_name_root+"_latest"+self._file_extension)
        if self.compress:
            path = path + ".xz"
        return path

    def get_content(self):
        headers = {'Pragma': 'no-cache', 'Cache-Control': 'no-cache'}
        r = requests.get(self.url, headers=headers, allow_redirects=True, timeout=10.0)
        if r.status_code != 200:
            if self.verbose:
                print("Download failed!")
            raise ValueError(f'Download failed! File {self.full_path} was not created!')
        else:
            return r.content
        
    def write_file(self):
        if self.compress:
            with lzma.open(self.full_path, 'wb') as file:
                file.write(self.content)
                file.close()
        else:
            with open(self.full_path, 'wb') as file:
                file.write(self.content)
                file.close()
        print(f"File {self.full_path} created.")
        if self.add_latest:
            copyfile(self.full_path, self.full_path_latest)
            print(f"File {self.full_path_latest} created.")