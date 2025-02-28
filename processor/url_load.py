"""
This script loads the dataset
from the specified url.
"""
from io import BytesIO
from zipfile import ZipFile
import requests

print("Download Starting!!!")

URL = "https://archive.ics.uci.edu/static/public/502/online+retail+ii.zip"
res = requests.get(URL, timeout=20)

if res.status_code == 200:
    with ZipFile(BytesIO(res.content)) as zip_file:
        zip_file.extractall()
        print('File Download Complete!!!')
        print('File extracted successfully.')
else:
    print(f"Failed to download file: {res.status_code}")
