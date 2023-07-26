import requests
import zipfile
import io
import os
import sys

# Used by Github Action runners to upgrade sqlite version to 3.42.0

DLL_URL = "https://www.sqlite.org/2023/sqlite-dll-win64-x64-3420000.zip"

if __name__ == "__main__":
    # Download and extract the DLL

    r = requests.get(DLL_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(".")
    # Print current DLL path
    exec_path = os.path.dirname(sys.executable)
    print(exec_path)
    # Print contents of folder
    print(os.listdir(exec_path))
