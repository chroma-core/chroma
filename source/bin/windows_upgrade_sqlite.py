import httpx
import zipfile
import io
import os
import sys
import shutil
import time
from typing import Optional

# Used by Github Action runners to upgrade sqlite version to 3.42.0
DLL_URL = "https://www.sqlite.org/2023/sqlite-dll-win64-x64-3420000.zip"
DOWNLOAD_TIMEOUT_SECONDS = 30
DOWNLOAD_RETRIES = 5


def download_sqlite_dll_zip() -> bytes:
    last_error: Optional[httpx.HTTPError] = None
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            response = httpx.get(
                DLL_URL,
                follow_redirects=True,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            return bytes(response.content)
        except httpx.HTTPError as exc:
            last_error = exc
            if attempt == DOWNLOAD_RETRIES:
                raise
            print(
                f"Download attempt {attempt} failed: {exc}. Retrying...",
                flush=True,
            )
            time.sleep(attempt)
    raise RuntimeError("failed to download sqlite DLL") from last_error


if __name__ == "__main__":
    # Download and extract the DLL
    z = zipfile.ZipFile(io.BytesIO(download_sqlite_dll_zip()))
    z.extractall(".")
    # Print current Python path
    exec_path = os.path.dirname(sys.executable)
    dlls_path = os.path.join(exec_path, "DLLs")
    # Copy the DLL to the Python DLLs folder
    shutil.copy("sqlite3.dll", dlls_path)
