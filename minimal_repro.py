import chromadb
import sys
import os
import uvicorn
from chromadb.api import API
import chromadb.server.fastapi
from chromadb.config import Settings
import tempfile
import random
from multiprocessing import Process
import logging
import time

logger = logging.getLogger(__name__)


def _run_server(port: int) -> None:
    """Run a Chroma server locally"""
    sys.stdout = open(str(os.getpid()) + ".out", "a")
    sys.stderr = open(str(os.getpid()) + "_error.out", "a")
    persist_directory = (
        tempfile.gettempdir() + "/test_server" + str(random.randint(0, 100000))
    )
    settings = Settings(
        chroma_api_impl="local",
        chroma_db_impl="duckdb",
        persist_directory=persist_directory,
    )
    server = chromadb.server.fastapi.FastAPI(settings)
    uvicorn.run(server.app(), host="0.0.0.0", port=port, log_level="info")


def _await_server(api: API, attempts: int = 0) -> None:
    try:
        api.heartbeat()
    except Exception as e:
        if attempts > 15:
            print("Test server failed to start after 15 attempts")
            raise e
        else:
            print("Waiting for server to start...")
            time.sleep(4)
            _await_server(api, attempts + 1)


if __name__ == "__main__":
    port = 6666
    proc = Process(target=_run_server, args=(port,), daemon=True)
    proc.start()
    api = chromadb.Client(
        Settings(
            chroma_api_impl="rest",
            chroma_server_host="localhost",
            chroma_server_http_port=str(port),
        )
    )
    _await_server(api)
