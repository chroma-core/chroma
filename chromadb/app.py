import chromadb.config
from chromadb.server.fastapi import FastAPI
from chromadb.utils.client_utils import _upgrade_check

_upgrade_check()
settings = chromadb.config.Settings()
server = FastAPI(settings)
app = server.app()
