import chromadb
import chromadb.config
from chromadb.server.fastapi.v1 import FastAPIWithV1

settings = chromadb.config.Settings()
server = FastAPIWithV1(settings)
app = server.app()
