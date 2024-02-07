import chromadb
import chromadb.config
from chromadb.server.fastapi import FastAPI

settings = chromadb.config.Settings()
settings.chroma_server_backend_impl = chromadb.config.get_fqn(FastAPI)
server = FastAPI(settings)
app = server.app()
