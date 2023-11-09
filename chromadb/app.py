import chromadb
import chromadb.config
from chromadb.server.fastapi import FastAPI

settings = chromadb.config.Settings(persist_directory="/storage")
server = FastAPI(settings)
app = server.app()
