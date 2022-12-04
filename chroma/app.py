import chroma
from chroma.server.fastapi import FastAPI
settings = chroma.config.Settings()
server = FastAPI(settings)
app = server.app()
