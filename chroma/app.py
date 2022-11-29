import chroma
from chroma.server.fastapi import FastAPI

server = FastAPI(chroma.get_settings())
app = server.app()

