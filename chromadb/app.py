import chromadb
import chromadb.config
from chromadb.server.fastapi import FastAPI
from fastapi import Request, HTTPException

settings = chromadb.config.Settings()
server = FastAPI(settings)
app = server.app()


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    if request.headers.get(settings.auth_header_name) != settings.auth_token:
        raise HTTPException(status_code=401, detail="Unauthorized")
    response = await call_next(request)
    return response
