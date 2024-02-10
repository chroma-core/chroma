from chromadb.telemetry.opentelemetry import get_otel_client

get_otel_client()  # noqa: F401 - make sure Telemetry client is loaded early
import chromadb.config
from chromadb.server.fastapi import FastAPI

settings = chromadb.config.Settings()

server = FastAPI(settings)
app = server.app()
