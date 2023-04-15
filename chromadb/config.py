from pydantic import BaseSettings
from typing import List

TELEMETRY_WHITELISTED_SETTINGS = ["chroma_db_impl", "chroma_api_impl", "chroma_server_ssl_enabled"]


class Settings(BaseSettings):
    environment: str = ""

    chroma_db_impl: str = "duckdb"
    chroma_api_impl: str = "local"

    clickhouse_host: str = None
    clickhouse_port: str = None

    persist_directory: str = ".chroma"

    chroma_server_host: str = None
    chroma_server_http_port: str = None
    chroma_server_ssl_enabled: bool = False
    chroma_server_grpc_port: str = None
    chroma_server_cors_allow_origins: List[str] = []  # eg ["http://localhost:3000"]

    anonymized_telemetry: bool = True

    def __getitem__(self, item):
        return getattr(self, item)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
