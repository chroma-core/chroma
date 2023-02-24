from pydantic import BaseSettings, Field
from typing import Optional


class Settings(BaseSettings):
    environment: str = ""

    chroma_db_impl: str = "duckdb"
    chroma_api_impl: str = "local"

    clickhouse_host: Optional[str] = None
    clickhouse_port: Optional[str] = None

    persist_directory: str = ".chroma"

    chroma_server_host: Optional[str] = None
    chroma_server_http_port: Optional[str] = None
    chroma_server_grpc_port: Optional[str] = None

    def __getitem__(self, item):
        return getattr(self, item)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
