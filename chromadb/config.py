from pydantic import BaseSettings, Field


class Settings(BaseSettings):

    environment: str = ""

    chroma_db_impl: str = "duckdb"
    chroma_api_impl: str = "local"

    clickhouse_host: str = None
    clickhouse_port: str = None

    persist_directory: str = ".chroma"

    chroma_server_host: str = None
    chroma_server_http_port: str = None
    chroma_server_grpc_port: str = None

    def __getitem__(self, item):
        return getattr(self, item)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
