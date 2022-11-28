from pydantic import BaseSettings, Field


class Settings(BaseSettings):

    disable_anonymized_telemetry: bool = False
    telemetry_anonymized_uuid: str = ""
    environment: str = ""

    chroma_db_impl: str = "duckdb"
    chroma_api_impl: str = "local"

    clickhouse_host: str = None
    clickhouse_port: str = None

    celery_broker_url: str = None
    celery_result_backend: str = None

    chroma_cache_dir: str = ".chroma"

    chroma_server_host: str = None
    chroma_server_http_port: str = None
    chroma_server_grpc_port: str = None

    def __getitem__(self, item):
        return getattr(self, item)

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
