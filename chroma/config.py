from pydantic import BaseSettings, Field


class Settings(BaseSettings):

    disable_anonymized_telemetry: bool = False
    telemetry_anonymized_uuid: str = ""
    environment: str = ""

    clickhouse_host: str = None
    clickhouse_port: str = None

    celery_broker_url: str = None
    celery_result_backend: str = None

    chroma_cache_dir: str = None

    chroma_server_host: str = None
    chroma_server_http_port: str = None
    chroma_server_grpc_port: str = None

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'
