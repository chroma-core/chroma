from functools import lru_cache
from pydantic import BaseSettings
from starlette.config import Config
config = Config(".env")

class Settings(BaseSettings):
    disable_anonymized_telemetry: bool = False
    telemetry_anonymized_uuid: str = False
    SECRET_KEY = "supersecret"
    
    POSTGRES_USER = "postgres"
    POSTGRES_PASSWORD = "postgres"
    POSTGRES_SERVER = "db"
    POSTGRES_PORT = 5432
    POSTGRES_DB = "postgres"
    DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DB}"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()