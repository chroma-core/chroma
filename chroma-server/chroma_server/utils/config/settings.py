from functools import lru_cache
from typing import Union
from pydantic import BaseSettings

class Settings(BaseSettings):
    disable_anonymized_telemetry: bool = False
    telemetry_anonymized_uuid: str = False
    environment: str = 'development'
    user_sentry_dsn: str = ''

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()