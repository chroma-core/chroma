from functools import lru_cache
from pydantic import BaseSettings

class Settings(BaseSettings):
    disable_anonymized_telemetry: bool = False
    telemetry_anonymized_uuid: str = 'not-set'

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()