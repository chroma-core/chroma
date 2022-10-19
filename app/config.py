from functools import lru_cache
from pydantic import BaseSettings


class Settings(BaseSettings):
    dataset_path: str = "/opt/chroma/datasets"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
