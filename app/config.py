from functools import lru_cache
from pydantic import BaseSettings


class Settings(BaseSettings):
    main_database: str = "SQLite"
    embeddings_database: str = "SQLite"
    indexer: str = "Indexer"

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    return Settings()
