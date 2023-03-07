import chromadb.config
import logging

logger = logging.getLogger(__name__)

__settings = chromadb.config.Settings()

def configure(**kwargs):
    """Override Chroma's default settings, environment variables or .env files"""
    __settings = chromadb.config.Settings(**kwargs)

def get_db(settings=__settings):
    """Return a DB instance based on the provided or environmental settings,
    optionally overriding the DB instance."""
    return chromadb.config.get_component(settings, "chroma_db_impl")

def Client(settings=__settings):
    """Return a chroma.API instance based on the provided or environmental
    settings, optionally overriding the DB instance."""
    return chromadb.config.get_component(settings, "chroma_api_impl")
