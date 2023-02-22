from typing import Optional
import chromadb
from chromadb.api import API
from chromadb.config import Settings
from chromadb.experimental.ExperimentalClient import ExperimentalClient as EClient

# A monkeypatch wrapper for the regular client. 
def ExperimentalClient(*args, **kwargs):
    return EClient(*args, **kwargs)