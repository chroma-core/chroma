import chromadb
from chromadb.experimental.ExperimentalCollection import ExperimentalCollection

class ExperimentalClient():
    def __init__(self, *args, **kwargs):
        self._client = chromadb.Client(*args, **kwargs)

    def create_collection(self, *args, **kwargs):
        base_collection = self._client.create_collection(*args, **kwargs)
        return ExperimentalCollection(base_collection)
    
    def get_collection(self, *args, **kwargs):
        base_collection = self._client.get_collection(*args, **kwargs)
        return ExperimentalCollection(base_collection)

    # Delegate everything else to the base collection
    def __getattr__(self, name):
        return getattr(self._client, name)
    
