import chromadb.config
import chromadb.ingest


class LocalSynchronousStream(chromadb.ingest.Stream):
    """Stream implementation that immediately and synchronously writes
    embeddings to the MetaDB and Vector segments"""

    def __init__(self, settings):
        self.metadb = chromadb.config.get_component(settings, 'metadata_db')
        self.sysdb = chromadb.config.get_component(settings, 'system_db')

    def submit(self, topic, message):
        pass



