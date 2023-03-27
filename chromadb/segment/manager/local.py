from chromadb.segment import SegmentManager
from chromadb.config import Settings
import chromadb.config


class LocalSegmentManager(SegmentManager):
    """Local segment strategy for embedded or single-server use cases"""

    def __init__(self, settings: Settings):
        self.sysdb = chromadb.config.get_component(settings, "chroma_system_db_impl")

    def create_collection(
        self, name: str, embedding_function: str, metadata: dict[str, str]
    ) -> None:

        pass

    def reset(self):

        pass
