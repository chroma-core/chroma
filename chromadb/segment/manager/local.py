from chromadb.segment import SegmentManager, SegmentImplementation
from chromadb.config import Settings
from chromadb.types import Topic, Segment
import chromadb.db
import chromadb.config


class LocalSegmentManager(SegmentManager):
    """Local segment strategy for embedded or single-server use cases"""

    sysdb: chromadb.db.SysDB

    def __init__(self, settings: Settings):
        self.sysdb = chromadb.config.get_component(settings, "chroma_system_db_impl")

    def create_topic_segments(self, topic: Topic) -> None:
        raise NotImplementedError()

    def initialize_all(self):
        raise NotImplementedError()

    def get_instance(self, segment: Segment) -> SegmentImplementation:
        raise NotImplementedError()

    def delete_topic_segments(self, name: str) -> None:
        raise NotImplementedError()

    def reset(self):
        pass
