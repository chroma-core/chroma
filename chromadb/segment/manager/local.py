from chromadb.segment import SegmentManager, SegmentImplementation
from chromadb.config import Settings
from chromadb.types import Topic, Segment
import chromadb.db
import chromadb.config
import uuid
import importlib

# Mapping of segment type names to Python classes so we can load them
type_to_class = {
    "hnswlib-local": "chromadb.segment.impl.hnswlib.Local",
    "hnswlib-local-memory": "chromadb.segment.impl.hnswlib.LocalMemory",
    "duckdb": "chromadb.segment.impl.duckdb.DuckDB",
}


class LocalSegmentManager(SegmentManager):
    """Local segment strategy for embedded or single-server use cases"""

    sysdb: chromadb.db.SysDB
    loaded_segments: dict[str, dict[uuid.UUID, SegmentImplementation]]

    def __init__(self, settings: Settings):
        settings.validate("chroma_default_vector_segment_type")
        settings.validate("chroma_default_metadata_segment_type")
        self.settings = settings
        self.sysdb = chromadb.config.get_component(settings, "chroma_system_db_impl")
        self.loaded_segments = {}

    def create_topic_segments(self, topic: Topic) -> None:

        vector_segment = Segment(
            id=uuid.uuid4(),
            topic=topic["name"],
            scope="vector",
            type=self.settings.chroma_default_vector_segment_type,
            metadata={},
        )

        metadata_segment = Segment(
            id=uuid.uuid4(),
            topic=topic["name"],
            scope="metadata",
            type=self.settings.chroma_default_metadata_segment_type,
            metadata={},
        )

        self.sysdb.create_segment(vector_segment)
        self.sysdb.create_segment(metadata_segment)
        self.get_instance(vector_segment)
        self.get_instance(metadata_segment)

    def initialize_all(self):
        raise NotImplementedError()

    def get_instance(self, segment: Segment) -> SegmentImplementation:
        if segment["type"] not in type_to_class:
            raise ValueError(f"Unknown segment type: {segment['type']}")

        fqn = type_to_class[segment["type"]]

        instance = self.loaded_segments.get(fqn, {}).get(segment["id"], None)
        if instance is not None:
            return instance

        module_name, class_name = fqn.rsplit(".", 1)
        module = importlib.import_module(module_name)
        cls = getattr(module, class_name)
        return cls(self.settings, segment)

    def delete_topic_segments(self, name: str) -> None:
        raise NotImplementedError()

    def reset(self):
        self.loaded_segments = {}
