from chromadb.segment import Segment, VectorReader
from chromadb.config import Settings
from chromadb.types import VectorQuery, VectorQueryResult, VectorEmbeddingRecord
from typing import Optional, Sequence


class Local(VectorReader):
    """Vector search & storage using HNSWlib, persisted locally"""

    _data: Segment
    _settings: Settings

    def __init__(self, settings: Settings, data: Segment):
        self._settings = settings
        self._data = data

    def get_vectors(self, ids: Optional[Sequence[str]]) -> Sequence[VectorEmbeddingRecord]:
        raise NotImplementedError

    def query_vectors(
        self, queries: Sequence[VectorQuery]
    ) -> Sequence[Sequence[VectorQueryResult]]:
        raise NotImplementedError


class LocalMemory(Local):
    """Vector search & storage using HNSWlib, in-memory only"""

    def __init__(self, settings: Settings, data: Segment):
        super().__init__(settings, data)
