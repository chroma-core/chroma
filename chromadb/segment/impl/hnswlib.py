from chromadb.segment import Segment, VectorReader
from chromadb.config import Settings
from chromadb.types import VectorQuery, VectorQueryResult, VectorEmbeddingRecord
from typing import Optional, Sequence
from overrides import override


class Local(VectorReader):
    """Vector search & storage using HNSWlib, persisted locally"""

    _data: Segment
    _settings: Settings

    def __init__(self, settings: Settings, data: Segment):
        self._settings = settings
        self._data = data

    @override
    def get_vectors(self, ids: Optional[Sequence[str]]) -> Sequence[VectorEmbeddingRecord]:
        raise NotImplementedError

    @override
    def query_vectors(
        self, queries: Sequence[VectorQuery]
    ) -> Sequence[Sequence[VectorQueryResult]]:
        raise NotImplementedError
