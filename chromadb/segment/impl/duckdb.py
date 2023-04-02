from chromadb.segment import Segment, MetadataReader
from chromadb.config import Settings
from chromadb.types import MetadataEmbeddingRecord, Where, WhereDocument
from typing import Optional, Sequence


class DuckDB(MetadataReader):
    """Metadata search & storage using DuckDB"""

    settings: Settings
    data: Segment

    def __init__(self, settings: Settings, data: Segment):
        self._settings = settings
        self._data = data

    def get_metadata(
        self,
        where: Where = {},
        where_document: WhereDocument = {},
        ids: Optional[Sequence[str]] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        raise NotImplementedError

    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        raise NotImplementedError


class DuckDBMemory(MetadataReader):
    """Metadata search & storage using DuckDB"""

    settings: Settings
    data: Segment

    def __init__(self, settings: Settings, data: Segment):
        self._settings = settings
        self._data = data

    def get_metadata(
        self,
        where: Where = {},
        where_document: WhereDocument = {},
        ids: Optional[Sequence[str]] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        raise NotImplementedError

    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        raise NotImplementedError
