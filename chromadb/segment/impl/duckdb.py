from chromadb.segment import Segment, MetadataReader
from chromadb.config import Settings, get_component
from chromadb.types import MetadataEmbeddingRecord, Where, WhereDocument
import chromadb.db.impl.duckdb
from typing import Optional, Sequence, cast
from overrides import override


class DuckDB(MetadataReader):
    """Metadata search & storage using DuckDB"""

    _settings: Settings
    _topic: str

    def __init__(self, settings: Settings, segment: Segment):
        self._settings = settings
        self._topic = cast(str, segment["topic"])
        self._duckdb = get_component(settings, "chroma_ingest_impl")

        if not isinstance(self._duckdb, chromadb.db.impl.duckdb.DuckDB):
            raise ValueError(
                "DuckDB metadata segments may only be used when `chroma_ingest_impl` == `chromadb.db.impl.duckdb.DuckDB`"
            )

    @override
    def get_metadata(
        self,
        where: Optional[Where],
        where_document: Optional[WhereDocument],
        ids: Optional[Sequence[str]] = None,
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> Sequence[MetadataEmbeddingRecord]:
        return self._duckdb.get_metadata(
            self._topic, where, where_document, ids, sort, limit, offset
        )

    @override
    def count_metadata(self) -> int:
        """Get the number of embeddings in this segment."""
        return self._duckdb.count_embeddings(self._topic)
