from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TypedDict, Union, Optional
from chromadb.segment import InsertType, EmbeddingRecord, Vector

class VectorQuery(TypedDict):
    """A KNN/ANN query to the segment"""
    vector: Vector
    k: int
    approximate: bool
    allowed_ids: Optional[Sequence[str]]


class VectorQueryResult(EmbeddingRecord):
    """A KNN/ANN query result"""
    distance: float


class VectorWriter(ABC):
    """Write-side of the segment driver interface, used to store and index vectors."""


    @abstractmethod
    def insert_vectors(self,
                       vectors: Sequence[EmbeddingRecord],
                       insert_type: InsertType = InsertType.ADD_OR_UPDATE) -> None:
        """Add new embeddings to the segment."""
        pass


    @abstractmethod
    def delete_vectors(self, ids: Sequence[str]) -> None:
        """Remove vectors from the segment."""
        pass


class VectorReader(ABC):
    """Read-side of Segment Driver interface, used to retrieve
    vectors and to perform KNN/ANN searches."""


    @abstractmethod
    def get_vectors(self, ids: Optional[Sequence[str]]) -> Sequence[EmbeddingRecord]:
        """Get embeddings from the segment. If no IDs are provided,
        all embeddings are returned."""
        pass

    @abstractmethod
    def query_vectors(self, queries: Sequence[VectorQuery]) -> Sequence[Sequence[VectorQueryResult]]:
        """Given a list of vector queries, return the top-k nearest
        neighbors for each query."""
        pass
